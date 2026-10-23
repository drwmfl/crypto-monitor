from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from alerts.fast_followup import FastFollowUpClient, format_fast_followup, summarize_followup
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.fast_followup import (
        FastFollowUpClient,
        format_fast_followup,
        summarize_followup,
    )
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir

logger = logging.getLogger(__name__)

DEFAULT_DISCOVERY: Dict[str, Any] = {
    "enabled": False,
    "t1_change_pct": 4.0,
    "t1_min_rvol": 1.2,
    "t2_runup_pct": 8.0,
    "t2_confirm_on_close": False,
    "thread_hours": 4.0,
    "upgrade_pct": 15.0,
    "daily_cap": 3,
    "cap_exempt_runup_pct": 0.0,
    "cap_exempt_max": 2,
    "directions": ["up", "down"],
    "push_directions": [],
    "instrument_types": [],
    "followup_enabled": True,
    "followup_minutes": 15,
    "followup_hold_pct": 1.0,
    "followup_skip_statuses": [],
    "timezone_offset_hours": 8,
    "shadow_file": "discovery_shadow.jsonl",
    "state_file": "discovery_state.json",
}

TRIGGER_LABELS = {
    ("T1", "up"): "1分钟急拉",
    ("T1", "down"): "1分钟急跌",
    ("T2", "up"): "持续拉升",
    ("T2", "down"): "持续下跌",
}


class DiscoveryEngine:
    """Two triggers plus a per-symbol move thread.

    T1 fast spike : the current 1m candle has moved >= t1_change_pct with rvol >= t1_min_rvol.
    T2 steady run : price is >= t2_runup_pct away from the recent 1m extreme (low for up, high for down);
                    with t2_confirm_on_close only a closed 1m candle counts, which drops intra-minute wicks.
    Thread        : the first trigger opens a thread; inside thread_hours only a further move of
                    upgrade_pct beyond the last alert price records again; daily_cap per symbol and direction.

    Every alert is written to the shadow file. Directions listed in push_directions are marked
    mode "live" for the caller to push; the rest stay mode "shadow".
    """

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = dict(DEFAULT_DISCOVERY)
        if settings:
            self.settings.update(settings)
        self.enabled = _parse_bool(self.settings.get("enabled"), False)
        self.t1_change_pct = max(0.0, _to_float(self.settings.get("t1_change_pct"), 4.0))
        self.t1_min_rvol = max(0.0, _to_float(self.settings.get("t1_min_rvol"), 1.2))
        self.t2_runup_pct = max(0.0, _to_float(self.settings.get("t2_runup_pct"), 8.0))
        self.t2_confirm_on_close = _parse_bool(self.settings.get("t2_confirm_on_close"), False)
        self.thread_ms = int(max(0.0, _to_float(self.settings.get("thread_hours"), 4.0)) * 3600 * 1000)
        self.upgrade_pct = max(0.0, _to_float(self.settings.get("upgrade_pct"), 15.0))
        self.daily_cap = max(1, int(_to_float(self.settings.get("daily_cap"), 3)))
        self.cap_exempt_runup_pct = max(0.0, _to_float(self.settings.get("cap_exempt_runup_pct"), 0.0))
        self.cap_exempt_max = max(0, int(_to_float(self.settings.get("cap_exempt_max"), 2)))
        self.directions = _direction_set(self.settings.get("directions"), default={"up", "down"})
        self.push_directions = _direction_set(self.settings.get("push_directions"), default=set()) & self.directions
        self.instrument_types = {
            str(item).strip().lower() for item in _as_list(self.settings.get("instrument_types")) if str(item).strip()
        }
        self.tz = timezone(timedelta(hours=_to_float(self.settings.get("timezone_offset_hours"), 8.0)))
        self.runtime_dir: Optional[Path] = None
        self.shadow_path: Optional[Path] = None
        self.state_path: Optional[Path] = None
        if self.enabled:
            self.runtime_dir = resolve_runtime_dir(self.settings)
            self.shadow_path = self.runtime_dir / str(self.settings.get("shadow_file") or "discovery_shadow.jsonl")
            self.state_path = self.runtime_dir / str(self.settings.get("state_file") or "discovery_state.json")
        self._threads: Dict[str, Dict[str, float]] = {}
        self._day_counts: Dict[str, int] = {}
        self._exempt_counts: Dict[str, int] = {}
        self._load_state()

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "DiscoveryEngine":
        raw = config.get("discovery", {}) if isinstance(config, dict) else {}
        return cls(raw if isinstance(raw, dict) else {})

    def observe(
        self,
        *,
        symbol: str,
        now_ms: int,
        price: float,
        change_1m_pct: Optional[float],
        rvol: Optional[float],
        runup_up_pct: Optional[float],
        runup_down_pct: Optional[float],
        runup_minutes: int,
        is_closed: bool = True,
        instrument_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        if not self.enabled or price <= 0:
            return []
        if self.instrument_types and str(instrument_type or "crypto").strip().lower() not in self.instrument_types:
            return []
        recorded: List[Dict[str, Any]] = []
        for direction in ("up", "down"):
            if direction not in self.directions:
                continue
            triggers = self._triggers(
                direction,
                change_1m_pct=change_1m_pct,
                rvol=rvol,
                runup_pct=runup_up_pct if direction == "up" else runup_down_pct,
                is_closed=is_closed,
            )
            if not triggers:
                continue
            runup_pct = runup_up_pct if direction == "up" else runup_down_pct
            decision = self._thread_decision(symbol, direction, now_ms, price, runup_pct)
            if decision is None:
                continue
            kind, prev_price, cap_exempt = decision
            row = {
                "created_at": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(),
                "symbol": symbol,
                "direction": direction,
                "triggers": triggers,
                "kind": kind,
                "price": price,
                "prev_price": prev_price,
                "change_1m_pct": _round(change_1m_pct),
                "rvol": _round(rvol),
                "runup_pct": _round(runup_pct),
                "runup_minutes": runup_minutes,
                "day_count": self._day_counts.get(self._day_key(symbol, direction, now_ms), 0),
                "cap_exempt": cap_exempt,
                "mode": "live" if direction in self.push_directions else "shadow",
            }
            recorded.append(row)
            logger.info(
                "Discovery alert: mode=%s symbol=%s dir=%s triggers=%s kind=%s price=%s change_1m=%s runup=%s/%sm",
                row["mode"],
                symbol,
                direction,
                "+".join(triggers),
                kind,
                price,
                row["change_1m_pct"],
                row["runup_pct"],
                runup_minutes,
            )
        if recorded:
            self._append(recorded)
            self._save_state(now_ms)
        return recorded

    def _triggers(
        self,
        direction: str,
        *,
        change_1m_pct: Optional[float],
        rvol: Optional[float],
        runup_pct: Optional[float],
        is_closed: bool,
    ) -> List[str]:
        sign = 1.0 if direction == "up" else -1.0
        triggers: List[str] = []
        if (
            change_1m_pct is not None
            and sign * change_1m_pct >= self.t1_change_pct
            and (self.t1_min_rvol <= 0 or (rvol is not None and rvol >= self.t1_min_rvol))
        ):
            triggers.append("T1")
        if (
            runup_pct is not None
            and sign * runup_pct >= self.t2_runup_pct
            and (is_closed or not self.t2_confirm_on_close)
        ):
            triggers.append("T2")
        return triggers

    def _thread_decision(
        self,
        symbol: str,
        direction: str,
        now_ms: int,
        price: float,
        runup_pct: Optional[float] = None,
    ) -> Optional[tuple]:
        day_key = self._day_key(symbol, direction, now_ms)
        capped = self._day_counts.get(day_key, 0) >= self.daily_cap
        if capped and not self._cap_exempt(day_key, direction, runup_pct):
            return None
        thread_key = f"{symbol}:{direction}"
        thread = self._threads.get(thread_key)
        prev_price = None
        if thread and now_ms - thread["ts"] < self.thread_ms:
            last = thread["price"]
            threshold = last * (1 + self.upgrade_pct / 100) if direction == "up" else last * (1 - self.upgrade_pct / 100)
            extended = price >= threshold if direction == "up" else price <= threshold
            if not extended:
                return None
            kind = "upgrade"
            prev_price = last
        else:
            kind = "new"
        self._threads[thread_key] = {"ts": now_ms, "price": price}
        self._day_counts[day_key] = self._day_counts.get(day_key, 0) + 1
        if capped:
            self._exempt_counts[day_key] = self._exempt_counts.get(day_key, 0) + 1
        return kind, prev_price, capped

    def _cap_exempt(self, day_key: str, direction: str, runup_pct: Optional[float]) -> bool:
        """A move that is already this large is the case the cap must not hide; allow a few extra per day."""
        if self.cap_exempt_runup_pct <= 0 or runup_pct is None:
            return False
        sign = 1.0 if direction == "up" else -1.0
        if sign * runup_pct < self.cap_exempt_runup_pct:
            return False
        return self._exempt_counts.get(day_key, 0) < self.cap_exempt_max

    def _day_key(self, symbol: str, direction: str, now_ms: int) -> str:
        day = datetime.fromtimestamp(now_ms / 1000, self.tz).date().isoformat()
        return f"{day}:{symbol}:{direction}"

    def _append(self, rows: List[Dict[str, Any]]) -> None:
        if self.shadow_path is None:
            return
        try:
            with self.shadow_path.open("a", encoding="utf-8") as fh:
                for row in rows:
                    fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.warning("Discovery shadow write failed: %s", exc)

    def _load_state(self) -> None:
        if self.state_path is None or not self.state_path.exists():
            return
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Discovery state load failed, starting empty: %s", exc)
            return
        threads = payload.get("threads") if isinstance(payload, dict) else None
        counts = payload.get("day_counts") if isinstance(payload, dict) else None
        if isinstance(threads, dict):
            self._threads = {
                str(k): {"ts": float(v.get("ts", 0)), "price": float(v.get("price", 0))}
                for k, v in threads.items()
                if isinstance(v, dict)
            }
        if isinstance(counts, dict):
            self._day_counts = {str(k): int(v) for k, v in counts.items()}
        exempt = payload.get("exempt_counts") if isinstance(payload, dict) else None
        if isinstance(exempt, dict):
            self._exempt_counts = {str(k): int(v) for k, v in exempt.items()}

    def _save_state(self, now_ms: int) -> None:
        if self.state_path is None:
            return
        today = datetime.fromtimestamp(now_ms / 1000, self.tz).date().isoformat()
        self._threads = {k: v for k, v in self._threads.items() if now_ms - v["ts"] < self.thread_ms}
        self._day_counts = {k: v for k, v in self._day_counts.items() if k.startswith(today)}
        self._exempt_counts = {k: v for k, v in self._exempt_counts.items() if k.startswith(today)}
        try:
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(
                json.dumps(
                    {
                        "saved_at": time.time(),
                        "threads": self._threads,
                        "day_counts": self._day_counts,
                        "exempt_counts": self._exempt_counts,
                    }
                ),
                encoding="utf-8",
            )
            tmp.replace(self.state_path)
        except Exception as exc:
            logger.warning("Discovery state save failed: %s", exc)


# Earlier name, kept so existing imports keep working.
DiscoveryShadow = DiscoveryEngine


def format_discovery_alert(row: Dict[str, Any], *, followup_minutes: Optional[int] = None) -> str:
    symbol = str(row.get("symbol") or "")
    token = _token_name(symbol)
    direction = str(row.get("direction") or "up")
    up = direction == "up"
    runup = _to_float(row.get("runup_pct"), 0.0)
    minutes = int(_to_float(row.get("runup_minutes"), 60.0))
    move_icon = "📈" if up else "📉"
    lines: List[str] = []
    if row.get("kind") == "upgrade":
        prev = _to_float(row.get("prev_price"), 0.0)
        price = _to_float(row.get("price"), 0.0)
        extra = (price / prev - 1) * 100 if prev > 0 else 0.0
        lines.append(f"**🔥 异动加速 | {token}（触发1次）**")
        lines.append(
            f"{move_icon} 较上次提醒 {_fmt_price(prev)} → {_fmt_price(price)}（{extra:+.2f}%）"
            f" | 近{minutes}分钟 {runup:+.2f}%"
        )
        lines.append("⚠️ 加速阶段，注意过热回落" if up else "⚠️ 加速下跌，注意急跌后的反弹")
    else:
        lines.append(f"**🔎 异动发现 | {token}（触发1次）**")
        parts = [f"近{minutes}分钟 {runup:+.2f}%"]
        change_1m = row.get("change_1m_pct")
        if change_1m is not None:
            parts.append(f"1分钟 {_to_float(change_1m, 0.0):+.2f}%")
        rvol = row.get("rvol")
        if rvol is not None:
            parts.append(f"RVOL {_to_float(rvol, 0.0):.2f}x")
        lines.append(f"{move_icon} " + " | ".join(parts))
    triggers = [TRIGGER_LABELS.get((str(t), direction), str(t)) for t in row.get("triggers") or []]
    if triggers:
        lines.append("🧭 触发：" + " + ".join(triggers))
    lines.append(f"💵 现价 {_fmt_price(row.get('price'))}")
    if followup_minutes:
        lines.append(f"⏱ {followup_minutes}分钟后跟进")
    lines.append(f"🔗 https://www.binance.com/futures/{symbol}")
    return "\n".join(lines)


class DiscoveryPusher:
    """Sends live discovery alerts through the notifier and follows each one up after a delay."""

    def __init__(self, settings: Optional[Dict[str, Any]], notifier: Any, *, client: Any = None) -> None:
        merged = dict(DEFAULT_DISCOVERY)
        merged.update(settings or {})
        self.notifier = notifier
        self.followup_enabled = _parse_bool(merged.get("followup_enabled"), True)
        self.followup_minutes = max(1, int(_to_float(merged.get("followup_minutes"), 15)))
        self.followup_hold_pct = max(0.0, _to_float(merged.get("followup_hold_pct"), 1.0))
        self.followup_skip_statuses = {
            str(item).strip().lower() for item in _as_list(merged.get("followup_skip_statuses")) if str(item).strip()
        }
        self.client = client or FastFollowUpClient()
        self._tasks: set = set()

    @classmethod
    def from_config(cls, config: Dict[str, Any], notifier: Any) -> "DiscoveryPusher":
        raw = config.get("discovery", {}) if isinstance(config, dict) else {}
        return cls(raw if isinstance(raw, dict) else {}, notifier)

    def push(self, row: Dict[str, Any]) -> Optional[asyncio.Task]:
        if self.notifier is None or not hasattr(self.notifier, "notify_text"):
            return None
        task = asyncio.create_task(self._send(row), name=f"discovery_push_{row.get('symbol')}")
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def close(self) -> None:
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def _followup_delay_sec(self) -> float:
        return float(self.followup_minutes * 60)

    async def _send(self, row: Dict[str, Any]) -> None:
        symbol = str(row.get("symbol") or "")
        try:
            message = format_discovery_alert(row, followup_minutes=self.followup_minutes if self.followup_enabled else None)
            sent = await self.notifier.notify_text(
                message,
                symbol=symbol,
                rule_name="discovery_upgrade" if row.get("kind") == "upgrade" else "discovery_alert",
                level="high",
                metadata={
                    "alert_type": "discovery_alert",
                    "kind": row.get("kind"),
                    "direction": row.get("direction"),
                    "triggers": row.get("triggers"),
                    "runup_pct": row.get("runup_pct"),
                },
            )
            logger.info("Discovery push: symbol=%s kind=%s sent=%s", symbol, row.get("kind"), sent)
            if not sent or not self.followup_enabled:
                return
            sent_ms = int(time.time() * 1000)
            await asyncio.sleep(self._followup_delay_sec())
            bars = await self.client.fetch_1m_bars(symbol, start_ms=sent_ms - sent_ms % 60000, limit=self.followup_minutes + 1)
            summary = summarize_followup(
                bars,
                entry_price=_to_float(row.get("price"), 0.0),
                direction=str(row.get("direction") or "up"),
                hold_pct=self.followup_hold_pct,
            )
            if summary is None:
                logger.warning("Discovery follow-up skipped, no candles: symbol=%s", symbol)
                return
            if summary["status"] in self.followup_skip_statuses:
                logger.info(
                    "Discovery follow-up not sent: symbol=%s status=%s change=%.2f",
                    symbol,
                    summary["status"],
                    summary["change_pct"],
                )
                return
            followup = format_fast_followup(
                token=_token_name(symbol),
                symbol=symbol,
                direction=str(row.get("direction") or "up"),
                minutes=self.followup_minutes,
                summary=summary,
                title="异动跟进",
            )
            # No symbol here: a follow-up must not bump the daily push counter in the title.
            await self.notifier.notify_text(
                followup,
                rule_name="discovery_followup",
                level="medium",
                metadata={"symbol": symbol, "alert_type": "discovery_followup", "status": summary["status"]},
            )
            logger.info("Discovery follow-up: symbol=%s status=%s change=%.2f", symbol, summary["status"], summary["change_pct"])
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Discovery push failed: symbol=%s", symbol)


def _token_name(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
        if text.endswith(suffix) and len(text) > len(suffix):
            return text[: -len(suffix)]
    return text or "UNKNOWN"


def _fmt_price(value: Any) -> str:
    number = _to_float(value, 0.0)
    if number <= 0:
        return "N/A"
    return f"{number:.8f}".rstrip("0").rstrip(".")


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return []


def _direction_set(value: Any, *, default: set) -> set:
    if value is None:
        return set(default)
    return {str(item).strip().lower() for item in _as_list(value) if str(item).strip().lower() in {"up", "down"}}


def _round(value: Optional[float]) -> Optional[float]:
    return None if value is None else round(float(value), 4)


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
