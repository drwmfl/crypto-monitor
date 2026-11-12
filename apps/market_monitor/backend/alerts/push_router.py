from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

try:
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir

logger = logging.getLogger(__name__)

DEFAULT_PUSH_ROUTER: Dict[str, Any] = {
    "enabled": False,
    "thread_minutes": 60,
    "context_window_minutes": 30,
    "followups_silent": True,
    "shadow_file": "push_router_shadow.jsonl",
    # A ringing signal may take over a thread that so far only carried a silent push.
    "ring_overrides_silent_thread": True,
    "blocked_instrument_types": [],
    # Market-wide moves: once min_coins coins fire in one direction within window_sec, further pushes
    # are held back and delivered as a single digest message.
    "burst": {
        "enabled": False,
        "min_coins": 6,
        "window_sec": 60,
        "quiet_sec": 45,
        "max_hold_sec": 180,
        "max_list": 40,
    },
    "legacy": {
        "up": {
            "ring_types": ["strong_direct_alert", "actionable_alert", "startup_alert"],
            "silent_types": ["risk_alert"],
            "windows": ["1m", "5m"],
        },
        "down": {
            "ring_types": ["strong_direct_alert"],
            "silent_types": [],
            "windows": ["1m", "5m"],
        },
    },
    "discovery": {
        "t2_full_runup_pct": 10.0,
        "up": {
            "t1_max_speed_minutes": 30,
            "t1_slow_mode": "drop",
            "t2_low_weak_mode": "drop",
            "t2_low_enabled": True,
            "t2_low_min_vol5x": 2.0,
            "t2_low_ring_vol5x": 3.0,
            "t2_high_min_qv24": 0.0,
            "upgrade_mode": "silent",
        },
        "down": {
            "t1_max_speed_minutes": 0,
            "t1_slow_mode": "drop",
            "t2_low_weak_mode": "drop",
            "t2_low_enabled": False,
            "t2_low_min_vol5x": 0.0,
            "t2_low_ring_vol5x": 0.0,
            "t2_high_min_qv24": 5_000_000.0,
            "upgrade_mode": "silent",
        },
    },
}

LEGACY_LABELS = {
    "strong_direct_alert": "强势直推",
    "actionable_alert": "可操作",
    "startup_alert": "启动",
    "risk_alert": "风险",
}
DISCOVERY_LABELS = {
    "upgrade": "异动加速",
    "T1": "异动1分钟急变",
    "T2_low": "异动持续",
    "T2_high": "异动持续",
}


@dataclass
class Route:
    """Verdict for one candidate push."""

    send: bool
    silent: bool
    reason: str
    symbol: str
    direction: str
    source: str
    family: str
    label: str
    features: Dict[str, Any] = field(default_factory=dict)
    co_signals: List[str] = field(default_factory=list)
    claimed_at: float = 0.0
    move_pct: Optional[float] = None


class PushRouter:
    """Strategy A: one push per coin and direction per thread across both strategies.

    Every candidate push from the legacy pipeline and the discovery engine is routed here. Low-yield
    families are dropped, the rest ring or go out silently, and the first push of a coin+direction
    claims a thread that blocks further pushes for thread_minutes. Every verdict is written to the
    shadow file so filtered signals can be reviewed later.
    """

    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        merged = _deep_merge(DEFAULT_PUSH_ROUTER, settings or {})
        self.settings = merged
        self.enabled = _parse_bool(merged.get("enabled"), False)
        self.thread_sec = max(0.0, _to_float(merged.get("thread_minutes"), 60.0)) * 60.0
        self.context_sec = max(0.0, _to_float(merged.get("context_window_minutes"), 30.0)) * 60.0
        self.followups_silent = _parse_bool(merged.get("followups_silent"), True)
        self.legacy = merged.get("legacy") if isinstance(merged.get("legacy"), dict) else {}
        self.discovery = merged.get("discovery") if isinstance(merged.get("discovery"), dict) else {}
        self.ring_overrides_silent = _parse_bool(merged.get("ring_overrides_silent_thread"), True)
        self.blocked_instruments = {
            str(item).strip().lower() for item in _as_list(merged.get("blocked_instrument_types")) if str(item).strip()
        }
        burst = merged.get("burst") if isinstance(merged.get("burst"), dict) else {}
        self.burst_enabled = _parse_bool(burst.get("enabled"), False)
        self.burst_min_coins = max(2, int(_to_float(burst.get("min_coins"), 6)))
        self.burst_window_sec = max(1.0, _to_float(burst.get("window_sec"), 60.0))
        self.burst_quiet_sec = max(0.0, _to_float(burst.get("quiet_sec"), 45.0))
        self.burst_max_hold_sec = max(self.burst_quiet_sec, _to_float(burst.get("max_hold_sec"), 180.0))
        self.burst_max_list = max(1, int(_to_float(burst.get("max_list"), 40)))
        # Set by the realtime watcher; the burst digest is the only message the router sends itself.
        self.notifier: Any = None
        self.feature_provider: Optional[Callable[[str, str], Dict[str, Any]]] = None
        self.shadow_path: Optional[Path] = None
        if self.enabled:
            runtime_dir = resolve_runtime_dir(merged)
            self.shadow_path = runtime_dir / str(merged.get("shadow_file") or "push_router_shadow.jsonl")
        self._threads: Dict[Tuple[str, str], Tuple[float, bool]] = {}
        self._burst_recent: Dict[str, List[Tuple[float, str]]] = {}
        self._burst_pending: Dict[str, List[Tuple[float, Route]]] = {}
        self._burst_tasks: Dict[str, asyncio.Task] = {}
        self._signals: Dict[Tuple[str, str], List[Tuple[float, str]]] = {}

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "PushRouter":
        raw = config.get("push_router", {}) if isinstance(config, dict) else {}
        settings = dict(raw) if isinstance(raw, dict) else {}
        strategy = config.get("alert_strategy", {}) if isinstance(config, dict) else {}
        if isinstance(strategy, dict) and strategy.get("runtime_dir") and not settings.get("runtime_dir"):
            settings["runtime_dir"] = strategy.get("runtime_dir")
        return cls(settings)

    def features(self, symbol: str, direction: str) -> Dict[str, Any]:
        provider = self.feature_provider
        if provider is None:
            return {}
        try:
            return dict(provider(symbol, direction) or {})
        except Exception:
            logger.debug("Push router feature lookup failed: symbol=%s", symbol, exc_info=True)
            return {}

    def route_legacy(
        self,
        *,
        symbol: str,
        direction: str,
        alert_type: str,
        window: str,
        instrument_type: Any = None,
        change_pct: Any = None,
    ) -> Route:
        symbol = str(symbol or "").upper()
        direction = str(direction or "").lower()
        label = LEGACY_LABELS.get(alert_type, alert_type)
        if str(instrument_type or "").strip().lower() in self.blocked_instruments:
            return self._finish_route(symbol, direction, "legacy", alert_type, label, {}, False, False, "instrument_blocked")
        features = self.features(symbol, direction)
        if change_pct is not None:
            features["move_pct"] = _to_float(change_pct, 0.0)
        rules = self.legacy.get(direction) if isinstance(self.legacy.get(direction), dict) else None
        if rules is None:
            # Unknown direction: nothing to filter on, keep the old behaviour.
            return self._finish_route(symbol, direction, "legacy", alert_type, label, features, True, False, "unrouted_direction")
        windows = {str(w) for w in _as_list(rules.get("windows"))}
        ring_types = {str(t) for t in _as_list(rules.get("ring_types"))}
        silent_types = {str(t) for t in _as_list(rules.get("silent_types"))}
        if windows and str(window or "") not in windows:
            return self._finish_route(symbol, direction, "legacy", alert_type, label, features, False, False, "legacy_window_filtered")
        if alert_type in ring_types:
            return self._finish_route(symbol, direction, "legacy", alert_type, label, features, True, False, "legacy_ring")
        if alert_type in silent_types:
            return self._finish_route(symbol, direction, "legacy", alert_type, label, features, True, True, "legacy_silent")
        return self._finish_route(symbol, direction, "legacy", alert_type, label, features, False, False, "legacy_type_filtered")

    def route_discovery(self, row: Dict[str, Any]) -> Route:
        symbol = str(row.get("symbol") or "").upper()
        direction = str(row.get("direction") or "").lower()
        triggers = [str(t) for t in row.get("triggers") or []]
        runup = abs(_to_float(row.get("runup_pct"), 0.0))
        full_runup = _to_float(self.discovery.get("t2_full_runup_pct"), 10.0)
        if row.get("kind") == "upgrade":
            family = "upgrade"
        elif triggers == ["T1"]:
            family = "T1"
        else:
            family = "T2_low" if runup < full_runup else "T2_high"
        label = DISCOVERY_LABELS.get(family, family)
        if str(row.get("instrument_type") or "").strip().lower() in self.blocked_instruments:
            return self._finish_route(symbol, direction, "discovery", family, label, {}, False, False, "instrument_blocked")
        features = self.features(symbol, direction)
        if row.get("runup_pct") is not None:
            features["move_pct"] = _to_float(row.get("runup_pct"), 0.0)
        rules = self.discovery.get(direction) if isinstance(self.discovery.get(direction), dict) else {}
        speed = features.get("speed_min")
        vol5x = features.get("vol5x")
        qv24 = features.get("qv24")

        send, silent, reason = False, False, "discovery_filtered"
        if family == "upgrade":
            mode = str(rules.get("upgrade_mode") or "silent").lower()
            send, silent, reason = mode in {"ring", "silent"}, mode == "silent", f"upgrade_{mode}"
        elif family == "T1":
            max_speed = _to_float(rules.get("t1_max_speed_minutes"), 0.0)
            if max_speed > 0 and speed is not None and speed > max_speed:
                if str(rules.get("t1_slow_mode") or "drop").lower() == "silent":
                    send, silent, reason = True, True, "t1_slow_silent"
                else:
                    reason = "t1_slow_move"
            else:
                send, reason = True, "t1_ring"
        elif family == "T2_low":
            if not _parse_bool(rules.get("t2_low_enabled"), True):
                reason = "t2_low_disabled"
            else:
                min_vol = _to_float(rules.get("t2_low_min_vol5x"), 0.0)
                ring_vol = _to_float(rules.get("t2_low_ring_vol5x"), 0.0)
                if vol5x is None:
                    # No volume history yet (e.g. right after a restart): send, but without ringing.
                    send, silent, reason = True, True, "t2_low_no_volume_history"
                elif vol5x < min_vol:
                    if str(rules.get("t2_low_weak_mode") or "drop").lower() == "silent":
                        send, silent, reason = True, True, "t2_low_weak_silent"
                    else:
                        reason = "t2_low_no_volume"
                elif ring_vol > 0 and vol5x >= ring_vol:
                    send, reason = True, "t2_low_ring"
                else:
                    send, silent, reason = True, True, "t2_low_silent"
        else:
            min_qv = _to_float(rules.get("t2_high_min_qv24"), 0.0)
            if min_qv > 0 and qv24 is not None and qv24 < min_qv:
                reason = "t2_high_low_liquidity"
            else:
                send, silent, reason = True, True, "t2_high_silent"
        return self._finish_route(symbol, direction, "discovery", family, label, features, send, silent, reason)

    def release(self, route: Route) -> None:
        """Undo a thread claim when the message could not be queued."""
        key = (route.symbol, route.direction)
        claim = self._threads.get(key)
        if route.claimed_at and claim is not None and claim[0] == route.claimed_at:
            self._threads.pop(key, None)

    def decorate(self, message: str, route: Route) -> str:
        extra: List[str] = []
        parts = []
        speed = route.features.get("speed_min")
        if speed is not None:
            parts.append(f"起涨{int(speed)}分钟" if route.direction == "up" else f"起跌{int(speed)}分钟")
        vol5x = route.features.get("vol5x")
        if vol5x is not None:
            parts.append(f"5分钟量 {vol5x:.1f}倍")
        qv24 = route.features.get("qv24")
        if qv24:
            parts.append(f"24h额 {_fmt_amount(qv24)}")
        if parts:
            extra.append("📊 " + " | ".join(parts))
        if route.co_signals:
            extra.append("🤝 30分钟内同时命中：" + "、".join(route.co_signals))
        if route.silent:
            extra.append("🔕 次要信号（静音）")
        if not extra:
            return message
        lines = message.split("\n")
        link_at = next((i for i in range(len(lines) - 1, -1, -1) if lines[i].startswith("🔗")), len(lines))
        return "\n".join(lines[:link_at] + extra + lines[link_at:])

    def _finish_route(
        self,
        symbol: str,
        direction: str,
        source: str,
        family: str,
        label: str,
        features: Dict[str, Any],
        send: bool,
        silent: bool,
        reason: str,
    ) -> Route:
        now = time.time()
        key = (symbol, direction)
        co_signals = self._note_signal(key, now, label)
        route = Route(
            send=send,
            silent=silent,
            reason=reason,
            symbol=symbol,
            direction=direction,
            source=source,
            family=family,
            label=label,
            features=features,
            co_signals=co_signals,
            move_pct=features.pop("move_pct", None),
        )
        if send:
            last = self._threads.get(key)
            blocked = last is not None and now - last[0] < self.thread_sec
            if blocked and self.ring_overrides_silent and last[1] and not silent:
                blocked = False
            if blocked:
                route.send = False
                route.reason = f"thread_dedupe:{reason}"
            else:
                self._threads[key] = (now, silent)
                route.claimed_at = now
                if self._absorb_into_burst(route, now):
                    # The thread stays claimed: the coin is reported through the digest.
                    route.send = False
                    route.reason = f"market_burst:{reason}"
        self._record(route, now)
        return route

    def _absorb_into_burst(self, route: Route, now: float) -> bool:
        if not self.burst_enabled or self.notifier is None or not hasattr(self.notifier, "notify_text"):
            return False
        direction = route.direction
        recent = [(ts, sym) for ts, sym in self._burst_recent.get(direction, []) if now - ts <= self.burst_window_sec]
        recent.append((now, route.symbol))
        self._burst_recent[direction] = recent
        if len({sym for _, sym in recent}) < self.burst_min_coins:
            return False
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        self._burst_pending.setdefault(direction, []).append((now, route))
        if direction not in self._burst_tasks:
            self._burst_tasks[direction] = loop.create_task(self._flush_burst(direction), name=f"push_burst_{direction}")
        return True

    async def _flush_burst(self, direction: str) -> None:
        try:
            while True:
                pending = self._burst_pending.get(direction) or []
                if not pending:
                    return
                now = time.time()
                wait = min(pending[-1][0] + self.burst_quiet_sec, pending[0][0] + self.burst_max_hold_sec) - now
                if wait <= 0:
                    break
                await asyncio.sleep(min(wait, 1.0))
        finally:
            # Unregister before sending so signals arriving during the send start a new digest.
            self._burst_tasks.pop(direction, None)
            routes = [route for _, route in self._burst_pending.pop(direction, [])]
        try:
            sent = await self.notifier.notify_text(
                format_burst_digest(direction, routes, window_sec=self.burst_window_sec, max_list=self.burst_max_list),
                rule_name="market_burst",
                level="high",
                metadata={"alert_type": "market_burst", "direction": direction, "count": len(routes)},
            )
            logger.info("Push burst digest: dir=%s coins=%s sent=%s", direction, len(routes), sent)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Push burst digest failed: dir=%s coins=%s", direction, len(routes))

    def _note_signal(self, key: Tuple[str, str], now: float, label: str) -> List[str]:
        rows = [(ts, lbl) for ts, lbl in self._signals.get(key, []) if now - ts <= self.context_sec]
        others: List[str] = []
        for _, lbl in rows:
            if lbl != label and lbl not in others:
                others.append(lbl)
        rows.append((now, label))
        self._signals[key] = rows
        if len(self._signals) > 5000:
            for stale in [k for k, v in self._signals.items() if not v or now - v[-1][0] > self.context_sec]:
                self._signals.pop(stale, None)
        return others

    def _record(self, route: Route, now: float) -> None:
        logger.info(
            "Push route: symbol=%s dir=%s source=%s family=%s send=%s silent=%s reason=%s",
            route.symbol,
            route.direction,
            route.source,
            route.family,
            route.send,
            route.silent,
            route.reason,
        )
        if self.shadow_path is None:
            return
        row = {
            "created_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "symbol": route.symbol,
            "direction": route.direction,
            "source": route.source,
            "family": route.family,
            "send": route.send,
            "silent": route.silent,
            "reason": route.reason,
            "features": {k: _round(v) for k, v in route.features.items()},
            "co_signals": route.co_signals,
        }
        try:
            with self.shadow_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.warning("Push router shadow write failed: %s", exc)


def format_burst_digest(direction: str, routes: List[Route], *, window_sec: float = 60.0, max_list: int = 40) -> str:
    up = direction == "up"
    coins: Dict[str, Route] = {}
    for route in routes:
        coins.setdefault(route.symbol, route)
    ordered = sorted(coins.values(), key=lambda r: -_to_float(r.features.get("qv24"), 0.0))
    lines = [
        f"**🌊 市场集体{'急涨' if up else '急跌'} | {len(ordered)} 个币同时异动**",
        f"{'📈' if up else '📉'} {int(window_sec)}秒内同方向信号过多，已合并为一条",
    ]
    for route in ordered[:max_list]:
        parts = [_token_name(route.symbol), route.label]
        if route.move_pct is not None:
            parts.append(f"{route.move_pct:+.1f}%")
        qv24 = route.features.get("qv24")
        if qv24:
            parts.append(f"24h额 {_fmt_amount(qv24)}")
        lines.append("• " + " | ".join(parts))
    if len(ordered) > max_list:
        lines.append(f"…另有 {len(ordered) - max_list} 个")
    return "\n".join(lines)


def _token_name(symbol: str) -> str:
    text = str(symbol or "").strip().upper()
    return text[:-4] if text.endswith("USDT") and len(text) > 4 else text


def _fmt_amount(value: Any) -> str:
    number = _to_float(value, 0.0)
    if number >= 1e8:
        return f"{number / 1e8:.2f}亿"
    if number >= 1e4:
        return f"{number / 1e4:.0f}万"
    return f"{number:.0f}"


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    out = {k: (_deep_merge(v, {}) if isinstance(v, dict) else v) for k, v in base.items()}
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    if isinstance(value, (list, tuple, set)):
        return list(value)
    return []


def _round(value: Any) -> Any:
    return round(value, 4) if isinstance(value, float) else value


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
