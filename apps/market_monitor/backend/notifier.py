from __future__ import annotations

import asyncio
import copy
import json
import logging
import os
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

try:
    from packages.notifier.telegram_alert import send_telegram_alert
except ModuleNotFoundError:
    repo_root = Path(__file__).resolve().parents[1]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from utils.telegram_alert import send_telegram_alert

logger = logging.getLogger(__name__)
VALID_LEVELS = {"low", "medium", "high"}
BEIJING_TZ = timezone(timedelta(hours=8))
DEFAULT_PUSH_POLICY: Dict[str, Any] = {
    "repeat_window_minutes": 10,
    "merge_window_minutes": 8,
    "symbol_hourly_max_pushes": 6,
    "cross_window_merge_enabled": True,
    "cross_window_merge_windows": ["15m", "30m", "1h"],
    "cross_window_coalesce_sec": 90,
    "cross_window_upgrade_sec": 180,
    "cross_window_upgrade_min_score_delta": 8.0,
    "attention_windows": ["5m"],
    "high_priority": {
        "window": "5m",
        "confidence_min": 85.0,
        "rvol_min": 2.0,
    },
    "watch_bars_by_window": {
        "1m": 10,
        "5m": 6,
        "15m": 4,
        "30m": 3,
        "1h": 2,
    },
    "trade_params_by_window": {
        "1m": {"sl_pct": 1.4, "tp1_pct": 1.6, "tp2_pct": 2.6},
        "5m": {"sl_pct": 3.0, "tp1_pct": 3.6, "tp2_pct": 6.0},
        "15m": {"sl_pct": 4.0, "tp1_pct": 5.0, "tp2_pct": 8.0},
        "30m": {"sl_pct": 5.0, "tp1_pct": 6.5, "tp2_pct": 10.0},
        "1h": {"sl_pct": 6.0, "tp1_pct": 8.0, "tp2_pct": 12.0},
    },
    "position_by_tier": {
        "observation": {
            "conservative_pct": 3.0,
            "aggressive_pct": 5.0,
            "conservative_max_lev": 1.5,
            "aggressive_max_lev": 2.0,
        },
        "attention": {
            "conservative_pct": 5.0,
            "aggressive_pct": 8.0,
            "conservative_max_lev": 1.8,
            "aggressive_max_lev": 2.5,
        },
        "high_priority": {
            "conservative_pct": 8.0,
            "aggressive_pct": 12.0,
            "conservative_max_lev": 2.0,
            "aggressive_max_lev": 3.0,
        },
    },
}


@dataclass
class AlertEvent:
    symbol: str
    window: str
    direction: str
    change_pct: float
    price: float
    rule_name: str
    reasons: List[str] = field(default_factory=list)
    level: str = "medium"
    event_time: Optional[datetime] = None
    change_1h_pct: Optional[float] = None
    change_24h_pct: Optional[float] = None
    mc: Optional[float] = None
    fdv: Optional[float] = None
    trigger_source: Optional[str] = None
    candle_state: Optional[str] = None
    confidence: Optional[float] = None
    confidence_band: Optional[str] = None
    rvol: Optional[float] = None
    alert_tier: Optional[str] = None
    repeat_count: int = 1
    merged_count: int = 0
    merged_peak_change_pct: Optional[float] = None
    coalesced_windows: List[str] = field(default_factory=list)
    coalesced_changes: Dict[str, float] = field(default_factory=dict)
    coalesced_confidences: Dict[str, float] = field(default_factory=dict)

    def direction_key(self) -> str:
        direction = (self.direction or "").strip().lower()
        if direction in {"up", "long", "bullish"}:
            return "up"
        if direction in {"down", "short", "bearish"}:
            return "down"
        return "flat"

    def normalized_level(self) -> str:
        level = (self.level or "medium").lower().strip()
        return level if level in VALID_LEVELS else "medium"

    def direction_label(self) -> str:
        key = self.direction_key()
        if key == "up":
            return "上涨"
        if key == "down":
            return "下跌"
        return "未知"

    def direction_icon(self) -> str:
        key = self.direction_key()
        if key == "up":
            return "📈"
        if key == "down":
            return "📉"
        return "↔️"

    def window_label(self) -> str:
        text = str(self.window or "").strip()
        return text if text else "N/A"

    def trigger_source_key(self) -> str:
        source = str(self.trigger_source or "").strip().lower()
        if source in {"ws", "poll"}:
            return source
        return "unknown"

    def trigger_source_label(self) -> str:
        key = self.trigger_source_key()
        if key == "ws":
            return "WS瀹炴椂"
        if key == "poll":
            return "杞"
        return "鏈煡"

    def candle_state_key(self) -> str:
        state = str(self.candle_state or "").strip().lower()
        if state in {"open", "closed"}:
            return state
        return "unknown"

    def candle_state_label(self) -> str:
        key = self.candle_state_key()
        if key == "open":
            return "寮€K"
        if key == "closed":
            return "鏀禟"
        return "鏈煡"

    def format_time(self) -> str:
        dt = self.event_time if isinstance(self.event_time, datetime) else datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt_bj = dt.astimezone(BEIJING_TZ)
        return dt_bj.strftime("%Y-%m-%d %H:%M:%S UTC+8")


class AlertNotifier:
    def __init__(
        self,
        token: str,
        chat_id: str,
        telegram_enabled: bool = True,
        send_low_priority: bool = False,
        push_policy: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.token = (token or "").strip()
        self.chat_id = (chat_id or "").strip()
        self.telegram_enabled = bool(telegram_enabled)
        self.send_low_priority = bool(send_low_priority)
        self.push_policy = _normalize_push_policy(push_policy)

        self.queue_enabled = _parse_bool(os.getenv("TELEGRAM_RETRY_QUEUE_ENABLED", "true"), default=True)
        self.max_queue_size = max(100, _safe_int(os.getenv("TELEGRAM_RETRY_QUEUE_MAX_SIZE"), 5000))
        self.max_retry_attempts = max(1, _safe_int(os.getenv("TELEGRAM_RETRY_MAX_ATTEMPTS"), 30))
        self.retry_base_delay_sec = max(0.2, _safe_float(os.getenv("TELEGRAM_RETRY_BASE_DELAY_SEC"), 1.0))
        self.retry_max_delay_sec = max(2.0, _safe_float(os.getenv("TELEGRAM_RETRY_MAX_DELAY_SEC"), 120.0))
        self.worker_count = max(1, _safe_int(os.getenv("TELEGRAM_RETRY_WORKERS"), 1))

        queue_file = os.getenv("TELEGRAM_RETRY_QUEUE_FILE", "/app/telegram_retry_queue.json")
        failed_file = os.getenv("TELEGRAM_RETRY_FAILED_FILE", "/app/telegram_retry_failed.jsonl")
        daily_count_file = os.getenv("TELEGRAM_DAILY_COUNT_FILE", "/app/telegram_daily_counts.json")
        self.queue_file = Path(queue_file)
        self.failed_file = Path(failed_file)
        self.daily_count_file = Path(daily_count_file)

        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._pending: Dict[str, Dict[str, Any]] = {}
        self._retry_tasks: Dict[str, asyncio.Task] = {}
        self._workers: List[asyncio.Task] = []
        self._started = False
        self._start_lock = asyncio.Lock()
        self._persist_lock = asyncio.Lock()
        self._stop_event = asyncio.Event()
        self._daily_push_counts: Dict[str, int] = {}
        self._daily_push_date = self._beijing_today()
        self._daily_count_lock = asyncio.Lock()
        self._daily_counts_loaded = False
        self._policy_lock = asyncio.Lock()
        self._repeat_state: Dict[str, List[float]] = {}
        self._merge_state: Dict[str, Dict[str, float]] = {}
        self._symbol_hourly_state: Dict[str, List[float]] = {}
        self._cross_window_state: Dict[str, Dict[str, Any]] = {}
        self._cross_window_tasks: Dict[str, asyncio.Task] = {}
        self._cross_window_lock = asyncio.Lock()

    @classmethod
    def from_config(cls, cfg: dict) -> "AlertNotifier":
        telegram = cfg.get("telegram", {}) if isinstance(cfg, dict) else {}
        push_policy = cfg.get("push_policy", {}) if isinstance(cfg, dict) else {}
        return cls(
            token=str(telegram.get("token", "")),
            chat_id=str(telegram.get("chat_id", "")),
            telegram_enabled=bool(telegram.get("enabled", True)),
            send_low_priority=bool(telegram.get("send_low_priority", False)),
            push_policy=push_policy if isinstance(push_policy, dict) else {},
        )

    async def start(self) -> None:
        if self._started:
            return
        async with self._start_lock:
            if self._started:
                return
            self._stop_event.clear()
            await self._ensure_daily_counts_loaded()
            await self._load_pending()
            for item_id in list(self._pending.keys()):
                self._queue.put_nowait(item_id)
            self._workers = [
                asyncio.create_task(self._worker_loop(idx), name=f"tg_sender_worker_{idx}")
                for idx in range(self.worker_count)
            ]
            self._started = True
            logger.info(
                "Notifier queue started: enabled=%s pending=%s workers=%s queue_file=%s",
                self.queue_enabled,
                len(self._pending),
                self.worker_count,
                self.queue_file,
            )

    async def stop(self) -> None:
        if not self._started:
            return
        self._stop_event.set()

        for task in list(self._cross_window_tasks.values()):
            task.cancel()
        self._cross_window_tasks.clear()

        for task in list(self._retry_tasks.values()):
            task.cancel()
        self._retry_tasks.clear()

        for worker in self._workers:
            worker.cancel()
        if self._workers:
            await asyncio.gather(*self._workers, return_exceptions=True)
        self._workers = []

        await self._persist_pending()
        self._started = False
        logger.info("Notifier queue stopped: pending=%s", len(self._pending))

    def stats(self) -> Dict[str, int]:
        return {
            "pending": len(self._pending),
            "in_memory_queue": self._queue.qsize(),
            "scheduled_retries": len(self._retry_tasks),
        }

    async def notify(self, event: AlertEvent) -> bool:
        return await self._notify_internal(event=event, allow_cross_window_merge=True)

    async def _notify_internal(self, event: AlertEvent, allow_cross_window_merge: bool) -> bool:
        level = event.normalized_level()

        if level == "low":
            logger.info("[LOW] %s", self._log_line(event))
            if not self.send_low_priority:
                return True

        if not self.telegram_enabled:
            logger.info("Telegram disabled, skip send: %s", self._log_line(event))
            return False

        if not self.token or not self.chat_id:
            logger.error("Telegram token/chat_id missing, cannot send alert.")
            return False

        if allow_cross_window_merge:
            merged_event = await self._cross_window_accept_or_buffer(event)
            if merged_event is None:
                return True
            event = merged_event

        policy_allowed = await self._apply_push_policy(event)
        if not policy_allowed:
            return True

        daily_push_count = await self._next_daily_push_count(event.symbol)
        message = self.build_message(event, daily_push_count=daily_push_count)

        if not self.queue_enabled:
            ok = await self._send_now(message)
            if ok:
                await self._mark_cross_window_sent(event)
                logger.info("Alert sent: %s", self._log_line(event))
            else:
                logger.error("Alert send failed: %s", self._log_line(event))
            return ok

        await self.start()
        item = self._build_queue_item(event=event, message=message)
        await self._enqueue_item(item)
        await self._mark_cross_window_sent(event)
        logger.info("Alert queued: %s | queue_stats=%s", self._log_line(event), self.stats())
        return True

    async def notify_from_dict(self, payload: dict) -> bool:
        event = AlertEvent(
            symbol=str(payload.get("symbol", "")),
            window=str(payload.get("window", "")),
            direction=str(payload.get("direction", "")),
            change_pct=float(payload.get("change_pct", 0.0)),
            price=float(payload.get("price", 0.0)),
            rule_name=str(payload.get("rule_name", "unknown_rule")),
            reasons=[str(x) for x in (payload.get("reasons") or [])],
            level=str(payload.get("level", "medium")),
            event_time=payload.get("event_time"),
            change_1h_pct=_safe_float(payload.get("change_1h_pct"), default=None),
            change_24h_pct=_safe_float(payload.get("change_24h_pct"), default=None),
            mc=_safe_float(payload.get("mc"), default=None),
            fdv=_safe_float(payload.get("fdv"), default=None),
            trigger_source=str(payload.get("trigger_source", "")),
            candle_state=str(payload.get("candle_state", "")),
            confidence=_safe_float(payload.get("confidence"), default=None),
            confidence_band=str(payload.get("confidence_band", "")),
            rvol=_safe_float(payload.get("rvol"), default=None),
        )
        return await self.notify(event)

    async def notify_text(
        self,
        message: str,
        *,
        symbol: str = "",
        rule_name: str = "strategy_alert",
        level: str = "medium",
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        normalized_level = str(level or "medium").strip().lower()
        if normalized_level == "low" and not self.send_low_priority:
            logger.info("[LOW] Strategy alert skipped by low-priority policy: %s %s", symbol, rule_name)
            return True

        if not self.telegram_enabled:
            logger.info("Telegram disabled, skip strategy alert: %s %s", symbol, rule_name)
            return False

        if not self.token or not self.chat_id:
            logger.error("Telegram token/chat_id missing, cannot send strategy alert.")
            return False

        if not self.queue_enabled:
            ok = await self._send_now(message)
            if ok:
                logger.info("Strategy alert sent: symbol=%s rule=%s level=%s", symbol, rule_name, normalized_level)
            else:
                logger.error("Strategy alert send failed: symbol=%s rule=%s level=%s", symbol, rule_name, normalized_level)
            return ok

        await self.start()
        item = self._build_text_queue_item(
            message=message,
            symbol=symbol,
            rule_name=rule_name,
            level=normalized_level,
            metadata=metadata or {},
        )
        await self._enqueue_item(item)
        logger.info(
            "Strategy alert queued: symbol=%s rule=%s level=%s | queue_stats=%s",
            symbol,
            rule_name,
            normalized_level,
            self.stats(),
        )
        return True

    def _cross_window_runtime_cfg(self) -> Tuple[bool, Set[str], float, float, float]:
        enabled = _parse_bool(self.push_policy.get("cross_window_merge_enabled"), default=True)
        raw_windows = self.push_policy.get("cross_window_merge_windows", [])
        if not isinstance(raw_windows, list):
            raw_windows = []
        windows = {str(w).strip() for w in raw_windows if str(w).strip()}
        if not windows:
            windows = {"15m", "30m", "1h"}
        coalesce_sec = max(1.0, _safe_float(self.push_policy.get("cross_window_coalesce_sec"), 90.0) or 90.0)
        upgrade_sec = max(
            coalesce_sec,
            _safe_float(self.push_policy.get("cross_window_upgrade_sec"), 180.0) or 180.0,
        )
        upgrade_score_delta = max(
            0.0,
            _safe_float(self.push_policy.get("cross_window_upgrade_min_score_delta"), 8.0) or 8.0,
        )
        return enabled, windows, coalesce_sec, upgrade_sec, upgrade_score_delta

    @staticmethod
    def _window_priority(window: str) -> int:
        mapping = {"1m": 10, "5m": 20, "15m": 30, "30m": 40, "1h": 50}
        return mapping.get(str(window or "").strip(), 0)

    @staticmethod
    def _cross_window_key(event: AlertEvent) -> str:
        return f"{event.symbol.upper()}:{event.direction_key()}:{event.rule_name}"

    @staticmethod
    def _cross_window_opposite_key(event: AlertEvent) -> Optional[str]:
        direction = event.direction_key()
        if direction == "up":
            opposite = "down"
        elif direction == "down":
            opposite = "up"
        else:
            return None
        return f"{event.symbol.upper()}:{opposite}:{event.rule_name}"

    @staticmethod
    def _seed_coalesced_payload(event: AlertEvent) -> None:
        window = event.window_label()
        if window and window not in event.coalesced_windows:
            event.coalesced_windows.append(window)
        if window:
            event.coalesced_changes[window] = float(event.change_pct)
        score = _safe_float(event.confidence, default=None)
        if window and score is not None:
            event.coalesced_confidences[window] = float(score)

    @staticmethod
    def _merge_coalesced_payload(target: AlertEvent, source: AlertEvent) -> None:
        for window in source.coalesced_windows:
            if window not in target.coalesced_windows:
                target.coalesced_windows.append(window)
        for window, val in source.coalesced_changes.items():
            target.coalesced_changes[str(window)] = float(val)
        for window, val in source.coalesced_confidences.items():
            target.coalesced_confidences[str(window)] = float(val)

    def _select_better_coalesced_event(self, left: AlertEvent, right: AlertEvent) -> AlertEvent:
        def _rank(event: AlertEvent) -> Tuple[float, float, float, float]:
            window_score = float(self._window_priority(event.window_label()))
            confidence = _safe_float(event.confidence, default=-1.0) or -1.0
            abs_change = abs(float(event.change_pct))
            rvol = _safe_float(event.rvol, default=0.0) or 0.0
            return (window_score, confidence, abs_change, rvol)

        return right if _rank(right) > _rank(left) else left

    def _is_cross_window_enabled_for_event(self, event: AlertEvent, windows: Set[str]) -> bool:
        if event.direction_key() not in {"up", "down"}:
            return False
        if not event.symbol or not event.rule_name:
            return False
        return event.window_label() in windows

    def _cross_window_is_upgrade(self, event: AlertEvent, state: Dict[str, Any], score_delta: float) -> bool:
        prev_window = str(state.get("last_sent_window", "")).strip()
        prev_score = _safe_float(state.get("last_sent_score"), default=None)
        curr_score = _safe_float(event.confidence, default=None)
        if self._window_priority(event.window_label()) > self._window_priority(prev_window):
            return True
        if prev_score is not None and curr_score is not None and (curr_score - prev_score) >= float(score_delta):
            return True
        return False

    def _prune_cross_window_state_locked(self, now_ts: float, stale_sec: float) -> None:
        for key in list(self._cross_window_state.keys()):
            state = self._cross_window_state.get(key, {})
            pending_deadline = _safe_float(state.get("pending_deadline_ts"), default=0.0) or 0.0
            if pending_deadline > 0 and (now_ts - pending_deadline) > stale_sec:
                state.pop("pending_event", None)
                state.pop("pending_deadline_ts", None)
            last_sent_ts = _safe_float(state.get("last_sent_ts"), default=0.0) or 0.0
            has_pending = "pending_event" in state
            if not has_pending and last_sent_ts > 0 and (now_ts - last_sent_ts) > stale_sec:
                self._cross_window_state.pop(key, None)

    def _schedule_cross_window_flush_locked(self, key: str, delay_sec: float) -> None:
        task = self._cross_window_tasks.get(key)
        if task is not None and not task.done():
            return

        async def _runner() -> None:
            try:
                await asyncio.sleep(max(0.05, float(delay_sec)))
                await self._run_cross_window_flush(key)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Cross-window flush task failed: %s", key)
            finally:
                self._cross_window_tasks.pop(key, None)

        safe_name = key.replace(":", "_")[:40]
        self._cross_window_tasks[key] = asyncio.create_task(_runner(), name=f"cross_window_flush_{safe_name}")

    async def _run_cross_window_flush(self, key: str) -> None:
        event_to_send: Optional[AlertEvent] = None
        async with self._cross_window_lock:
            state = self._cross_window_state.get(key)
            if not isinstance(state, dict):
                return
            now_ts = time.time()
            pending_deadline = _safe_float(state.get("pending_deadline_ts"), default=0.0) or 0.0
            if pending_deadline > (now_ts + 0.2):
                self._schedule_cross_window_flush_locked(key=key, delay_sec=(pending_deadline - now_ts))
                return
            pending_event = state.pop("pending_event", None)
            state.pop("pending_deadline_ts", None)
            if isinstance(pending_event, AlertEvent):
                event_to_send = pending_event
            if not state:
                self._cross_window_state.pop(key, None)

        if event_to_send is None:
            return

        accepted = await self._notify_internal(event=event_to_send, allow_cross_window_merge=False)
        logger.info("Cross-window flush sent: key=%s accepted=%s", key, accepted)

    async def _cross_window_accept_or_buffer(self, event: AlertEvent) -> Optional[AlertEvent]:
        enabled, windows, coalesce_sec, upgrade_sec, upgrade_score_delta = self._cross_window_runtime_cfg()
        if not enabled or not self._is_cross_window_enabled_for_event(event, windows):
            return event

        incoming = copy.deepcopy(event)
        self._seed_coalesced_payload(incoming)
        key = self._cross_window_key(incoming)
        now_ts = time.time()

        async with self._cross_window_lock:
            stale_sec = max(3600.0, coalesce_sec * 6.0, upgrade_sec * 4.0)
            self._prune_cross_window_state_locked(now_ts=now_ts, stale_sec=stale_sec)

            # Reversal pass:
            # if opposite direction was sent recently, allow immediate push for direction flip.
            opposite_key = self._cross_window_opposite_key(incoming)
            if opposite_key:
                opposite_state = self._cross_window_state.get(opposite_key, {})
                if isinstance(opposite_state, dict):
                    opposite_last_sent_ts = _safe_float(opposite_state.get("last_sent_ts"), default=0.0) or 0.0
                    if opposite_last_sent_ts > 0 and (now_ts - opposite_last_sent_ts) < upgrade_sec:
                        opposite_state.pop("pending_event", None)
                        opposite_state.pop("pending_deadline_ts", None)
                        if not opposite_state:
                            self._cross_window_state.pop(opposite_key, None)
                        logger.info(
                            "Cross-window reversal pass: from=%s to=%s window=%s",
                            opposite_key,
                            key,
                            incoming.window_label(),
                        )
                        return incoming
                    if "pending_event" in opposite_state:
                        opposite_state.pop("pending_event", None)
                        opposite_state.pop("pending_deadline_ts", None)
                        if not opposite_state:
                            self._cross_window_state.pop(opposite_key, None)

            state = self._cross_window_state.setdefault(key, {})

            last_sent_ts = _safe_float(state.get("last_sent_ts"), default=0.0) or 0.0
            if last_sent_ts > 0 and (now_ts - last_sent_ts) < upgrade_sec:
                if self._cross_window_is_upgrade(incoming, state, score_delta=upgrade_score_delta):
                    logger.info("Cross-window upgrade pass: key=%s window=%s", key, incoming.window_label())
                    state.pop("pending_event", None)
                    state.pop("pending_deadline_ts", None)
                    return incoming
                logger.info("Cross-window upgrade skip: key=%s window=%s", key, incoming.window_label())
                return None

            pending = state.get("pending_event")
            if not isinstance(pending, AlertEvent):
                state["pending_event"] = incoming
                state["pending_deadline_ts"] = now_ts + coalesce_sec
                self._schedule_cross_window_flush_locked(key=key, delay_sec=coalesce_sec)
                logger.info("Cross-window buffer start: key=%s window=%s", key, incoming.window_label())
                return None

            self._seed_coalesced_payload(pending)
            self._merge_coalesced_payload(pending, incoming)
            best = self._select_better_coalesced_event(pending, incoming)
            if best is incoming:
                self._merge_coalesced_payload(incoming, pending)
                state["pending_event"] = incoming
            else:
                state["pending_event"] = pending

            pending_deadline = _safe_float(state.get("pending_deadline_ts"), default=0.0) or 0.0
            if pending_deadline <= now_ts:
                self._schedule_cross_window_flush_locked(key=key, delay_sec=0.1)

            current = state.get("pending_event")
            if isinstance(current, AlertEvent):
                logger.info(
                    "Cross-window buffer merge: key=%s pick=%s windows=%s",
                    key,
                    current.window_label(),
                    "/".join(sorted(current.coalesced_windows, key=self._window_priority)),
                )
            return None

    async def _mark_cross_window_sent(self, event: AlertEvent) -> None:
        enabled, windows, _, _, _ = self._cross_window_runtime_cfg()
        if not enabled or not self._is_cross_window_enabled_for_event(event, windows):
            return

        key = self._cross_window_key(event)
        now_ts = time.time()
        score = _safe_float(event.confidence, default=None)
        rvol = _safe_float(event.rvol, default=0.0) or 0.0
        abs_change = abs(float(event.change_pct))

        async with self._cross_window_lock:
            state = self._cross_window_state.setdefault(key, {})
            state["last_sent_ts"] = now_ts
            state["last_sent_window"] = event.window_label()
            state["last_sent_score"] = float(score) if score is not None else -1.0
            state["last_sent_abs_change"] = abs_change
            state["last_sent_rvol"] = float(rvol)
            state.pop("pending_event", None)
            state.pop("pending_deadline_ts", None)

    def build_message(self, event: AlertEvent, daily_push_count: int) -> str:
        tier_label = self._tier_label(event.alert_tier)
        binance_link = f"https://www.binance.com/futures/{event.symbol}"
        confidence_band = str(event.confidence_band or "").strip().upper() or "N/A"
        token_name = self._token_name(event.symbol)
        direction_badge = f"{event.direction_icon()} {event.direction_label()}"

        lines = [
            f"**{token_name}（今日第{daily_push_count}次推送） | {direction_badge} | {tier_label}**",
            (
                f"核心变化：**{event.change_pct:+.2f}%**"
                f"   现价：**{event.price:.6f}**"
            ),
            f"1h/24h：**{self._format_change_pct(event.change_1h_pct)} / {self._format_change_pct(event.change_24h_pct)}**",
            f"MC/FDV：**{self._format_compact_usd(event.mc)} / {self._format_compact_usd(event.fdv)}**",
            f"时间：**{event.format_time()}**",
            "",
            f"触发依据：**{self._trigger_basis_summary(event, confidence_band)}**",
        ]
        coalesced_change_line = self._coalesced_change_summary(event)
        if coalesced_change_line:
            lines.append(f"周期变化：**{coalesced_change_line}**")

        flow_summary = self._flow_metrics_summary(event)
        if flow_summary:
            lines.append(f"量能指标：**{flow_summary}**")
        if event.window_label() == "1m" and event.repeat_count >= 2:
            lines.append(f"1m同向连发：**{event.repeat_count} 次（10分钟内）**")
        if event.merged_count > 0:
            peak_text = (
                f"{event.merged_peak_change_pct:+.2f}%"
                if event.merged_peak_change_pct is not None
                else "N/A"
            )
            lines.append(
                f"合并更新：**8 分钟内同向额外触发 {event.merged_count} 次，峰值 {peak_text}**"
            )
        lines.append(binance_link)
        return "\n".join(lines)


    async def _worker_loop(self, worker_id: int) -> None:
        while not self._stop_event.is_set():
            try:
                item_id = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                raise

            try:
                item = self._pending.get(item_id)
                if item is None:
                    continue

                next_retry_ts = _safe_float(item.get("next_retry_ts"), 0.0)
                now_ts = time.time()
                if next_retry_ts > now_ts:
                    self._schedule_retry(item_id=item_id, delay_sec=max(0.1, next_retry_ts - now_ts))
                    continue

                ok = await self._send_now(str(item.get("message", "")))
                if ok:
                    self._pending.pop(item_id, None)
                    await self._persist_pending()
                    logger.info(
                        "Alert delivered by queue worker=%s item=%s symbol=%s rule=%s attempts=%s",
                        worker_id,
                        item_id,
                        item.get("event", {}).get("symbol"),
                        item.get("event", {}).get("rule_name"),
                        item.get("attempts", 0),
                    )
                    continue

                attempts = _safe_int(item.get("attempts"), 0) + 1
                item["attempts"] = attempts
                item["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                if attempts > self.max_retry_attempts:
                    await self._write_failed(item, reason="max_retry_exceeded")
                    self._pending.pop(item_id, None)
                    await self._persist_pending()
                    logger.error(
                        "Alert dropped after retries: item=%s symbol=%s rule=%s attempts=%s",
                        item_id,
                        item.get("event", {}).get("symbol"),
                        item.get("event", {}).get("rule_name"),
                        attempts,
                    )
                    continue

                delay_sec = min(self.retry_max_delay_sec, self.retry_base_delay_sec * (2 ** (attempts - 1)))
                item["next_retry_ts"] = time.time() + delay_sec
                await self._persist_pending()
                self._schedule_retry(item_id=item_id, delay_sec=delay_sec)
                logger.warning(
                    "Alert retry scheduled: item=%s symbol=%s rule=%s attempts=%s delay=%.1fs",
                    item_id,
                    item.get("event", {}).get("symbol"),
                    item.get("event", {}).get("rule_name"),
                    attempts,
                    delay_sec,
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Notifier worker loop error: worker=%s", worker_id)
            finally:
                self._queue.task_done()

    async def _send_now(self, message: str) -> bool:
        try:
            return await send_telegram_alert(
                token=self.token,
                chat_id=self.chat_id,
                message=message,
            )
        except Exception:
            logger.exception("Alert delivery raised unexpectedly.")
            return False

    def _build_queue_item(self, event: AlertEvent, message: str) -> Dict[str, Any]:
        item_id = uuid.uuid4().hex
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return {
            "id": item_id,
            "created_at": now_text,
            "updated_at": now_text,
            "attempts": 0,
            "next_retry_ts": 0.0,
            "message": message,
            "event": {
                "symbol": event.symbol,
                "window": event.window,
                "direction": event.direction,
                "change_pct": event.change_pct,
                "price": event.price,
                "rule_name": event.rule_name,
                "level": event.normalized_level(),
                "reasons": list(event.reasons),
                "event_time": event.format_time(),
                "change_1h_pct": event.change_1h_pct,
                "change_24h_pct": event.change_24h_pct,
                "mc": event.mc,
                "fdv": event.fdv,
                "trigger_source": event.trigger_source_key(),
                "candle_state": event.candle_state_key(),
                "confidence": event.confidence,
                "confidence_band": event.confidence_band,
                "rvol": event.rvol,
                "alert_tier": event.alert_tier,
                "repeat_count": event.repeat_count,
                "merged_count": event.merged_count,
                "merged_peak_change_pct": event.merged_peak_change_pct,
                "coalesced_windows": list(event.coalesced_windows),
                "coalesced_changes": dict(event.coalesced_changes),
                "coalesced_confidences": dict(event.coalesced_confidences),
            },
        }

    def _build_text_queue_item(
        self,
        *,
        message: str,
        symbol: str,
        rule_name: str,
        level: str,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        item_id = uuid.uuid4().hex
        now_text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        event = {
            "symbol": str(symbol or ""),
            "window": str(metadata.get("window", "")),
            "direction": str(metadata.get("direction", "")),
            "rule_name": str(rule_name or "strategy_alert"),
            "level": str(level or "medium"),
            "event_time": now_text,
            "kind": "strategy_alert",
        }
        event.update(metadata or {})
        return {
            "id": item_id,
            "created_at": now_text,
            "updated_at": now_text,
            "attempts": 0,
            "next_retry_ts": 0.0,
            "message": message,
            "event": event,
        }

    async def _enqueue_item(self, item: Dict[str, Any]) -> None:
        if len(self._pending) >= self.max_queue_size:
            self._drop_oldest_pending()
        item_id = str(item["id"])
        self._pending[item_id] = item
        await self._persist_pending()
        self._queue.put_nowait(item_id)

    def _drop_oldest_pending(self) -> None:
        if not self._pending:
            return
        oldest_id = min(
            self._pending.keys(),
            key=lambda k: str(self._pending[k].get("created_at", "")),
        )
        oldest = self._pending.pop(oldest_id)
        logger.warning(
            "Notifier queue full, dropped oldest pending alert: item=%s symbol=%s rule=%s",
            oldest_id,
            oldest.get("event", {}).get("symbol"),
            oldest.get("event", {}).get("rule_name"),
        )

    def _schedule_retry(self, item_id: str, delay_sec: float) -> None:
        existing = self._retry_tasks.get(item_id)
        if existing is not None and not existing.done():
            return

        async def _requeue() -> None:
            try:
                await asyncio.sleep(max(0.1, delay_sec))
                if self._stop_event.is_set():
                    return
                if item_id in self._pending:
                    self._queue.put_nowait(item_id)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Retry scheduling failed: item=%s", item_id)
            finally:
                self._retry_tasks.pop(item_id, None)

        self._retry_tasks[item_id] = asyncio.create_task(_requeue(), name=f"tg_retry_{item_id[:8]}")

    async def _load_pending(self) -> None:
        if not self.queue_enabled:
            return
        if not self.queue_file.exists():
            return

        try:
            content = await asyncio.to_thread(self.queue_file.read_text, "utf-8")
            if not content.strip():
                return
            payload = json.loads(content)
            raw_items = payload.get("pending", []) if isinstance(payload, dict) else []
            if not isinstance(raw_items, list):
                return
            restored = 0
            for raw in raw_items:
                if not isinstance(raw, dict):
                    continue
                item_id = str(raw.get("id") or uuid.uuid4().hex)
                raw["id"] = item_id
                self._pending[item_id] = raw
                restored += 1
            if restored > 0:
                logger.info("Notifier restored pending queue items: %s", restored)
        except Exception:
            logger.exception("Failed to load notifier queue file: %s", self.queue_file)

    async def _persist_pending(self) -> None:
        if not self.queue_enabled:
            return
        async with self._persist_lock:
            try:
                self.queue_file.parent.mkdir(parents=True, exist_ok=True)
                payload = {"pending": list(self._pending.values())}
                tmp_path = self.queue_file.with_suffix(self.queue_file.suffix + ".tmp")
                text = json.dumps(payload, ensure_ascii=False, indent=2)
                await asyncio.to_thread(tmp_path.write_text, text, "utf-8")
                await asyncio.to_thread(tmp_path.replace, self.queue_file)
            except Exception:
                logger.exception("Failed to persist notifier queue file: %s", self.queue_file)

    async def _write_failed(self, item: Dict[str, Any], reason: str) -> None:
        try:
            self.failed_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "reason": reason,
                "failed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "item": item,
            }
            line = json.dumps(payload, ensure_ascii=False) + "\n"
            await asyncio.to_thread(self._append_text, self.failed_file, line)
        except Exception:
            logger.exception("Failed to append notifier dead-letter file: %s", self.failed_file)

    @staticmethod
    def _append_text(path: Path, text: str) -> None:
        with path.open("a", encoding="utf-8") as f:
            f.write(text)

    def _title_line(self, level: str) -> str:
        if level == "high":
            return "馃毃 楂樹紭鍏堢骇 甯佸畨寮傚姩璀︽姤"
        if level == "medium":
            return "鈿狅笍 涓紭鍏堢骇 甯佸畨寮傚姩璀︽姤"
        return "鈩癸笍 浣庝紭鍏堢骇 甯佸畨寮傚姩璀︽姤"

    def _signal_name(self, event: AlertEvent) -> str:
        direction = event.direction_key()
        if event.rule_name == "short_breakout":
            if direction == "up":
                return "短期暴涨突破"
            if direction == "down":
                return "短期急跌破位"
            return "短期价格异动突破"
        if event.rule_name == "trend_acceleration":
            if direction == "up":
                return "放量加速上涨"
            if direction == "down":
                return "放量加速下跌"
            return "放量加速异动"
        if event.rule_name == "long_anomaly":
            if direction == "up":
                return "极端波动异常（上涨）"
            if direction == "down":
                return "极端波动异常（下跌）"
            return "极端波动异常"
        return "价格异动信号"

    def _signal_confirm(self, reasons: Iterable[str]) -> str:
        tags: List[str] = []
        for raw in reasons:
            reason = str(raw)
            if ("Volume Delta 强多头" in reason) or ("Volume Delta Bullish" in reason):
                tags.append("Volume Delta 多头确认")
            if ("Volume Delta 强空头" in reason) or ("Volume Delta Bearish" in reason):
                tags.append("Volume Delta 空头确认")
            if ("放量" in reason) or ("RVOL" in reason) or ("成交量倍数" in reason):
                tags.append("放量确认")
                continue
            if ("OBV 上升" in reason) or ("OBV 下降" in reason):
                tags.append("OBV趋势确认")
                continue
            if "RSI" in reason:
                tags.append("RSI确认")
                continue
            if "MACD" in reason:
                tags.append("MACD确认")
                continue
            if ("布林" in reason) or ("BB" in reason):
                tags.append("布林带突破")
                continue
            if ("偏离均线" in reason) or ("MA" in reason and "STD" in reason):
                tags.append("均线偏离异常")
                continue
            if "ATR" in reason or "绝对波动" in reason:
                tags.append("波动突破确认")
                continue
            if "强制触发阈值" in reason:
                tags.append("强势突破确认")
                continue
            if ("固定阈值" in reason) or ("最低门槛" in reason) or ("绝对涨跌幅" in reason):
                tags.append("价格突破确认")
                continue

        if not tags:
            return "多条件共振"
        dedup: List[str] = []
        for tag in tags:
            if tag not in dedup:
                dedup.append(tag)
        return "、".join(dedup)

    def _volume_desc(self, reasons: Iterable[str]) -> str:
        for raw in reasons:
            reason = str(raw)
            has_obv = "OBV" in reason
            has_volume = ("放量" in reason) or ("RVOL" in reason) or ("成交量倍数" in reason)
            if not (has_obv and has_volume):
                continue

            ratio_text = "放量"
            ratio_match = re.search(r"([0-9]+(?:\\.[0-9]+)?)x", reason)
            if ratio_match:
                ratio = _safe_float(ratio_match.group(1), 0.0)
                if ratio and ratio > 0:
                    ratio_text = f"放量 {ratio:.1f}x"

            if ("Volume Delta 强多头" in reason) or ("Volume Delta Bullish" in reason):
                return f"{ratio_text} + OBV 上升 + Volume Delta 强多头"
            if ("Volume Delta 强空头" in reason) or ("Volume Delta Bearish" in reason):
                if "OBV 下降" in reason:
                    return f"{ratio_text} + OBV 下降 + Volume Delta 强空头"
                return f"{ratio_text} + OBV 上升 + Volume Delta 强空头"
            if "OBV 下降" in reason:
                return f"{ratio_text} + OBV 下降确认"
            return f"{ratio_text} + OBV 上升确认"
        return ""

    async def _apply_push_policy(self, event: AlertEvent) -> bool:
        now_ts = time.time()
        async with self._policy_lock:
            self._prune_policy_state(now_ts)
            self._hydrate_event_quality(event)
            is_repeat_attention = self._mark_repeat_and_is_attention(event, now_ts)
            event.alert_tier = self._compute_alert_tier(event, is_repeat_attention)

            should_send, merged_count, merged_peak = self._precheck_merge_window(event, now_ts)
            if not should_send:
                logger.info("Merge-window skip: %s merged=%s", self._log_line(event), merged_count)
                return False

            if not self._allow_symbol_hourly_push(event.symbol, now_ts):
                self._accumulate_merge_skip(event, now_ts)
                logger.info(
                    "Hourly-cap skip: %s cap=%s/h",
                    self._log_line(event),
                    int(self.push_policy["symbol_hourly_max_pushes"]),
                )
                return False

            event.merged_count = max(0, merged_count)
            event.merged_peak_change_pct = merged_peak
            self._mark_merge_sent(event, now_ts)
            return True

    def _prune_policy_state(self, now_ts: float) -> None:
        repeat_window_sec = float(self.push_policy["repeat_window_minutes"]) * 60.0
        for key in list(self._repeat_state.keys()):
            recent = [x for x in self._repeat_state.get(key, []) if now_ts - x <= repeat_window_sec]
            if recent:
                self._repeat_state[key] = recent
            else:
                self._repeat_state.pop(key, None)

        for key in list(self._symbol_hourly_state.keys()):
            recent = [x for x in self._symbol_hourly_state.get(key, []) if now_ts - x <= 3600.0]
            if recent:
                self._symbol_hourly_state[key] = recent
            else:
                self._symbol_hourly_state.pop(key, None)

        stale_sec = max(3600.0, float(self.push_policy["merge_window_minutes"]) * 60.0 * 10.0)
        for key in list(self._merge_state.keys()):
            state = self._merge_state.get(key, {})
            last_ts = _safe_float(state.get("last_ts"), 0.0) or 0.0
            if now_ts - last_ts > stale_sec:
                self._merge_state.pop(key, None)

    def _hydrate_event_quality(self, event: AlertEvent) -> None:
        if event.rvol is None:
            event.rvol = self._extract_rvol(event.reasons)

        band = str(event.confidence_band or "").strip().upper()
        if band in {"A", "B", "C", "D"}:
            event.confidence_band = band
            return

        score = _safe_float(event.confidence, default=None)
        if score is None:
            event.confidence_band = "N/A"
        elif score >= 85:
            event.confidence_band = "A"
        elif score >= 70:
            event.confidence_band = "B"
        elif score >= 60:
            event.confidence_band = "C"
        else:
            event.confidence_band = "D"

    def _mark_repeat_and_is_attention(self, event: AlertEvent, now_ts: float) -> bool:
        key = f"{event.symbol.upper()}:{event.direction_key()}"
        repeat_window_sec = float(self.push_policy["repeat_window_minutes"]) * 60.0
        recent = [x for x in self._repeat_state.get(key, []) if now_ts - x <= repeat_window_sec]
        prior_count = len(recent)
        recent.append(now_ts)
        self._repeat_state[key] = recent
        event.repeat_count = len(recent)
        return event.window_label() == "1m" and prior_count >= 1

    def _compute_alert_tier(self, event: AlertEvent, is_repeat_attention: bool) -> str:
        high_cfg = self.push_policy.get("high_priority", {})
        high_window = str(high_cfg.get("window", "5m")).strip() or "5m"
        high_conf = _safe_float(high_cfg.get("confidence_min"), 85.0) or 85.0
        high_rvol = _safe_float(high_cfg.get("rvol_min"), 2.0) or 2.0
        score = _safe_float(event.confidence, default=None)
        rvol = _safe_float(event.rvol, default=0.0) or 0.0

        if event.window_label() == high_window and score is not None and score >= high_conf and rvol >= high_rvol:
            return "high_priority"

        attention_windows = self.push_policy.get("attention_windows", [])
        if isinstance(attention_windows, list) and event.window_label() in [str(x).strip() for x in attention_windows]:
            return "attention"
        if is_repeat_attention:
            return "attention"
        return "observation"

    def _merge_key(self, event: AlertEvent) -> str:
        return f"{event.symbol.upper()}:{event.window_label()}:{event.direction_key()}:{event.rule_name}"

    def _get_merge_state(self, event: AlertEvent) -> Dict[str, float]:
        key = self._merge_key(event)
        state = self._merge_state.get(key)
        if state is None:
            state = {
                "last_sent_ts": 0.0,
                "suppressed": 0.0,
                "peak_abs_change": 0.0,
                "last_change_pct": 0.0,
                "last_price": 0.0,
                "last_ts": 0.0,
            }
            self._merge_state[key] = state
        return state

    @staticmethod
    def _update_merge_metrics(state: Dict[str, float], event: AlertEvent, now_ts: float) -> None:
        abs_change = abs(float(event.change_pct))
        state["peak_abs_change"] = max(float(state.get("peak_abs_change", 0.0)), abs_change)
        state["last_change_pct"] = float(event.change_pct)
        state["last_price"] = float(event.price)
        state["last_ts"] = float(now_ts)

    def _precheck_merge_window(self, event: AlertEvent, now_ts: float) -> tuple[bool, int, Optional[float]]:
        state = self._get_merge_state(event)
        merge_window_sec = float(self.push_policy["merge_window_minutes"]) * 60.0
        last_sent_ts = float(state.get("last_sent_ts", 0.0))

        if last_sent_ts > 0 and (now_ts - last_sent_ts) < merge_window_sec:
            state["suppressed"] = float(state.get("suppressed", 0.0)) + 1.0
            self._update_merge_metrics(state, event, now_ts)
            return False, int(state["suppressed"]), float(state.get("peak_abs_change", 0.0))

        merged_count = int(state.get("suppressed", 0.0))
        merged_peak = float(state.get("peak_abs_change", 0.0)) if merged_count > 0 else None
        return True, merged_count, merged_peak

    def _accumulate_merge_skip(self, event: AlertEvent, now_ts: float) -> None:
        state = self._get_merge_state(event)
        state["suppressed"] = float(state.get("suppressed", 0.0)) + 1.0
        self._update_merge_metrics(state, event, now_ts)

    def _mark_merge_sent(self, event: AlertEvent, now_ts: float) -> None:
        state = self._get_merge_state(event)
        state["last_sent_ts"] = float(now_ts)
        state["suppressed"] = 0.0
        state["peak_abs_change"] = abs(float(event.change_pct))
        state["last_change_pct"] = float(event.change_pct)
        state["last_price"] = float(event.price)
        state["last_ts"] = float(now_ts)

    def _allow_symbol_hourly_push(self, symbol: str, now_ts: float) -> bool:
        sym = str(symbol or "").strip().upper()
        if not sym:
            return True
        hourly_cap = max(1, _safe_int(self.push_policy.get("symbol_hourly_max_pushes"), 6))
        recent = [x for x in self._symbol_hourly_state.get(sym, []) if now_ts - x <= 3600.0]
        if len(recent) >= hourly_cap:
            self._symbol_hourly_state[sym] = recent
            return False
        recent.append(now_ts)
        self._symbol_hourly_state[sym] = recent
        return True

    @staticmethod
    def _tier_label(tier: Optional[str]) -> str:
        key = str(tier or "").strip().lower()
        if key == "high_priority":
            return "高优先级"
        if key == "attention":
            return "关注级"
        return "观察级"

    @staticmethod
    def _token_name(symbol: str) -> str:
        text = str(symbol or "").strip().upper()
        for suffix in ("USDT", "USDC", "FDUSD", "BUSD", "USD"):
            if text.endswith(suffix) and len(text) > len(suffix):
                return text[: -len(suffix)]
        return text or "UNKNOWN"

    def _trigger_basis_summary(self, event: AlertEvent, confidence_band: str) -> str:
        windows = event.coalesced_windows[:] if event.coalesced_windows else [event.window_label()]
        windows = [w for w in windows if str(w).strip()]
        if not windows:
            windows = [event.window_label()]
        windows_sorted = sorted(set(windows), key=self._window_priority)

        parts: List[str] = [f"{event.window_label()}主信号"]
        if len(windows_sorted) >= 2:
            parts.append(f"多周期共振 {'/'.join(windows_sorted)}")
        parts.append(f"信号强度 {self._format_confidence(event.confidence)}（{confidence_band}）")

        if event.window_label() == "1m" and event.repeat_count >= 2:
            parts.append(f"1m连发 {event.repeat_count}次/10m")
        if event.merged_count > 0:
            parts.append(f"近期合并 {event.merged_count}次")
        return " | ".join(parts)

    def _coalesced_change_summary(self, event: AlertEvent) -> str:
        values = dict(event.coalesced_changes or {})
        if not values:
            values[event.window_label()] = float(event.change_pct)
        windows_sorted = sorted(values.keys(), key=self._window_priority)
        if len(windows_sorted) <= 1:
            return ""
        parts: List[str] = []
        for w in windows_sorted:
            val = _safe_float(values.get(w), default=None)
            if val is None:
                continue
            parts.append(f"{w} {val:+.2f}%")
        return " | ".join(parts)

    def _flow_metrics_summary(self, event: AlertEvent) -> str:
        parts: List[str] = []
        if event.rvol is not None:
            parts.append(f"RVOL {event.rvol:.2f}x")

        has_obv = False
        obv_text = ""
        delta_text = ""
        for raw in event.reasons:
            reason = str(raw)
            if ("OBV 上升" in reason) or ("OBV rising" in reason):
                has_obv = True
                obv_text = "OBV 上升"
            elif ("OBV 下降" in reason) or ("OBV falling" in reason):
                has_obv = True
                obv_text = "OBV 下降"
            if ("Volume Delta 强多头" in reason) or ("Volume Delta Bullish" in reason):
                delta_text = "Volume Delta 强多头"
            elif ("Volume Delta 强空头" in reason) or ("Volume Delta Bearish" in reason):
                delta_text = "Volume Delta 强空头"

        if has_obv:
            parts.append(obv_text or "OBV")
        if delta_text:
            parts.append(delta_text)
        return " | ".join(parts)

    def _trade_plan_for_event(self, event: AlertEvent) -> Dict[str, Any]:
        window = event.window_label()
        cfg_map = self.push_policy.get("trade_params_by_window", {})
        if not isinstance(cfg_map, dict):
            cfg_map = {}
        params = cfg_map.get(window, cfg_map.get("1m", {}))
        sl_pct = max(0.1, _safe_float(params.get("sl_pct"), 1.4) or 1.4)
        tp1_pct = max(0.1, _safe_float(params.get("tp1_pct"), sl_pct * 1.2) or (sl_pct * 1.2))
        tp2_pct = max(tp1_pct, _safe_float(params.get("tp2_pct"), tp1_pct * 1.6) or (tp1_pct * 1.6))

        direction_key = event.direction_key()
        if direction_key == "up":
            invalid_price = event.price * (1.0 - sl_pct / 100.0)
            tp1_price = event.price * (1.0 + tp1_pct / 100.0)
            tp2_price = event.price * (1.0 + tp2_pct / 100.0)
            direction_text = "鍋氬"
            invalid_hint = "璺岀牬澶辨晥"
            tp1_pct_signed = f"+{tp1_pct:.2f}%"
            tp2_pct_signed = f"+{tp2_pct:.2f}%"
            sl_pct_signed = f"-{sl_pct:.2f}%"
        elif direction_key == "down":
            invalid_price = event.price * (1.0 + sl_pct / 100.0)
            tp1_price = event.price * (1.0 - tp1_pct / 100.0)
            tp2_price = event.price * (1.0 - tp2_pct / 100.0)
            direction_text = "鍋氱┖"
            invalid_hint = "鍗囩牬澶辨晥"
            tp1_pct_signed = f"+{tp1_pct:.2f}%"
            tp2_pct_signed = f"+{tp2_pct:.2f}%"
            sl_pct_signed = f"-{sl_pct:.2f}%"
        else:
            invalid_price = event.price
            tp1_price = event.price
            tp2_price = event.price
            direction_text = "瑙傚療"
            invalid_hint = "鏂瑰悜鏈畾"
            tp1_pct_signed = "N/A"
            tp2_pct_signed = "N/A"
            sl_pct_signed = "N/A"

        pos_cfg_map = self.push_policy.get("position_by_tier", {})
        if not isinstance(pos_cfg_map, dict):
            pos_cfg_map = {}
        tier_key = str(event.alert_tier or "observation").strip().lower()
        pos_cfg = pos_cfg_map.get(tier_key, pos_cfg_map.get("observation", {}))
        cons_pos = max(1.0, _safe_float(pos_cfg.get("conservative_pct"), 3.0) or 3.0)
        agg_pos = max(cons_pos, _safe_float(pos_cfg.get("aggressive_pct"), 5.0) or 5.0)
        cons_lev = max(1.0, _safe_float(pos_cfg.get("conservative_max_lev"), 1.5) or 1.5)
        agg_lev = max(cons_lev, _safe_float(pos_cfg.get("aggressive_max_lev"), 2.0) or 2.0)

        watch_cfg = self.push_policy.get("watch_bars_by_window", {})
        if not isinstance(watch_cfg, dict):
            watch_cfg = {}
        watch_bars = max(1, _safe_int(watch_cfg.get(window), 6))

        return {
            "direction_text": direction_text,
            "watch_bars": watch_bars,
            "invalid_price": invalid_price,
            "invalid_hint": invalid_hint,
            "sl_price": event.price if direction_key == "flat" else invalid_price,
            "tp1_price": tp1_price,
            "tp2_price": tp2_price,
            "cons_pos_pct": cons_pos,
            "agg_pos_pct": agg_pos,
            "cons_lev": cons_lev,
            "agg_lev": agg_lev,
            "tp1_pct_signed": tp1_pct_signed,
            "tp2_pct_signed": tp2_pct_signed,
            "sl_pct_signed": sl_pct_signed,
        }

    @staticmethod
    def _extract_rvol(reasons: Iterable[str]) -> Optional[float]:
        values: List[float] = []
        for raw in reasons:
            reason = str(raw)
            if not any(k in reason for k in ["RVOL", "鎴愪氦閲忓€嶆暟", "鏀鹃噺", "Volume"]):
                continue
            for text in re.findall(r"([0-9]+(?:\.[0-9]+)?)x", reason):
                num = _safe_float(text, default=None)
                if num is not None and num > 0:
                    values.append(float(num))
        if not values:
            return None
        return max(values)

    def _action_suggestion(self, event: AlertEvent) -> str:
        level = event.normalized_level()
        direction = event.direction_key()

        if level == "high":
            if direction == "up":
                return "建议立即关注，波动可能继续放大，务必控制风险。"
            if direction == "down":
                return "建议立即关注，下跌动能可能延续，谨慎应对。"
            return "建议立即关注，波动可能继续放大。"
        if level == "medium":
            if direction == "up":
                return "可继续观察上行动能，注意回撤风险。"
            if direction == "down":
                return "可继续观察下跌延续，避免逆势重仓。"
            return "建议先观察确认，再决定是否参与。"
        return "以观察为主，避免激进操作。"

    @staticmethod
    def _beijing_today() -> str:
        return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d")

    async def _next_daily_push_count(self, symbol: str) -> int:
        await self._ensure_daily_counts_loaded()
        today = self._beijing_today()
        normalized_symbol = str(symbol or "").strip().upper()

        async with self._daily_count_lock:
            if today != self._daily_push_date:
                self._daily_push_date = today
                self._daily_push_counts.clear()

            current = self._daily_push_counts.get(normalized_symbol, 0) + 1
            self._daily_push_counts[normalized_symbol] = current
            await self._persist_daily_counts()
            return current

    async def _ensure_daily_counts_loaded(self) -> None:
        if self._daily_counts_loaded:
            return

        async with self._daily_count_lock:
            if self._daily_counts_loaded:
                return

            await self._load_daily_counts()
            self._daily_counts_loaded = True

    async def _load_daily_counts(self) -> None:
        if not self.daily_count_file.exists():
            self._daily_push_date = self._beijing_today()
            self._daily_push_counts = {}
            return

        try:
            content = await asyncio.to_thread(self.daily_count_file.read_text, "utf-8")
            if not content.strip():
                self._daily_push_date = self._beijing_today()
                self._daily_push_counts = {}
                return

            payload = json.loads(content)
            if not isinstance(payload, dict):
                self._daily_push_date = self._beijing_today()
                self._daily_push_counts = {}
                return

            file_date = str(payload.get("date", "")).strip()
            counts = payload.get("counts", {})
            if not isinstance(counts, dict):
                counts = {}

            today = self._beijing_today()
            if file_date != today:
                self._daily_push_date = today
                self._daily_push_counts = {}
                return

            normalized: Dict[str, int] = {}
            for key, value in counts.items():
                symbol = str(key or "").strip().upper()
                if not symbol:
                    continue
                try:
                    count = int(value)
                except (TypeError, ValueError):
                    continue
                if count > 0:
                    normalized[symbol] = count

            self._daily_push_date = today
            self._daily_push_counts = normalized
            if normalized:
                logger.info("Daily push counts restored: date=%s symbols=%s", today, len(normalized))
        except Exception:
            logger.exception("Failed to load daily push counts: %s", self.daily_count_file)
            self._daily_push_date = self._beijing_today()
            self._daily_push_counts = {}

    async def _persist_daily_counts(self) -> None:
        try:
            self.daily_count_file.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "date": self._daily_push_date,
                "counts": self._daily_push_counts,
            }
            tmp_path = self.daily_count_file.with_suffix(self.daily_count_file.suffix + ".tmp")
            text = json.dumps(payload, ensure_ascii=False, indent=2)
            await asyncio.to_thread(tmp_path.write_text, text, "utf-8")
            await asyncio.to_thread(tmp_path.replace, self.daily_count_file)
        except Exception:
            logger.exception("Failed to persist daily push counts: %s", self.daily_count_file)

    def _log_line(self, event: AlertEvent) -> str:
        return (
            f"{event.symbol} | {event.rule_name} | {event.window} | "
            f"{event.direction_label()} | {event.change_pct:+.2f}% | "
            f"price={event.price:.6f} | source={event.trigger_source_key()} | "
            f"candle={event.candle_state_key()} | conf={self._format_confidence(event.confidence)} | "
            f"tier={event.alert_tier or 'observation'} | rvol={event.rvol if event.rvol is not None else 'N/A'} | "
            f"merged={event.merged_count}"
        )

    @staticmethod
    def _format_change_pct(value: Optional[float]) -> str:
        if value is None:
            return "N/A"
        return f"{value:+.2f}%"

    @staticmethod
    def _format_confidence(value: Optional[float]) -> str:
        if value is None:
            return "N/A"
        confidence = max(0.0, min(100.0, float(value)))
        return f"{confidence:.1f}/100"

    @staticmethod
    def _format_compact_usd(value: Optional[float]) -> str:
        if value is None or value <= 0:
            return "N/A"

        if value >= 1_000_000_000:
            return f"${value / 1_000_000_000:.2f}b"
        if value >= 1_000_000:
            return f"${value / 1_000_000:.2f}m"
        return f"${value:.2f}"


def _deep_merge_dict(target: Dict[str, Any], source: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in source.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            _deep_merge_dict(target[key], value)
        else:
            target[key] = value
    return target


def _normalize_push_policy(raw: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    policy = copy.deepcopy(DEFAULT_PUSH_POLICY)
    if isinstance(raw, dict):
        _deep_merge_dict(policy, raw)

    policy["repeat_window_minutes"] = max(1, _safe_int(policy.get("repeat_window_minutes"), 10))
    policy["merge_window_minutes"] = max(1, _safe_int(policy.get("merge_window_minutes"), 8))
    policy["symbol_hourly_max_pushes"] = max(1, _safe_int(policy.get("symbol_hourly_max_pushes"), 6))

    policy["cross_window_merge_enabled"] = _parse_bool(
        policy.get("cross_window_merge_enabled"),
        default=True,
    )
    raw_cross_windows = policy.get("cross_window_merge_windows", [])
    if not isinstance(raw_cross_windows, list):
        raw_cross_windows = []
    normalized_cross_windows = [
        str(w).strip() for w in raw_cross_windows if str(w).strip() in {"15m", "30m", "1h"}
    ]
    if not normalized_cross_windows:
        normalized_cross_windows = ["15m", "30m", "1h"]
    policy["cross_window_merge_windows"] = normalized_cross_windows
    coalesce_sec = max(5.0, _safe_float(policy.get("cross_window_coalesce_sec"), 90.0) or 90.0)
    policy["cross_window_coalesce_sec"] = float(coalesce_sec)
    policy["cross_window_upgrade_sec"] = float(
        max(
            coalesce_sec,
            _safe_float(policy.get("cross_window_upgrade_sec"), 180.0) or 180.0,
        )
    )
    policy["cross_window_upgrade_min_score_delta"] = float(
        max(
            0.0,
            _safe_float(policy.get("cross_window_upgrade_min_score_delta"), 8.0) or 8.0,
        )
    )

    attention_windows = policy.get("attention_windows", [])
    if not isinstance(attention_windows, list):
        attention_windows = []
    policy["attention_windows"] = [str(x).strip() for x in attention_windows if str(x).strip()]
    if not policy["attention_windows"]:
        policy["attention_windows"] = ["5m"]

    high = policy.get("high_priority", {})
    if not isinstance(high, dict):
        high = {}
    high["window"] = str(high.get("window", "5m")).strip() or "5m"
    high["confidence_min"] = max(0.0, min(100.0, _safe_float(high.get("confidence_min"), 85.0) or 85.0))
    high["rvol_min"] = max(0.0, _safe_float(high.get("rvol_min"), 2.0) or 2.0)
    policy["high_priority"] = high

    watch = policy.get("watch_bars_by_window", {})
    if not isinstance(watch, dict):
        watch = {}
    normalized_watch: Dict[str, int] = {}
    for window, default in DEFAULT_PUSH_POLICY["watch_bars_by_window"].items():
        normalized_watch[str(window)] = max(1, _safe_int(watch.get(window), default))
    policy["watch_bars_by_window"] = normalized_watch

    trade_map = policy.get("trade_params_by_window", {})
    if not isinstance(trade_map, dict):
        trade_map = {}
    normalized_trade: Dict[str, Dict[str, float]] = {}
    for window, defaults in DEFAULT_PUSH_POLICY["trade_params_by_window"].items():
        src = trade_map.get(window, {})
        if not isinstance(src, dict):
            src = {}
        sl = max(0.1, _safe_float(src.get("sl_pct"), defaults["sl_pct"]) or defaults["sl_pct"])
        tp1 = max(0.1, _safe_float(src.get("tp1_pct"), defaults["tp1_pct"]) or defaults["tp1_pct"])
        tp2 = max(tp1, _safe_float(src.get("tp2_pct"), defaults["tp2_pct"]) or defaults["tp2_pct"])
        normalized_trade[window] = {
            "sl_pct": float(sl),
            "tp1_pct": float(tp1),
            "tp2_pct": float(tp2),
        }
    policy["trade_params_by_window"] = normalized_trade

    pos_map = policy.get("position_by_tier", {})
    if not isinstance(pos_map, dict):
        pos_map = {}
    normalized_pos: Dict[str, Dict[str, float]] = {}
    for tier, defaults in DEFAULT_PUSH_POLICY["position_by_tier"].items():
        src = pos_map.get(tier, {})
        if not isinstance(src, dict):
            src = {}
        cons_pos = max(1.0, _safe_float(src.get("conservative_pct"), defaults["conservative_pct"]) or defaults["conservative_pct"])
        agg_pos = max(cons_pos, _safe_float(src.get("aggressive_pct"), defaults["aggressive_pct"]) or defaults["aggressive_pct"])
        cons_lev = max(1.0, _safe_float(src.get("conservative_max_lev"), defaults["conservative_max_lev"]) or defaults["conservative_max_lev"])
        agg_lev = max(cons_lev, _safe_float(src.get("aggressive_max_lev"), defaults["aggressive_max_lev"]) or defaults["aggressive_max_lev"])
        normalized_pos[tier] = {
            "conservative_pct": float(cons_pos),
            "aggressive_pct": float(agg_pos),
            "conservative_max_lev": float(cons_lev),
            "aggressive_max_lev": float(agg_lev),
        }
    policy["position_by_tier"] = normalized_pos

    return policy


def _parse_bool(value: str, default: bool = False) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
