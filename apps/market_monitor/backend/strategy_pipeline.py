from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

try:
    from alerts.event_deduper import EventDeduper
    from alerts.alert_policy import AlertDecision, AlertPolicy
    from alerts.tg_formatter import format_strategy_alert
    from alerts.trade_confirmation import confirmation_count, confirmation_stage, evaluate_trade_confirmation
    from candidates.candidate_engine import CandidateEngine
    from candidates.event_store import RawEventStore
    from candidates.raw_event import RawEvent
    from factors.factor_enricher import FactorEnricher
    from scoring.candidate_score import score_candidate
    from scoring.risk_score import score_risk
except ModuleNotFoundError:
    from apps.market_monitor.backend.alerts.event_deduper import EventDeduper
    from apps.market_monitor.backend.alerts.alert_policy import AlertDecision, AlertPolicy
    from apps.market_monitor.backend.alerts.tg_formatter import format_strategy_alert
    from apps.market_monitor.backend.alerts.trade_confirmation import (
        confirmation_count,
        confirmation_stage,
        evaluate_trade_confirmation,
    )
    from apps.market_monitor.backend.candidates.candidate_engine import CandidateEngine
    from apps.market_monitor.backend.candidates.event_store import RawEventStore
    from apps.market_monitor.backend.candidates.raw_event import RawEvent
    from apps.market_monitor.backend.factors.factor_enricher import FactorEnricher
    from apps.market_monitor.backend.scoring.candidate_score import score_candidate
    from apps.market_monitor.backend.scoring.risk_score import score_risk

logger = logging.getLogger(__name__)


@dataclass
class StrategyProcessResult:
    candidate_id: str
    alert_sent: bool
    alert_type: str
    reason: str
    candidate_score: float
    risk_score: float


class AlertStrategyPipeline:
    def __init__(self, settings: Optional[Dict[str, Any]] = None) -> None:
        self.settings = settings or {}
        self.event_store = RawEventStore(self.settings)
        self.candidate_engine = CandidateEngine(self.settings)
        self.policy = AlertPolicy(self.settings)
        self.event_deduper = EventDeduper(self.settings)
        raw_factor_settings = self.settings.get("confirmation_factors", {}) if isinstance(self.settings, dict) else {}
        factor_settings = dict(raw_factor_settings) if isinstance(raw_factor_settings, dict) else {}
        if self.settings.get("runtime_dir") and not factor_settings.get("runtime_dir"):
            factor_settings["runtime_dir"] = self.settings.get("runtime_dir")
        self.factor_enricher = FactorEnricher(factor_settings)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "AlertStrategyPipeline":
        raw_settings = config.get("alert_strategy", {}) if isinstance(config, dict) else {}
        settings = raw_settings if isinstance(raw_settings, dict) else {}
        return cls(settings=settings)

    @property
    def enabled(self) -> bool:
        return _parse_bool(self.settings.get("enabled"), True)

    @property
    def direct_tg_enabled(self) -> bool:
        return _parse_bool(self.settings.get("direct_tg_enabled"), False)

    async def process_event(self, payload: Dict[str, Any], *, source: str, notifier: Any) -> StrategyProcessResult:
        raw_event = RawEvent.from_alert_payload(payload, source=source)
        self.event_store.append(raw_event)

        candidate = self.candidate_engine.update_candidate(raw_event)
        score, score_breakdown, priority = score_candidate(candidate)
        risk_score_value, risk_level, risk_breakdown = score_risk(candidate)

        candidate.score = score
        candidate.score_breakdown = score_breakdown
        candidate.priority = priority
        candidate.risk_score = risk_score_value
        candidate.risk_level = risk_level
        candidate.risk_breakdown = risk_breakdown
        self.candidate_engine.save()

        if self._is_startup_event(payload):
            if self.factor_enricher.should_enrich(candidate, base_score=score):
                candidate = await self.factor_enricher.enrich(candidate)
                score, score_breakdown, priority = score_candidate(candidate)
                risk_score_value, risk_level, risk_breakdown = score_risk(candidate)
                candidate.score = score
                candidate.score_breakdown = score_breakdown
                candidate.priority = priority
                candidate.risk_score = risk_score_value
                candidate.risk_level = risk_level
                candidate.risk_breakdown = risk_breakdown
            self._apply_trade_confirmation(candidate)
            self.candidate_engine.save()

            startup_allowed, startup_reason = self._startup_allowed(candidate)
            alert_sent = False
            dedupe_decision = None
            decision = AlertDecision(False, alert_type="startup_alert", reason=startup_reason)
            if startup_allowed:
                decision = self.policy.allow_alert_type(
                    candidate.symbol,
                    "startup_alert",
                    cooldown_minutes=self._startup_cooldown_minutes(),
                )
                if decision.should_send:
                    dedupe_decision = self.event_deduper.decide(candidate, alert_type="startup_alert")
                    if dedupe_decision.should_send:
                        alert_sent = await self._send_decision(notifier=notifier, candidate=candidate, decision=decision)
                        if alert_sent:
                            self.event_deduper.mark_sent(candidate, alert_type="startup_alert")
                            self.policy.mark_sent(candidate.symbol, "startup_alert")
            logger.info(
                "Startup alert processed: symbol=%s score=%.1f risk=%.1f confirm=%s stage=%s sent=%s reason=%s dedupe=%s",
                candidate.symbol,
                candidate.score,
                candidate.risk_score,
                confirmation_count(candidate),
                confirmation_stage(candidate),
                alert_sent,
                decision.reason,
                dedupe_decision.reason if dedupe_decision else "not_checked",
            )
            return StrategyProcessResult(
                candidate_id=candidate.candidate_id,
                alert_sent=alert_sent,
                alert_type="startup_alert",
                reason=dedupe_decision.reason if dedupe_decision and not dedupe_decision.should_send else decision.reason,
                candidate_score=candidate.score,
                risk_score=candidate.risk_score,
            )

        if self._is_strong_direct_event(payload):
            candidate = await self.factor_enricher.enrich(candidate)
            score, score_breakdown, priority = score_candidate(candidate)
            risk_score_value, risk_level, risk_breakdown = score_risk(candidate)
            candidate.score = score
            candidate.score_breakdown = score_breakdown
            candidate.priority = priority
            candidate.risk_score = risk_score_value
            candidate.risk_level = risk_level
            candidate.risk_breakdown = risk_breakdown
            self._apply_trade_confirmation(candidate)
            self.candidate_engine.save()

            direct_allowed, direct_reason = self._strong_direct_confirmation_allowed(candidate)
            alert_sent = False
            dedupe_decision = None
            decision = AlertDecision(False, alert_type="strong_direct_alert", reason=direct_reason)
            if direct_allowed:
                decision = self.policy.allow_alert_type(
                    candidate.symbol,
                    "strong_direct_alert",
                    cooldown_minutes=self._strong_direct_cooldown_minutes(),
                )
                if decision.should_send:
                    dedupe_decision = self.event_deduper.decide(candidate, alert_type="strong_direct_alert")
                    if dedupe_decision.should_send:
                        alert_sent = await self._send_strong_direct(notifier=notifier, candidate=candidate)
                        if alert_sent:
                            self.event_deduper.mark_sent(candidate, alert_type="strong_direct_alert")
                            self.policy.mark_sent(candidate.symbol, "strong_direct_alert")
            logger.info(
                "Strong direct processed: symbol=%s score=%.1f risk=%.1f confirm=%s stage=%s sent=%s reason=%s dedupe=%s",
                candidate.symbol,
                candidate.score,
                candidate.risk_score,
                confirmation_count(candidate),
                confirmation_stage(candidate),
                alert_sent,
                decision.reason,
                dedupe_decision.reason if dedupe_decision else "not_checked",
            )
            return StrategyProcessResult(
                candidate_id=candidate.candidate_id,
                alert_sent=alert_sent,
                alert_type="strong_direct_alert",
                reason=dedupe_decision.reason if dedupe_decision and not dedupe_decision.should_send else decision.reason,
                candidate_score=candidate.score,
                risk_score=candidate.risk_score,
            )

        if self.factor_enricher.should_enrich(candidate, base_score=score):
            candidate = await self.factor_enricher.enrich(candidate)
            score, score_breakdown, priority = score_candidate(candidate)
            risk_score_value, risk_level, risk_breakdown = score_risk(candidate)

        candidate.score = score
        candidate.score_breakdown = score_breakdown
        candidate.priority = priority
        candidate.risk_score = risk_score_value
        candidate.risk_level = risk_level
        candidate.risk_breakdown = risk_breakdown
        self._apply_trade_confirmation(candidate)
        self.candidate_engine.save()

        decision = self.policy.decide(candidate)
        alert_sent = False
        dedupe_decision = None
        if decision.should_send:
            dedupe_decision = self.event_deduper.decide(candidate, alert_type=decision.alert_type)
            if dedupe_decision.should_send:
                alert_sent = await self._send_decision(notifier=notifier, candidate=candidate, decision=decision)
                if alert_sent:
                    self.event_deduper.mark_sent(candidate, alert_type=decision.alert_type)
                    self.policy.mark_sent(candidate.symbol, decision.alert_type)

        logger.info(
            "Strategy processed: symbol=%s score=%.1f risk=%.1f confirm=%s stage=%s priority=%s decision=%s sent=%s reason=%s dedupe=%s",
            candidate.symbol,
            candidate.score,
            candidate.risk_score,
            confirmation_count(candidate),
            confirmation_stage(candidate),
            candidate.priority,
            decision.alert_type,
            alert_sent,
            decision.reason,
            dedupe_decision.reason if dedupe_decision else "not_checked",
        )
        return StrategyProcessResult(
            candidate_id=candidate.candidate_id,
            alert_sent=alert_sent,
            alert_type=decision.alert_type,
            reason=dedupe_decision.reason if dedupe_decision and not dedupe_decision.should_send else decision.reason,
            candidate_score=candidate.score,
            risk_score=candidate.risk_score,
        )

    async def _send_decision(self, *, notifier: Any, candidate: Any, decision: AlertDecision) -> bool:
        detail_level = str(self.settings.get("telegram_detail_level") or "compact")
        message = format_strategy_alert(candidate, decision, detail_level=detail_level)
        level = "high" if decision.alert_type in {"actionable_alert", "risk_alert"} else "medium"
        if hasattr(notifier, "notify_text"):
            return await notifier.notify_text(
                message,
                symbol=candidate.symbol,
                rule_name=f"strategy_{decision.alert_type}",
                level=level,
                metadata={
                    "candidate_id": candidate.candidate_id,
                    "candidate_score": candidate.score,
                    "risk_score": candidate.risk_score,
                    "confirmation_count": confirmation_count(candidate),
                    "confirmation_stage": confirmation_stage(candidate),
                    "alert_type": decision.alert_type,
                },
            )
        logger.warning("Notifier does not support notify_text; strategy alert dropped: %s", candidate.symbol)
        return False

    def _is_startup_event(self, payload: Dict[str, Any]) -> bool:
        if not _parse_bool(self.settings.get("startup_alert_enabled"), True):
            return False
        if str(payload.get("rule_name") or "").strip() != "early_start":
            return False
        return str(payload.get("direction") or "").strip().lower() == "up"

    def _startup_allowed(self, candidate: Any) -> tuple[bool, str]:
        min_score = _to_float(self.settings.get("startup_alert_min_score"), 45.0)
        if float(candidate.score or 0.0) < min_score:
            return False, "startup_score_below_min"
        return True, "accepted"

    def _startup_cooldown_minutes(self) -> int:
        return max(1, _to_int(self.settings.get("startup_cooldown_minutes"), 45))

    def _is_strong_direct_event(self, payload: Dict[str, Any]) -> bool:
        if not _parse_bool(self.settings.get("strong_direct_enabled"), True):
            return False

        window = str(payload.get("window") or "").strip()
        windows = self.settings.get("strong_direct_windows", ["1m", "5m"])
        if not isinstance(windows, list):
            windows = ["1m", "5m"]
        if window not in {str(item).strip() for item in windows}:
            return False

        thresholds = self.settings.get("strong_direct_change_pct", {})
        if not isinstance(thresholds, dict):
            thresholds = {}
        threshold = _to_float(thresholds.get(window), 0.0)
        if threshold <= 0:
            return False

        change_pct = abs(_to_float(payload.get("change_pct"), 0.0))
        if change_pct < threshold:
            return False

        level = str(payload.get("level") or "").strip().lower()
        if level and level != "high":
            return False
        return True

    def _apply_trade_confirmation(self, candidate: Any) -> None:
        confirmation = evaluate_trade_confirmation(candidate, self.settings)
        candidate.confirmation = confirmation.to_dict()

    def _strong_direct_confirmation_allowed(self, candidate: Any) -> tuple[bool, str]:
        latest = candidate.latest_features or {}
        window = str(latest.get("window") or "").strip()
        direct_windows = self.settings.get("strong_direct_direct_windows", ["1m"])
        if not isinstance(direct_windows, list):
            direct_windows = ["1m"]
        allowed_windows = {str(item).strip() for item in direct_windows if str(item).strip()}
        if window not in allowed_windows:
            return False, "strong_direct_candidate_only_window"

        direction = str(latest.get("direction") or "").strip().lower()
        if direction not in {"up", "down"}:
            return False, "strong_direct_unknown_direction"

        min_score = self._strong_direct_direction_float(
            direction,
            suffix="min_score",
            fallback_key="strong_direct_min_score",
            default=60.0,
        )
        if float(candidate.score or 0.0) < min_score:
            return False, "strong_direct_score_below_confirmed_min"

        max_risk = self._strong_direct_direction_float(
            direction,
            suffix="max_risk",
            fallback_key="strong_direct_max_risk",
            default=70.0,
        )
        if max_risk > 0 and float(candidate.risk_score or 0.0) > max_risk:
            return False, "strong_direct_risk_above_confirmed_max"

        max_change = self._strong_direct_direction_float(
            direction,
            suffix="max_change_pct",
            fallback_key="strong_direct_max_change_pct",
            default=0.0,
        )
        change_pct = abs(_to_float(latest.get("change_pct"), 0.0))
        if max_change > 0 and change_pct >= max_change:
            return False, "strong_direct_change_above_direction_max"

        required = max(
            1,
            self._strong_direct_direction_int(
                direction,
                suffix="min_confirmations",
                fallback_key="trade_confirmation_min_strong_direct_confirmations",
                default=3,
            ),
        )
        if confirmation_count(candidate) < required:
            return False, "strong_direct_confirmation_pending"

        return True, "confirmed"

    def _strong_direct_cooldown_minutes(self) -> int:
        return max(1, _to_int(self.settings.get("strong_direct_cooldown_minutes"), 20))

    def _strong_direct_direction_float(
        self,
        direction: str,
        *,
        suffix: str,
        fallback_key: str,
        default: float,
    ) -> float:
        directional_key = f"strong_direct_{direction}_{suffix}"
        if directional_key in self.settings:
            return _to_float(self.settings.get(directional_key), default)
        return _to_float(self.settings.get(fallback_key), default)

    def _strong_direct_direction_int(
        self,
        direction: str,
        *,
        suffix: str,
        fallback_key: str,
        default: int,
    ) -> int:
        directional_key = f"strong_direct_{direction}_{suffix}"
        if directional_key in self.settings:
            return _to_int(self.settings.get(directional_key), default)
        return _to_int(self.settings.get(fallback_key), default)

    async def _send_strong_direct(self, *, notifier: Any, candidate: Any) -> bool:
        detail_level = str(self.settings.get("telegram_detail_level") or "compact")
        decision = AlertDecision(True, alert_type="strong_direct_alert", reason="strong_direct")
        message = format_strategy_alert(candidate, decision, detail_level=detail_level)
        if hasattr(notifier, "notify_text"):
            return await notifier.notify_text(
                message,
                symbol=candidate.symbol,
                rule_name="strategy_strong_direct_alert",
                level="high",
                metadata={
                    "candidate_id": candidate.candidate_id,
                    "candidate_score": candidate.score,
                    "risk_score": candidate.risk_score,
                    "confirmation_count": confirmation_count(candidate),
                    "confirmation_stage": confirmation_stage(candidate),
                    "alert_type": "strong_direct_alert",
                    "strong_direct": True,
                },
            )
        logger.warning("Notifier does not support notify_text; strong direct alert dropped: %s", candidate.symbol)
        return False


def _parse_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_float(value: Any, default: float) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default
