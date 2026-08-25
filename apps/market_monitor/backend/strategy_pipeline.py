from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from alerts.derivatives_shadow_recorder import DerivativesShadowRecorder
    from alerts.position_pressure_shadow_recorder import PositionPressureShadowRecorder
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
    from apps.market_monitor.backend.alerts.derivatives_shadow_recorder import DerivativesShadowRecorder
    from apps.market_monitor.backend.alerts.position_pressure_shadow_recorder import PositionPressureShadowRecorder
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
        factor_quality_file = str(self.settings.get("factor_quality_file") or "factor_quality.jsonl").strip()
        self.factor_quality_path = self.event_store.runtime_dir / (factor_quality_file or "factor_quality.jsonl")
        shadow_file = str(
            self.settings.get("actionable_policy_shadow_file") or "actionable_policy_shadow.jsonl"
        ).strip()
        self.actionable_policy_shadow_path = self.event_store.runtime_dir / (
            shadow_file or "actionable_policy_shadow.jsonl"
        )
        self.derivatives_shadow_recorder = DerivativesShadowRecorder(self.settings)
        self.position_pressure_shadow_recorder = PositionPressureShadowRecorder(self.settings)
        self._prewarm_last_ts: Dict[str, float] = {}
        self._prewarm_tasks: set[asyncio.Task] = set()

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

    def start_background_tasks(self) -> None:
        self.factor_enricher.start_background_tasks()

    async def close(self) -> None:
        tasks = list(self._prewarm_tasks)
        self._prewarm_tasks.clear()
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.factor_enricher.close()

    async def process_event(self, payload: Dict[str, Any], *, source: str, notifier: Any) -> StrategyProcessResult:
        raw_event = RawEvent.from_alert_payload(payload, source=source)
        self.event_store.append(raw_event)

        candidate = self.candidate_engine.update_candidate(raw_event)
        score, score_breakdown, priority = score_candidate(candidate)
        risk_score_value, risk_level, risk_breakdown = score_risk(candidate, self.settings)

        candidate.score = score
        candidate.score_breakdown = score_breakdown
        candidate.priority = priority
        candidate.risk_score = risk_score_value
        candidate.risk_level = risk_level
        candidate.risk_breakdown = risk_breakdown
        self.candidate_engine.save()
        self._schedule_hot_pool_prewarm(candidate, base_score=score)

        if self._is_startup_event(payload):
            if self.factor_enricher.should_enrich(candidate, base_score=score):
                candidate = await self.factor_enricher.enrich(candidate)
                self._recalculate_candidate(candidate)
            self._apply_trade_confirmation(candidate)
            self.candidate_engine.save()

            startup_allowed, startup_reason = self._startup_allowed(candidate)
            alert_sent = False
            dedupe_decision = None
            decision = AlertDecision(False, alert_type="startup_alert", reason=startup_reason)
            if startup_allowed:
                suppressed, suppress_reason = self._startup_suppressed_by_primary_alert(candidate)
                if suppressed:
                    decision = AlertDecision(False, alert_type="startup_alert", reason=suppress_reason)
                else:
                    decision = self.policy.allow_alert_type(
                        candidate.symbol,
                        "startup_alert",
                        cooldown_minutes=self._startup_cooldown_minutes(),
                    )
                    if decision.should_send:
                        candidate = await self._retry_factor_fill_if_needed(candidate, "startup_alert")
                        startup_allowed, startup_reason = self._startup_allowed(candidate)
                        if not startup_allowed:
                            decision = AlertDecision(False, alert_type="startup_alert", reason=startup_reason)
                            dedupe_decision = None
                        else:
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
            self._record_derivatives_shadow(
                candidate,
                decision=decision,
                alert_sent=alert_sent,
                dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
            )
            self._record_position_pressure_shadow(
                candidate,
                decision=decision,
                alert_sent=alert_sent,
                dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
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
            self._recalculate_candidate(candidate)
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
                    candidate = await self._retry_factor_fill_if_needed(candidate, "strong_direct_alert")
                    direct_allowed, direct_reason = self._strong_direct_confirmation_allowed(candidate)
                    if not direct_allowed:
                        decision = AlertDecision(False, alert_type="strong_direct_alert", reason=direct_reason)
                        dedupe_decision = None
                    else:
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
            self._record_derivatives_shadow(
                candidate,
                decision=decision,
                alert_sent=alert_sent,
                dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
            )
            self._record_position_pressure_shadow(
                candidate,
                decision=decision,
                alert_sent=alert_sent,
                dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
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
            self._recalculate_candidate(candidate)
        else:
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
            candidate = await self._retry_factor_fill_if_needed(candidate, decision.alert_type)
            decision = self.policy.decide(candidate)
        if decision.should_send:
            dedupe_decision = self.event_deduper.decide(candidate, alert_type=decision.alert_type)
            if dedupe_decision.should_send:
                alert_sent = await self._send_decision(notifier=notifier, candidate=candidate, decision=decision)
                if alert_sent:
                    self.event_deduper.mark_sent(candidate, alert_type=decision.alert_type)
                    self.policy.mark_sent(candidate.symbol, decision.alert_type)

        self._record_actionable_policy_shadow(
            candidate,
            decision=decision,
            alert_sent=alert_sent,
            dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
        )
        self._record_derivatives_shadow(
            candidate,
            decision=decision,
            alert_sent=alert_sent,
            dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
        )
        self._record_position_pressure_shadow(
            candidate,
            decision=decision,
            alert_sent=alert_sent,
            dedupe_reason=dedupe_decision.reason if dedupe_decision else "not_checked",
        )

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
            sent = await notifier.notify_text(
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
            if sent:
                self._record_factor_quality(candidate, decision.alert_type, decision.reason)
            return sent
        logger.warning("Notifier does not support notify_text; strategy alert dropped: %s", candidate.symbol)
        return False

    def _recalculate_candidate(self, candidate: Any) -> None:
        score, score_breakdown, priority = score_candidate(candidate)
        risk_score_value, risk_level, risk_breakdown = score_risk(candidate, self.settings)
        candidate.score = score
        candidate.score_breakdown = score_breakdown
        candidate.priority = priority
        candidate.risk_score = risk_score_value
        candidate.risk_level = risk_level
        candidate.risk_breakdown = risk_breakdown

    async def _retry_factor_fill_if_needed(self, candidate: Any, alert_type: str) -> Any:
        if not _parse_bool(self.settings.get("factor_retry_enabled"), True):
            return candidate
        retry_types = self._factor_retry_alert_types()
        if alert_type not in retry_types:
            return candidate
        if self._factor_completeness_pct(candidate) >= self._factor_retry_min_completeness_pct():
            return candidate

        delay_sec = max(0.0, min(10.0, _to_float(self.settings.get("factor_retry_delay_sec"), 4.0)))
        if delay_sec > 0:
            await asyncio.sleep(delay_sec)

        before_pct = self._factor_completeness_pct(candidate)
        candidate = await self.factor_enricher.enrich(candidate, force_refresh=True)
        self._recalculate_candidate(candidate)
        self._apply_trade_confirmation(candidate)
        self.candidate_engine.save()
        logger.info(
            "Factor retry completed: symbol=%s alert_type=%s completeness=%.1f->%.1f",
            candidate.symbol,
            alert_type,
            before_pct,
            self._factor_completeness_pct(candidate),
        )
        return candidate

    def _factor_retry_alert_types(self) -> set[str]:
        raw_types = self.settings.get(
            "factor_retry_alert_types",
            ["startup_alert", "strong_direct_alert", "risk_alert", "actionable_alert"],
        )
        if isinstance(raw_types, str):
            items = raw_types.split(",")
        elif isinstance(raw_types, list):
            items = raw_types
        else:
            items = ["startup_alert", "strong_direct_alert", "risk_alert", "actionable_alert"]
        allowed = {"startup_alert", "strong_direct_alert", "risk_alert", "actionable_alert"}
        return {str(item).strip() for item in items if str(item).strip() in allowed} or allowed

    def _factor_retry_min_completeness_pct(self) -> float:
        return max(0.0, min(100.0, _to_float(self.settings.get("factor_retry_min_completeness_pct"), 80.0)))

    def _factor_completeness_pct(self, candidate: Any) -> float:
        snapshot = candidate.factor_snapshot or {}
        completeness = snapshot.get("factor_completeness") if isinstance(snapshot, dict) else {}
        if not isinstance(completeness, dict):
            return 0.0
        return max(0.0, min(100.0, _to_float(completeness.get("pct"), 0.0)))

    def _record_derivatives_shadow(
        self,
        candidate: Any,
        *,
        decision: AlertDecision,
        alert_sent: bool,
        dedupe_reason: str,
    ) -> None:
        try:
            derivatives = candidate.derivatives if isinstance(candidate.derivatives, dict) else {}
            snapshot = candidate.factor_snapshot if isinstance(candidate.factor_snapshot, dict) else {}
            latest = candidate.latest_features if isinstance(candidate.latest_features, dict) else {}
            opportunity_modifier = _to_float(
                derivatives.get("derivatives_shadow_opportunity_modifier"),
                0.0,
            )
            risk_modifier = _to_float(derivatives.get("derivatives_shadow_risk_modifier"), 0.0)
            proposed_score = max(0.0, min(100.0, float(candidate.score or 0.0) + opportunity_modifier))
            proposed_risk = max(0.0, min(100.0, float(candidate.risk_score or 0.0) + risk_modifier))
            factor_keys = (
                "index_price",
                "mark_price",
                "market_mid_price",
                "basis_bps_now",
                "market_basis_bps",
                "mark_basis_bps",
                "basis_delta_5m_bps",
                "basis_delta_15m_bps",
                "basis_delta_1h_bps",
                "basis_delta_zscore_5m",
                "basis_delta_zscore_15m",
                "basis_percentile_24h",
                "basis_zscore_24h",
                "basis_shadow_status",
                "basis_shadow_state",
                "basis_shadow_extreme",
                "oi_change_pct_5m",
                "oi_change_pct_15m",
                "oi_change_pct_1h",
                "oi_amount_change_pct_5m",
                "oi_amount_change_pct_15m",
                "oi_amount_change_pct_1h",
                "oi_amount_zscore_5m",
                "oi_amount_zscore_15m",
                "oi_amount_shadow_status",
                "oi_turn_direction",
                "oi_turn_stage",
                "oi_turn_strength",
                "oi_regime",
                "oi_shadow_regime",
                "oi_shadow_signal_level",
                "oi_shadow_primary_window",
                "funding_rate",
                "funding_rate_signed",
                "funding_rate_8h",
                "funding_interval_hours",
                "funding_delta_15m_bps",
                "funding_delta_1h_bps",
                "funding_percentile_24h",
                "funding_shadow_status",
                "funding_shadow_state",
            )
            row = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "policy_version": derivatives.get("derivatives_shadow_policy_version")
                or self.settings.get("derivatives_shadow_policy_version")
                or "derivatives-v1-shadow",
                "symbol": candidate.symbol,
                "base_asset": getattr(candidate, "base_asset", ""),
                "candidate_id": candidate.candidate_id,
                "event_count": int(candidate.event_count or 0),
                "latest_event": {
                    "event_id": latest.get("event_id"),
                    "event_type": latest.get("event_type"),
                    "event_time": latest.get("event_time"),
                    "window": latest.get("window"),
                    "direction": latest.get("direction"),
                    "price": latest.get("price"),
                    "change_pct": latest.get("change_pct"),
                    "rvol": latest.get("rvol"),
                },
                "actual_decision": {
                    "alert_type": decision.alert_type,
                    "should_send": bool(decision.should_send),
                    "reason": decision.reason,
                    "dedupe_reason": dedupe_reason,
                    "alert_sent": bool(alert_sent),
                    "score": float(candidate.score or 0.0),
                    "risk_score": float(candidate.risk_score or 0.0),
                },
                "shadow_decision": {
                    "opportunity_modifier": opportunity_modifier,
                    "risk_modifier": risk_modifier,
                    "proposed_score": round(proposed_score, 2),
                    "proposed_risk_score": round(proposed_risk, 2),
                    "reasons": list(derivatives.get("derivatives_shadow_reasons") or []),
                },
                "factor_state": {key: derivatives.get(key) for key in factor_keys},
                "shadow_completeness": snapshot.get("derivatives_shadow_completeness")
                if isinstance(snapshot.get("derivatives_shadow_completeness"), dict)
                else {},
            }
            self.derivatives_shadow_recorder.record(row, alert_sent=alert_sent)
        except Exception as exc:
            logger.debug("Failed to record derivatives shadow: symbol=%s err=%s", candidate.symbol, exc)

    def _record_position_pressure_shadow(
        self,
        candidate: Any,
        *,
        decision: AlertDecision,
        alert_sent: bool,
        dedupe_reason: str,
    ) -> None:
        if not _parse_bool(self.settings.get("position_pressure_shadow_enabled"), True):
            return
        try:
            snapshot = candidate.factor_snapshot if isinstance(candidate.factor_snapshot, dict) else {}
            latest = candidate.latest_features if isinstance(candidate.latest_features, dict) else {}
            confirmation = candidate.confirmation if isinstance(candidate.confirmation, dict) else {}
            checks = confirmation.get("checks") if isinstance(confirmation.get("checks"), list) else []
            liquidation_passed = any(
                isinstance(check, dict)
                and str(check.get("key") or "") == "liquidation"
                and bool(check.get("passed"))
                for check in checks
            )
            pressure = candidate.position_pressure if isinstance(candidate.position_pressure, dict) else {}
            shadow_confirmation = bool(pressure.get("shadow_confirmation_passed"))
            current_confirmation_count = confirmation_count(candidate)
            proposed_confirmation_count = max(
                0,
                current_confirmation_count - int(liquidation_passed) + int(shadow_confirmation),
            )
            risk_modifier = _to_float(pressure.get("shadow_risk_modifier"), 0.0)
            row = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "policy_version": pressure.get("policy_version")
                or self.settings.get("position_pressure_policy_version")
                or "position-pressure-v2-display-risk-holdout",
                "symbol": candidate.symbol,
                "base_asset": getattr(candidate, "base_asset", ""),
                "candidate_id": candidate.candidate_id,
                "alert_type": decision.alert_type,
                "event_count": int(candidate.event_count or 0),
                "latest_event": {
                    "event_id": latest.get("event_id"),
                    "event_type": latest.get("event_type"),
                    "event_time": latest.get("event_time"),
                    "window": latest.get("window"),
                    "direction": latest.get("direction"),
                    "price": latest.get("price"),
                    "change_pct": latest.get("change_pct"),
                    "rvol": latest.get("rvol"),
                },
                "actual_decision": {
                    "alert_type": decision.alert_type,
                    "should_send": bool(decision.should_send),
                    "reason": decision.reason,
                    "dedupe_reason": dedupe_reason,
                    "alert_sent": bool(alert_sent),
                    "score": float(candidate.score or 0.0),
                    "risk_score": float(candidate.risk_score or 0.0),
                    "confirmation_count": current_confirmation_count,
                },
                "shadow_decision": {
                    "risk_modifier": risk_modifier,
                    "proposed_risk_score": round(min(100.0, float(candidate.risk_score or 0.0) + risk_modifier), 2),
                    "legacy_liquidation_confirmation": liquidation_passed,
                    "position_pressure_confirmation": shadow_confirmation,
                    "proposed_confirmation_count": proposed_confirmation_count,
                },
                "smart_money": dict(candidate.smart_money or {}),
                "liquidation_v2": dict(candidate.liquidation_v2 or {}),
                "position_pressure": dict(pressure),
                "position_pressure_completeness": (
                    snapshot.get("position_pressure_completeness")
                    if isinstance(snapshot.get("position_pressure_completeness"), dict)
                    else {}
                ),
                "source_health": snapshot.get("source_health")
                if isinstance(snapshot.get("source_health"), dict)
                else {},
            }
            self.position_pressure_shadow_recorder.record(row, alert_sent=alert_sent)
        except Exception as exc:
            logger.debug("Failed to record position pressure shadow: symbol=%s err=%s", candidate.symbol, exc)

    def _record_factor_quality(self, candidate: Any, alert_type: str, reason: str = "") -> None:
        try:
            snapshot = candidate.factor_snapshot or {}
            if not isinstance(snapshot, dict):
                snapshot = {}
            latest = candidate.latest_features or {}
            if not isinstance(latest, dict):
                latest = {}
            recent_events = candidate.recent_events if isinstance(candidate.recent_events, list) else []
            last_event = recent_events[-1] if recent_events and isinstance(recent_events[-1], dict) else {}
            row = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "symbol": candidate.symbol,
                "base_asset": getattr(candidate, "base_asset", ""),
                "candidate_id": candidate.candidate_id,
                "alert_type": alert_type,
                "reason": reason,
                "score": float(candidate.score or 0.0),
                "risk_score": float(candidate.risk_score or 0.0),
                "priority": getattr(candidate, "priority", ""),
                "confirmation_count": confirmation_count(candidate),
                "confirmation_stage": confirmation_stage(candidate),
                "event_count": int(candidate.event_count or 0),
                "latest_event": {
                    "event_type": latest.get("event_type") or last_event.get("event_type"),
                    "window": latest.get("window") or last_event.get("window"),
                    "direction": latest.get("direction") or last_event.get("direction"),
                    "event_time": latest.get("event_time") or last_event.get("event_time"),
                    "change_pct": latest.get("change_pct") if "change_pct" in latest else last_event.get("change_pct"),
                    "rvol": latest.get("rvol") if "rvol" in latest else last_event.get("rvol"),
                },
                "factor_updated_at": getattr(candidate, "factor_updated_at", "") or snapshot.get("updated_at"),
                "factor_completeness": snapshot.get("factor_completeness") if isinstance(snapshot.get("factor_completeness"), dict) else {},
                "source_health": snapshot.get("source_health") if isinstance(snapshot.get("source_health"), dict) else {},
            }
            self.factor_quality_path.parent.mkdir(parents=True, exist_ok=True)
            with self.factor_quality_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.debug("Failed to record factor quality: symbol=%s alert_type=%s err=%s", candidate.symbol, alert_type, exc)

    def _record_actionable_policy_shadow(
        self,
        candidate: Any,
        *,
        decision: AlertDecision,
        alert_sent: bool,
        dedupe_reason: str,
    ) -> None:
        if not _parse_bool(self.settings.get("actionable_policy_shadow_enabled"), True):
            return

        try:
            shadow = self.policy.evaluate_actionable_shadow(candidate)
            if not bool(shadow.get("tracked")):
                return

            snapshot = candidate.factor_snapshot if isinstance(candidate.factor_snapshot, dict) else {}
            latest = candidate.latest_features if isinstance(candidate.latest_features, dict) else {}
            derivatives = candidate.derivatives if isinstance(candidate.derivatives, dict) else {}
            orderbook = candidate.orderbook if isinstance(candidate.orderbook, dict) else {}
            confirmation = candidate.confirmation if isinstance(candidate.confirmation, dict) else {}
            recent_events = candidate.recent_events if isinstance(candidate.recent_events, list) else []
            last_event = recent_events[-1] if recent_events and isinstance(recent_events[-1], dict) else {}
            row = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "policy_version": shadow.get("policy_version"),
                "symbol": candidate.symbol,
                "base_asset": getattr(candidate, "base_asset", ""),
                "candidate_id": candidate.candidate_id,
                "cohort": shadow.get("cohort"),
                "current_passed": bool(shadow.get("current_passed")),
                "legacy_passed": bool(shadow.get("legacy_passed")),
                "current_failures": list(shadow.get("current_failures") or []),
                "legacy_failures": list(shadow.get("legacy_failures") or []),
                "current_edge_min_confirmations": shadow.get("current_edge_min_confirmations"),
                "legacy_edge_min_confirmations": shadow.get("legacy_edge_min_confirmations"),
                "actual_decision": {
                    "alert_type": decision.alert_type,
                    "should_send": bool(decision.should_send),
                    "reason": decision.reason,
                    "dedupe_reason": dedupe_reason,
                    "alert_sent": bool(alert_sent),
                },
                "score": float(candidate.score or 0.0),
                "risk_score": float(candidate.risk_score or 0.0),
                "priority": getattr(candidate, "priority", ""),
                "event_count": int(candidate.event_count or 0),
                "latest_event": {
                    "event_id": latest.get("event_id") or last_event.get("event_id"),
                    "event_type": latest.get("event_type") or last_event.get("event_type"),
                    "window": latest.get("window") or last_event.get("window"),
                    "direction": latest.get("direction") or last_event.get("direction"),
                    "event_time": latest.get("event_time") or last_event.get("event_time"),
                    "price": latest.get("price"),
                    "change_pct": latest.get("change_pct") if "change_pct" in latest else last_event.get("change_pct"),
                    "rvol": latest.get("rvol") if "rvol" in latest else last_event.get("rvol"),
                },
                "confirmation": {
                    "passed_count": confirmation_count(candidate),
                    "stage": confirmation_stage(candidate),
                    "checks": confirmation.get("checks") if isinstance(confirmation.get("checks"), list) else [],
                },
                "factor_completeness": (
                    snapshot.get("factor_completeness")
                    if isinstance(snapshot.get("factor_completeness"), dict)
                    else {}
                ),
                "factor_state": {
                    "oi_signal_level": derivatives.get("oi_signal_level"),
                    "oi_regime": derivatives.get("oi_regime"),
                    "micro_signal_level": derivatives.get("micro_signal_level"),
                    "micro_regime": derivatives.get("micro_regime"),
                    "taker_buy_ratio": derivatives.get("taker_buy_ratio"),
                    "buy_aggressor_ratio_1m": derivatives.get("buy_aggressor_ratio_1m"),
                    "cvd_usdt_1m": derivatives.get("cvd_usdt_1m"),
                    "orderbook_imbalance": orderbook.get("imbalance"),
                    "orderbook_spread_bps": orderbook.get("spread_bps"),
                },
            }
            self.actionable_policy_shadow_path.parent.mkdir(parents=True, exist_ok=True)
            with self.actionable_policy_shadow_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
        except Exception as exc:
            logger.debug("Failed to record actionable policy shadow: symbol=%s err=%s", candidate.symbol, exc)

    def _schedule_hot_pool_prewarm(self, candidate: Any, *, base_score: float) -> None:
        if not _parse_bool(self.settings.get("factor_prewarm_enabled"), True):
            return
        if not getattr(self.factor_enricher, "enabled", False):
            return

        max_pending = max(1, _to_int(self.settings.get("factor_prewarm_max_pending"), 6))
        self._prune_done_prewarm_tasks()
        if len(self._prewarm_tasks) >= max_pending:
            return

        candidates = self._hot_prewarm_candidates(candidate, base_score=base_score)
        if not candidates:
            return

        for item in candidates:
            if len(self._prewarm_tasks) >= max_pending:
                break
            symbol = str(getattr(item, "symbol", "") or "").upper().strip()
            if not symbol or not self._prewarm_due(symbol):
                continue
            self._prewarm_last_ts[symbol] = time.time()
            include_micro = not (
                item is candidate and self._will_enrich_current_candidate(item, base_score=base_score)
            )
            task = asyncio.create_task(
                self._run_hot_prewarm(item, include_micro=include_micro),
                name=f"factor_prewarm_{symbol}",
            )
            self._prewarm_tasks.add(task)
            task.add_done_callback(lambda done: self._prewarm_tasks.discard(done))

    def _hot_prewarm_candidates(self, current: Any, *, base_score: float) -> list[Any]:
        max_items = max(1, _to_int(self.settings.get("factor_prewarm_batch_size"), 3))
        min_score = _to_float(self.settings.get("factor_prewarm_min_score"), 30.0)
        min_event_count = max(1, _to_int(self.settings.get("factor_prewarm_min_event_count"), 1))
        min_abs_change = max(0.0, _to_float(self.settings.get("factor_prewarm_min_abs_change_pct"), 1.0))
        min_rvol = max(0.0, _to_float(self.settings.get("factor_prewarm_min_rvol"), 1.2))
        max_age_minutes = max(1.0, _to_float(self.settings.get("factor_prewarm_candidate_age_minutes"), 60.0))

        pool: Dict[str, Any] = {}
        if current is not None:
            pool[str(getattr(current, "symbol", "") or "").upper()] = current
        for symbol, candidate in self.candidate_engine.all_candidates().items():
            pool[str(symbol or getattr(candidate, "symbol", "") or "").upper()] = candidate

        now = datetime.now(timezone.utc)
        rows: list[tuple[float, Any]] = []
        for item in pool.values():
            symbol = str(getattr(item, "symbol", "") or "").upper().strip()
            if not symbol:
                continue
            last_seen = _parse_iso_time(getattr(item, "last_seen", ""))
            age_minutes = ((now - last_seen).total_seconds() / 60.0) if last_seen is not None else 0.0
            if last_seen is not None and age_minutes > max_age_minutes:
                continue
            score_value = float(getattr(item, "score", 0.0) or 0.0)
            if item is current:
                score_value = max(score_value, float(base_score or 0.0))
            event_count = int(getattr(item, "event_count", 0) or 0)
            abs_change = abs(float(getattr(item, "max_abs_change_pct", 0.0) or 0.0))
            rvol = float(getattr(item, "max_rvol", 0.0) or 0.0)
            if (
                score_value < min_score
                and event_count < min_event_count
                and abs_change < min_abs_change
                and rvol < min_rvol
            ):
                continue
            hot_score = (
                score_value
                + min(20.0, event_count * 2.0)
                + min(20.0, abs_change * 2.0)
                + min(15.0, rvol * 3.0)
                - min(20.0, age_minutes / 3.0)
            )
            if item is current:
                hot_score += 10.0
            rows.append((hot_score, item))

        rows.sort(key=lambda row: row[0], reverse=True)
        return [item for _, item in rows[:max_items]]

    def _will_enrich_current_candidate(self, candidate: Any, *, base_score: float) -> bool:
        should_enrich = getattr(self.factor_enricher, "should_enrich", None)
        if not callable(should_enrich):
            return False
        try:
            return bool(should_enrich(candidate, base_score=base_score))
        except Exception:
            return False

    def _prewarm_due(self, symbol: str) -> bool:
        cooldown = max(10.0, _to_float(self.settings.get("factor_prewarm_cooldown_sec"), 180.0))
        last_ts = float(self._prewarm_last_ts.get(symbol, 0.0))
        return (time.time() - last_ts) >= cooldown

    def _prune_done_prewarm_tasks(self) -> None:
        if not self._prewarm_tasks:
            return
        self._prewarm_tasks = {task for task in self._prewarm_tasks if not task.done()}

    async def _run_hot_prewarm(self, candidate: Any, *, include_micro: bool) -> None:
        symbol = str(getattr(candidate, "symbol", "") or "").upper().strip()
        try:
            if include_micro:
                ok = await self.factor_enricher.prewarm_factors(candidate)
            else:
                ok = await self.factor_enricher.prewarm_derivatives_shadow(candidate)
            logger.debug("Factor prewarm completed: symbol=%s ok=%s", symbol, ok)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("Factor prewarm failed: symbol=%s err=%s", symbol, exc)

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

    def _startup_suppressed_by_primary_alert(self, candidate: Any) -> tuple[bool, str]:
        if not _parse_bool(self.settings.get("startup_suppress_after_primary_alert_enabled"), True):
            return False, "accepted"

        within_minutes = max(
            1.0,
            _to_float(self.settings.get("startup_suppress_after_primary_alert_minutes"), 15.0),
        )
        primary_types = self._startup_primary_alert_types()
        recent_type = self.policy.recently_sent_any(
            candidate.symbol,
            primary_types,
            within_minutes=within_minutes,
        )
        if recent_type:
            return True, f"startup_suppressed_by_primary_alert:{recent_type}"
        return False, "accepted"

    def _startup_primary_alert_types(self) -> list[str]:
        raw_types = self.settings.get("startup_suppress_after_primary_alert_types", ["actionable_alert", "risk_alert"])
        if isinstance(raw_types, str):
            items = raw_types.split(",")
        elif isinstance(raw_types, list):
            items = raw_types
        else:
            items = ["actionable_alert", "risk_alert"]

        normalized: list[str] = []
        for item in items:
            alert_type = str(item or "").strip()
            if alert_type in {"actionable_alert", "risk_alert"} and alert_type not in normalized:
                normalized.append(alert_type)
        return normalized or ["actionable_alert", "risk_alert"]

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
            sent = await notifier.notify_text(
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
            if sent:
                self._record_factor_quality(candidate, "strong_direct_alert", "strong_direct")
            return sent
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


def _parse_iso_time(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)
