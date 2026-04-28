from __future__ import annotations

import copy
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

VALID_WINDOW_LIST = ["1m", "5m", "15m", "30m", "1h"]
VALID_WINDOWS = set(VALID_WINDOW_LIST)
VALID_LEVELS = {"low", "medium", "high"}
FILE_DIR = Path(__file__).resolve().parent
APP_ROOT = FILE_DIR.parent if (FILE_DIR.parent / "config").exists() else FILE_DIR
REPO_ROOT = APP_ROOT.parent.parent if APP_ROOT.parent.name == "apps" else APP_ROOT

DEFAULT_CONFIG: Dict[str, Any] = {
    "symbols": ["BTCUSDT", "ETHUSDT"],
    "windows": ["1m", "5m", "15m", "30m", "1h"],
    "poll_interval_sec": 5,
    "timezone": "Asia/Shanghai",
    "log_level": "INFO",
    "telegram": {
        "enabled": True,
        "token": "",
        "chat_id": "",
        "send_low_priority": False,
    },
    "atr": {
        "enabled": True,
        "period": 14,
        "k_by_window": {
            "1m": 1.8,
            "5m": 2.2,
            "15m": 2.6,
            "30m": 3.0,
            "1h": 3.5,
        },
    },
    "indicators": {
        "rsi_period": 14,
        "bb_period": 20,
        "bb_std": 2.0,
        "macd_fast": 12,
        "macd_slow": 26,
        "macd_signal": 9,
        "ma_period": 20,
        "std_period": 20,
    },
    "rules": {
        "short_breakout": {
            "enabled": True,
            "windows": ["1m", "5m"],
            "price_change_pct": {
                "1m": 1.80,
                "5m": 3.20,
            },
            "min_change_pct": {
                "1m": 1.50,
                "5m": 2.80,
            },
            "atr_multiplier": 1.6,
            "volume_multiplier": 1.8,
            "use_rsi_filter": True,
            "rsi_upper": 70,
            "rsi_lower": 30,
            "force_trigger_pct": {
                "1m": 4.0,
                "5m": 6.0,
            },
            "force_trigger_min_rvol": 1.2,
            "force_trigger_severity": "high",
            "severity": "medium",
        },
        "trend_acceleration": {
            "enabled": True,
            "windows": ["15m", "30m"],
            "price_change_pct": {
                "15m": 5.50,
                "30m": 8.00,
            },
            "min_change_pct": {
                "15m": 5.00,
                "30m": 7.00,
            },
            "atr_multiplier": 1.6,
            "use_bb_breakout": True,
            "use_macd_confirm": True,
            "severity": "high",
        },
        "long_anomaly": {
            "enabled": True,
            "windows": ["1h"],
            "price_change_pct": {
                "1h": 10.00,
            },
            "min_change_pct": {
                "1h": 10.00,
            },
            "atr_multiplier": 1.4,
            "use_ma_sd_filter": True,
            "ma_period": 20,
            "std_period": 20,
            "sd_multiplier": 2.2,
            "severity": "high",
        },
    },
    "cooldown_minutes": {
        "default": 8,
        "high_priority_extra_minutes": 3,
        "by_window": {
            "1m": 5,
            "5m": 8,
            "15m": 12,
            "30m": 18,
            "1h": 35,
        },
    },
    "confidence": {
        "enabled": True,
        "min_push_score": 60.0,
        "bands": [
            {"label": "A", "min_score": 85.0, "cooldown_adjust_minutes": -2},
            {"label": "B", "min_score": 70.0, "cooldown_adjust_minutes": 0},
            {"label": "C", "min_score": 60.0, "cooldown_adjust_minutes": 2},
        ],
    },
    "alert_strategy": {
        "enabled": True,
        "direct_tg_enabled": False,
        "telegram_detail_level": "compact",
        "runtime_dir": "",
        "raw_event_file": "raw_events.jsonl",
        "candidate_file": "candidates.json",
        "policy_state_file": "alert_policy_state.json",
        "candidate_ttl_minutes": 120,
        "min_watch_score": 50.0,
        "min_actionable_score": 75.0,
        "max_watch_risk": 60.0,
        "max_actionable_risk": 45.0,
        "risk_alert_score": 70.0,
        "watch_cooldown_minutes": 30,
        "actionable_cooldown_minutes": 60,
        "risk_cooldown_minutes": 60,
        "global_max_10m": 5,
        "global_max_hour": 20,
        "require_oi_for_actionable": True,
        "min_actionable_oi_signal_level": "L2",
        "strong_direct_enabled": True,
        "strong_direct_windows": ["1m", "5m"],
        "strong_direct_change_pct": {
            "1m": 4.0,
            "5m": 6.0,
        },
        "strong_direct_cooldown_minutes": 12,
        "event_dedupe_enabled": True,
        "event_dedupe_state_file": "event_dedupe_state.json",
        "event_dedupe_window_minutes": 30,
        "event_dedupe_min_abs_change_delta_pct": 3.0,
        "event_dedupe_min_relative_change_delta": 0.35,
        "event_dedupe_min_score_delta": 8.0,
        "event_dedupe_min_risk_delta": 15.0,
        "event_dedupe_factor_upgrade_enabled": True,
        "event_dedupe_window_upgrade_enabled": True,
        "event_dedupe_window_upgrade_requires_score_delta": True,
        "event_dedupe_reversal_enabled": True,
        "confirmation_factors": {
            "enabled": True,
            "cache_ttl_sec": 60,
            "min_base_score": 40.0,
            "min_event_count": 2,
            "binance": {
                "enabled": True,
                "base_url": "https://fapi.binance.com",
                "timeout_sec": 3.0,
                "depth_limit": 100,
                "depth_levels": 20,
                "fetch_open_interest": True,
                "fetch_open_interest_hist": True,
                "fetch_funding": True,
                "fetch_long_short_ratio": False,
                "fetch_taker_buy_sell": True,
                "fetch_orderbook": True,
                "fetch_liquidations": True,
                "liquidation_lookback_minutes": 5,
                "open_interest_hist_period": "5m",
                "open_interest_hist_limit": 576,
                "taker_buy_sell_period": "5m",
                "oi_history": {
                    "enabled": True,
                    "history_file": "oi_history.json",
                    "max_samples_per_symbol": 1200,
                    "bootstrap_ttl_sec": 300,
                    "min_bootstrap_samples": 12,
                    "min_zscore_samples": 20,
                    "signal_thresholds": {
                        "price_up_pct": 1.5,
                        "price_down_pct": -1.5,
                        "price_flat_abs_pct": 2.0,
                        "oi_l0_1h_pct": 0.06,
                        "oi_l1_5m_pct": 0.02,
                        "oi_l1_15m_pct": 0.03,
                        "oi_l2_15m_pct": 0.05,
                        "oi_l2_1h_pct": 0.10,
                        "oi_l3_4h_pct": 0.25,
                        "oi_l3_24h_pct": 0.50,
                        "rvol_confirm": 2.0,
                        "taker_buy_confirm": 0.55,
                        "funding_overheated": 0.0015
                    }
                },
                "microstructure": {
                    "enabled": True,
                    "ws_url": "wss://fstream.binance.com/ws",
                    "track_ttl_sec": 900,
                    "subscription_refresh_sec": 5,
                    "reconnect_sec": 3,
                    "max_active_symbols": 40,
                    "max_trades_per_symbol": 5000,
                    "max_liquidations_per_symbol": 1000,
                    "windows_sec": [10, 30, 60, 180, 300],
                    "signal_thresholds": {
                        "min_trade_notional_1m": 25000.0,
                        "min_trade_notional_5m": 100000.0,
                        "new_longs_cvd_1m": 10000.0,
                        "new_longs_cvd_5m": 35000.0,
                        "new_longs_buy_ratio_1m": 0.55,
                        "new_longs_buy_ratio_5m": 0.53,
                        "breakout_cvd_1m": 18000.0,
                        "breakout_cvd_3m": 45000.0,
                        "breakout_cvd_5m": 80000.0,
                        "breakout_buy_ratio_1m": 0.57,
                        "breakout_buy_ratio_3m": 0.55,
                        "breakout_buy_ratio_5m": 0.54,
                        "breakout_short_liq_share_1m_max": 0.45,
                        "absorption_trade_notional_30s": 8000.0,
                        "absorption_cvd_10s": 3500.0,
                        "absorption_cvd_30s": 7500.0,
                        "absorption_cvd_1m": 8000.0,
                        "absorption_buy_ratio_10s": 0.60,
                        "absorption_buy_ratio_30s": 0.58,
                        "absorption_buy_ratio_1m": 0.55,
                        "absorption_cvd_5m_floor": -20000.0,
                        "absorption_buy_ratio_gap": 0.08,
                        "absorption_oi_15m_min": -0.01,
                        "short_cover_liq_1m": 50000.0,
                        "short_cover_liq_share_1m": 0.55,
                        "short_cover_oi_15m_max": 0.03,
                        "churn_abs_cvd_ratio_1m": 0.08,
                        "churn_abs_cvd_ratio_5m": 0.12,
                        "churn_buy_ratio_delta": 0.04,
                        "oi_positive_15m": 0.015,
                        "oi_positive_1h": 0.015
                    }
                },
            },
            "accumulation_pool": {
                "enabled": True,
                "runtime_dir": "",
                "pool_file": "accumulation_pool.json",
                "history_file": "accumulation_pool_history.jsonl",
                "max_age_hours": 36,
                "cache_ttl_sec": 60,
                "scan": {
                    "enabled": True,
                    "base_url": "https://fapi.binance.com",
                    "lookback_days": 210,
                    "min_data_days": 60,
                    "min_sideways_days": 45,
                    "max_range_pct": 80.0,
                    "max_slope_pct": 20.0,
                    "min_avg_quote_vol_usdt": 500000.0,
                    "max_avg_quote_vol_usdt": 20000000.0,
                    "warming_vol_ratio": 1.5,
                    "ready_vol_ratio": 2.5,
                    "max_recent_pump_pct": 250.0,
                    "min_score": 60.0,
                    "max_symbols": 0,
                    "concurrency": 2,
                    "request_delay_sec": 0.15,
                    "market_cap_api_enabled": True,
                },
            },
        },
    },
    "data_feed": {
        "kline_limit": 200,
        "volume_lookback": 20,
        "close_candle_only": True,
        "max_concurrent_requests": 6,
        "symbol_universe_mode": "all_usdt_perp",
        "universe_refresh_sec": 60,
        "exclude_symbols": [
            "BTCUSDT",
            "ETHUSDT",
            "SOLUSDT",
            "BNBUSDT",
            "XRPUSDT",
            "ADAUSDT",
            "DOGEUSDT",
            "TRXUSDT",
            "LINKUSDT",
            "LTCUSDT",
            "BCHUSDT"
        ],
        "include_symbols": [],
        "min_quote_volume_usdt": 1000000.0,
        "max_quote_volume_usdt": 0.0,
        "exclude_top_quote_volume_enabled": False,
        "exclude_top_quote_volume_n": 20,
        "max_symbols": 0,
        "window_schedule_enabled": True,
        "symbol_batch_size": 60,
        "full_scan_windows": ["1m", "5m"],
        "symbol_batch_size_by_window": {
            "1m": 0,
            "5m": 0,
            "15m": 60,
            "30m": 40,
            "1h": 30,
        },
        "priority_scan_enabled": True,
        "priority_scan_top_n": 50,
        "priority_scan_windows": ["1m", "5m"],
        "priority_scan_min_24h_change_pct": 0.0,
        "scan_coverage_file": "scan_coverage.json",
        "scan_coverage_flush_sec": 15,
        "ws_gap_detection_enabled": True,
        "ws_gap_window": "1m",
        "ws_gap_min_silence_sec": 90,
        "ws_gap_min_change_pct": {
            "1m": 4.0,
            "5m": 6.0,
        },
        "ws_realtime_enabled": True,
        "ws_realtime_windows": ["1m"],
        "ws_realtime_skip_poll_windows": True,
        "ws_realtime_prefilter_pct": {
            "1m": 1.0,
        },
        "ws_realtime_recheck_interval_sec": 6.0,
        "ws_realtime_reconnect_sec": 3,
        "ws_realtime_no_message_reconnect_sec": 45,
        "ws_realtime_subscription_chunk_size": 180,
        "ws_realtime_symbol_refresh_sec": 60,
        "ws_realtime_url": "wss://fstream.binance.com/ws",
        "ws_local_agg_enabled": True,
        "ws_local_agg_windows": ["15m", "30m", "1h"],
        "ws_local_agg_skip_poll_windows": True,
        "ws_local_agg_bootstrap_concurrency": 2,
        "ws_local_agg_history_limit": 200,
    },
}


@dataclass
class AlertConfig:
    symbols: List[str]
    windows: List[str]
    poll_interval_sec: int
    timezone: str
    log_level: str
    telegram: Dict[str, Any]
    atr: Dict[str, Any]
    indicators: Dict[str, Any]
    rules: Dict[str, Dict[str, Any]]
    cooldown_minutes: Dict[str, Any]
    confidence: Dict[str, Any]
    alert_strategy: Dict[str, Any]
    data_feed: Dict[str, Any]
    config_path: Optional[str] = None
    loaded_env_files: List[str] = field(default_factory=list)
    raw: Dict[str, Any] = field(default_factory=dict)

    def cooldown_for(self, window: str) -> int:
        default_cd = int(self.cooldown_minutes.get("default", 5))
        by_window = self.cooldown_minutes.get("by_window", {})
        return int(by_window.get(window, default_cd))

    def high_priority_cooldown_extra_minutes(self) -> int:
        try:
            return max(0, int(self.cooldown_minutes.get("high_priority_extra_minutes", 0)))
        except (TypeError, ValueError):
            return 0

    def confidence_enabled(self) -> bool:
        return bool(self.confidence.get("enabled", False))

    def confidence_min_push_score(self) -> float:
        try:
            return float(self.confidence.get("min_push_score", 0.0))
        except (TypeError, ValueError):
            return 0.0

    def confidence_band_for(self, score: Optional[float]) -> Dict[str, Any]:
        default_band = {"label": "N/A", "min_score": 0.0, "cooldown_adjust_minutes": 0}
        if score is None:
            return default_band

        bands = self.confidence.get("bands", [])
        if not isinstance(bands, list):
            return default_band

        s = float(score)
        for item in bands:
            if not isinstance(item, dict):
                continue
            try:
                min_score = float(item.get("min_score", 0.0))
            except (TypeError, ValueError):
                min_score = 0.0
            if s >= min_score:
                return {
                    "label": str(item.get("label", "N/A")).upper(),
                    "min_score": min_score,
                    "cooldown_adjust_minutes": int(item.get("cooldown_adjust_minutes", 0)),
                }
        return default_band

    def rule(self, rule_name: str) -> Dict[str, Any]:
        return self.rules.get(rule_name, {})

    def threshold_for(self, rule_name: str, window: str, default: float = 0.0) -> float:
        rule_cfg = self.rule(rule_name)
        threshold_map = rule_cfg.get("price_change_pct", {})
        try:
            return float(threshold_map.get(window, default))
        except (TypeError, ValueError):
            return float(default)

    def atr_enabled(self) -> bool:
        return bool(self.atr.get("enabled", False))

    def atr_k_for(self, window: str, default: float = 1.5) -> float:
        k_map = self.atr.get("k_by_window", {})
        try:
            return float(k_map.get(window, default))
        except (TypeError, ValueError):
            return float(default)


def _parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_csv_list(value: str) -> List[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _normalize_symbol(symbol: str) -> str:
    return symbol.strip().upper().replace("/", "")


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _load_json_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        logger.warning("Alert config file not found, fallback to defaults: %s", path)
        return {}

    try:
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            logger.warning("Alert config must be a JSON object: %s", path)
            return {}
        return data
    except Exception as exc:
        logger.exception("Failed to parse alert config file %s: %s", path, exc)
        return {}


def _load_dotenv_files() -> List[str]:
    explicit = os.getenv("ALERT_DOTENV_PATH")
    candidates: List[Path] = []

    if explicit:
        candidates.append(Path(explicit))

    candidates.extend(
        [
            Path.cwd() / ".env",
            Path(__file__).resolve().parent / ".env",
            APP_ROOT / ".env",
            REPO_ROOT / ".env",
        ]
    )

    loaded: List[str] = []
    seen: set[str] = set()
    for item in candidates:
        path = item if item.is_absolute() else (Path.cwd() / item)
        resolved = path.resolve()
        if str(resolved) in seen:
            continue
        seen.add(str(resolved))

        if resolved.exists():
            load_dotenv(resolved, override=False)
            loaded.append(str(resolved))

    return loaded


def _resolve_config_path(config_path: Optional[str]) -> Path:
    raw_path = (config_path or os.getenv("ALERT_CONFIG_PATH") or "config.json").strip()
    candidate = Path(raw_path)

    if candidate.is_absolute():
        return candidate

    search_paths = [
        Path.cwd() / candidate,
        APP_ROOT / candidate,
        APP_ROOT / "config" / candidate,
        REPO_ROOT / candidate,
        Path(__file__).resolve().parent / candidate,
    ]

    for path in search_paths:
        if path.exists():
            return path.resolve()

    return (REPO_ROOT / candidate).resolve()


def _merge_json_env(env_key: str, target: Dict[str, Any], warn_prefix: str) -> None:
    raw = os.getenv(env_key)
    if not raw:
        return
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, dict):
            _deep_merge(target, parsed)
        else:
            logger.warning("%s must be a JSON object, ignored.", warn_prefix)
    except json.JSONDecodeError:
        logger.warning("%s is not valid JSON, ignored.", warn_prefix)


def _apply_env_overrides(config: Dict[str, Any]) -> Dict[str, Any]:
    telegram = config.setdefault("telegram", {})
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if token:
        telegram["token"] = token.strip()
    if chat_id:
        telegram["chat_id"] = chat_id.strip()
    if os.getenv("ALERT_TELEGRAM_ENABLED") is not None:
        telegram["enabled"] = _parse_bool(os.getenv("ALERT_TELEGRAM_ENABLED"), default=True)
    if os.getenv("ALERT_SEND_LOW_PRIORITY") is not None:
        telegram["send_low_priority"] = _parse_bool(os.getenv("ALERT_SEND_LOW_PRIORITY"), default=False)

    if os.getenv("ALERT_SYMBOLS"):
        config["symbols"] = _parse_csv_list(os.getenv("ALERT_SYMBOLS", ""))
    if os.getenv("ALERT_WINDOWS"):
        config["windows"] = _parse_csv_list(os.getenv("ALERT_WINDOWS", ""))
    if os.getenv("ALERT_POLL_INTERVAL_SEC"):
        config["poll_interval_sec"] = _to_int(os.getenv("ALERT_POLL_INTERVAL_SEC"), default=5)
    if os.getenv("ALERT_TIMEZONE"):
        config["timezone"] = os.getenv("ALERT_TIMEZONE", "Asia/Shanghai").strip()
    if os.getenv("ALERT_LOG_LEVEL"):
        config["log_level"] = os.getenv("ALERT_LOG_LEVEL", "INFO").strip().upper()

    cooldown = config.setdefault("cooldown_minutes", {})
    if os.getenv("ALERT_DEFAULT_COOLDOWN_MINUTES"):
        cooldown["default"] = _to_int(os.getenv("ALERT_DEFAULT_COOLDOWN_MINUTES"), default=5)
    if os.getenv("ALERT_HIGH_PRIORITY_EXTRA_MINUTES"):
        cooldown["high_priority_extra_minutes"] = _to_int(
            os.getenv("ALERT_HIGH_PRIORITY_EXTRA_MINUTES"),
            default=0,
        )

    cooldown_json = os.getenv("ALERT_COOLDOWN_BY_WINDOW_JSON")
    if cooldown_json:
        try:
            data = json.loads(cooldown_json)
            if isinstance(data, dict):
                cooldown.setdefault("by_window", {})
                for key, value in data.items():
                    cooldown["by_window"][str(key)] = _to_int(value, default=5)
        except json.JSONDecodeError:
            logger.warning("ALERT_COOLDOWN_BY_WINDOW_JSON is not valid JSON, ignored.")

    atr = config.setdefault("atr", {})
    if os.getenv("ALERT_ATR_ENABLED") is not None:
        atr["enabled"] = _parse_bool(os.getenv("ALERT_ATR_ENABLED"), default=False)
    if os.getenv("ALERT_ATR_PERIOD"):
        atr["period"] = _to_int(os.getenv("ALERT_ATR_PERIOD"), default=14)
    _merge_json_env("ALERT_ATR_K_BY_WINDOW_JSON", atr, "ALERT_ATR_K_BY_WINDOW_JSON")

    _merge_json_env("ALERT_RULE_OVERRIDES_JSON", config.setdefault("rules", {}), "ALERT_RULE_OVERRIDES_JSON")

    indicators = config.setdefault("indicators", {})
    _merge_json_env("ALERT_INDICATORS_JSON", indicators, "ALERT_INDICATORS_JSON")
    scalar_indicator_map = {
        "ALERT_RSI_PERIOD": ("rsi_period", 14, _to_int),
        "ALERT_BB_PERIOD": ("bb_period", 20, _to_int),
        "ALERT_BB_STD": ("bb_std", 2.0, _to_float),
        "ALERT_MACD_FAST": ("macd_fast", 12, _to_int),
        "ALERT_MACD_SLOW": ("macd_slow", 26, _to_int),
        "ALERT_MACD_SIGNAL": ("macd_signal", 9, _to_int),
        "ALERT_MA_PERIOD": ("ma_period", 20, _to_int),
        "ALERT_STD_PERIOD": ("std_period", 20, _to_int),
    }
    for env_key, (cfg_key, default_value, parser) in scalar_indicator_map.items():
        raw = os.getenv(env_key)
        if raw is None:
            continue
        indicators[cfg_key] = parser(raw, default_value)

    data_feed = config.setdefault("data_feed", {})
    if os.getenv("ALERT_KLINE_LIMIT"):
        data_feed["kline_limit"] = _to_int(os.getenv("ALERT_KLINE_LIMIT"), default=200)
    if os.getenv("ALERT_VOLUME_LOOKBACK"):
        data_feed["volume_lookback"] = _to_int(os.getenv("ALERT_VOLUME_LOOKBACK"), default=20)
    if os.getenv("ALERT_CLOSE_CANDLE_ONLY") is not None:
        data_feed["close_candle_only"] = _parse_bool(os.getenv("ALERT_CLOSE_CANDLE_ONLY"), default=True)
    if os.getenv("ALERT_MAX_CONCURRENT_REQUESTS"):
        data_feed["max_concurrent_requests"] = _to_int(os.getenv("ALERT_MAX_CONCURRENT_REQUESTS"), default=8)
    if os.getenv("ALERT_SYMBOL_UNIVERSE_MODE"):
        data_feed["symbol_universe_mode"] = str(os.getenv("ALERT_SYMBOL_UNIVERSE_MODE")).strip().lower()
    if os.getenv("ALERT_UNIVERSE_REFRESH_SEC"):
        data_feed["universe_refresh_sec"] = _to_int(os.getenv("ALERT_UNIVERSE_REFRESH_SEC"), default=60)
    if os.getenv("ALERT_EXCLUDE_SYMBOLS"):
        data_feed["exclude_symbols"] = _parse_csv_list(os.getenv("ALERT_EXCLUDE_SYMBOLS", ""))
    if os.getenv("ALERT_INCLUDE_SYMBOLS"):
        data_feed["include_symbols"] = _parse_csv_list(os.getenv("ALERT_INCLUDE_SYMBOLS", ""))
    if os.getenv("ALERT_FULL_SCAN_WINDOWS"):
        data_feed["full_scan_windows"] = _parse_csv_list(os.getenv("ALERT_FULL_SCAN_WINDOWS", ""))
    if os.getenv("ALERT_PRIORITY_SCAN_ENABLED") is not None:
        data_feed["priority_scan_enabled"] = _parse_bool(
            os.getenv("ALERT_PRIORITY_SCAN_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_PRIORITY_SCAN_TOP_N"):
        data_feed["priority_scan_top_n"] = _to_int(os.getenv("ALERT_PRIORITY_SCAN_TOP_N"), default=50)
    if os.getenv("ALERT_PRIORITY_SCAN_WINDOWS"):
        data_feed["priority_scan_windows"] = _parse_csv_list(os.getenv("ALERT_PRIORITY_SCAN_WINDOWS", ""))
    if os.getenv("ALERT_PRIORITY_SCAN_MIN_24H_CHANGE_PCT"):
        data_feed["priority_scan_min_24h_change_pct"] = _to_float(
            os.getenv("ALERT_PRIORITY_SCAN_MIN_24H_CHANGE_PCT"),
            default=0.0,
        )
    if os.getenv("ALERT_SCAN_COVERAGE_FILE"):
        data_feed["scan_coverage_file"] = str(os.getenv("ALERT_SCAN_COVERAGE_FILE")).strip()
    if os.getenv("ALERT_SCAN_COVERAGE_FLUSH_SEC"):
        data_feed["scan_coverage_flush_sec"] = _to_float(
            os.getenv("ALERT_SCAN_COVERAGE_FLUSH_SEC"),
            default=15.0,
        )
    if os.getenv("ALERT_WS_GAP_DETECTION_ENABLED") is not None:
        data_feed["ws_gap_detection_enabled"] = _parse_bool(
            os.getenv("ALERT_WS_GAP_DETECTION_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_WS_GAP_WINDOW"):
        data_feed["ws_gap_window"] = str(os.getenv("ALERT_WS_GAP_WINDOW")).strip()
    if os.getenv("ALERT_WS_GAP_MIN_SILENCE_SEC"):
        data_feed["ws_gap_min_silence_sec"] = _to_float(
            os.getenv("ALERT_WS_GAP_MIN_SILENCE_SEC"),
            default=90.0,
        )
    ws_gap_min_change_raw = os.getenv("ALERT_WS_GAP_MIN_CHANGE_BY_WINDOW_JSON")
    if ws_gap_min_change_raw:
        try:
            parsed = json.loads(ws_gap_min_change_raw)
            if isinstance(parsed, dict):
                data_feed["ws_gap_min_change_pct"] = {
                    str(k): _to_float(v, default=0.0) for k, v in parsed.items()
                }
            else:
                logger.warning("ALERT_WS_GAP_MIN_CHANGE_BY_WINDOW_JSON must be a JSON object, ignored.")
        except json.JSONDecodeError:
            logger.warning("ALERT_WS_GAP_MIN_CHANGE_BY_WINDOW_JSON is not valid JSON, ignored.")
    if os.getenv("ALERT_MIN_QUOTE_VOLUME_USDT"):
        data_feed["min_quote_volume_usdt"] = _to_float(os.getenv("ALERT_MIN_QUOTE_VOLUME_USDT"), default=0.0)
    if os.getenv("ALERT_MAX_QUOTE_VOLUME_USDT"):
        data_feed["max_quote_volume_usdt"] = _to_float(os.getenv("ALERT_MAX_QUOTE_VOLUME_USDT"), default=0.0)
    if os.getenv("ALERT_MAX_SYMBOLS"):
        data_feed["max_symbols"] = _to_int(os.getenv("ALERT_MAX_SYMBOLS"), default=0)
    if os.getenv("ALERT_WINDOW_SCHEDULE_ENABLED") is not None:
        data_feed["window_schedule_enabled"] = _parse_bool(
            os.getenv("ALERT_WINDOW_SCHEDULE_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_SYMBOL_BATCH_SIZE"):
        data_feed["symbol_batch_size"] = _to_int(os.getenv("ALERT_SYMBOL_BATCH_SIZE"), default=25)
    batch_size_by_window_raw = os.getenv("ALERT_SYMBOL_BATCH_SIZE_BY_WINDOW_JSON")
    if batch_size_by_window_raw:
        try:
            parsed = json.loads(batch_size_by_window_raw)
            if isinstance(parsed, dict):
                data_feed["symbol_batch_size_by_window"] = {
                    str(k): _to_int(v, default=0) for k, v in parsed.items()
                }
            else:
                logger.warning("ALERT_SYMBOL_BATCH_SIZE_BY_WINDOW_JSON must be a JSON object, ignored.")
        except json.JSONDecodeError:
            logger.warning("ALERT_SYMBOL_BATCH_SIZE_BY_WINDOW_JSON is not valid JSON, ignored.")
    if os.getenv("ALERT_EXCLUDE_TOP_QUOTE_VOLUME_N"):
        data_feed["exclude_top_quote_volume_n"] = _to_int(
            os.getenv("ALERT_EXCLUDE_TOP_QUOTE_VOLUME_N"),
            default=0,
        )
    if os.getenv("ALERT_EXCLUDE_TOP_QUOTE_VOLUME_ENABLED") is not None:
        data_feed["exclude_top_quote_volume_enabled"] = _parse_bool(
            os.getenv("ALERT_EXCLUDE_TOP_QUOTE_VOLUME_ENABLED"),
            default=False,
        )
    if os.getenv("ALERT_WS_REALTIME_ENABLED") is not None:
        data_feed["ws_realtime_enabled"] = _parse_bool(os.getenv("ALERT_WS_REALTIME_ENABLED"), default=True)
    if os.getenv("ALERT_WS_REALTIME_WINDOWS"):
        data_feed["ws_realtime_windows"] = _parse_csv_list(os.getenv("ALERT_WS_REALTIME_WINDOWS", ""))
    if os.getenv("ALERT_WS_REALTIME_SKIP_POLL_WINDOWS") is not None:
        data_feed["ws_realtime_skip_poll_windows"] = _parse_bool(
            os.getenv("ALERT_WS_REALTIME_SKIP_POLL_WINDOWS"),
            default=True,
        )
    if os.getenv("ALERT_WS_REALTIME_PREFILTER_PCT"):
        data_feed["ws_realtime_prefilter_pct"] = _to_float(
            os.getenv("ALERT_WS_REALTIME_PREFILTER_PCT"),
            default=1.0,
        )
    ws_prefilter_json = os.getenv("ALERT_WS_REALTIME_PREFILTER_BY_WINDOW_JSON")
    if ws_prefilter_json:
        try:
            parsed = json.loads(ws_prefilter_json)
            if isinstance(parsed, dict):
                data_feed["ws_realtime_prefilter_pct"] = {
                    str(k): _to_float(v, default=0.0) for k, v in parsed.items()
                }
            else:
                logger.warning("ALERT_WS_REALTIME_PREFILTER_BY_WINDOW_JSON must be a JSON object, ignored.")
        except json.JSONDecodeError:
            logger.warning("ALERT_WS_REALTIME_PREFILTER_BY_WINDOW_JSON is not valid JSON, ignored.")
    if os.getenv("ALERT_WS_REALTIME_RECHECK_INTERVAL_SEC"):
        data_feed["ws_realtime_recheck_interval_sec"] = _to_float(
            os.getenv("ALERT_WS_REALTIME_RECHECK_INTERVAL_SEC"),
            default=6.0,
        )
    if os.getenv("ALERT_WS_REALTIME_RECONNECT_SEC"):
        data_feed["ws_realtime_reconnect_sec"] = _to_int(
            os.getenv("ALERT_WS_REALTIME_RECONNECT_SEC"),
            default=3,
        )
    if os.getenv("ALERT_WS_REALTIME_NO_MESSAGE_RECONNECT_SEC"):
        data_feed["ws_realtime_no_message_reconnect_sec"] = _to_float(
            os.getenv("ALERT_WS_REALTIME_NO_MESSAGE_RECONNECT_SEC"),
            default=45.0,
        )
    if os.getenv("ALERT_WS_REALTIME_SUB_CHUNK_SIZE"):
        data_feed["ws_realtime_subscription_chunk_size"] = _to_int(
            os.getenv("ALERT_WS_REALTIME_SUB_CHUNK_SIZE"),
            default=180,
        )
    if os.getenv("ALERT_WS_REALTIME_SYMBOL_REFRESH_SEC"):
        data_feed["ws_realtime_symbol_refresh_sec"] = _to_int(
            os.getenv("ALERT_WS_REALTIME_SYMBOL_REFRESH_SEC"),
            default=60,
        )
    if os.getenv("ALERT_WS_REALTIME_URL"):
        data_feed["ws_realtime_url"] = str(os.getenv("ALERT_WS_REALTIME_URL")).strip()
    if os.getenv("ALERT_WS_LOCAL_AGG_ENABLED") is not None:
        data_feed["ws_local_agg_enabled"] = _parse_bool(os.getenv("ALERT_WS_LOCAL_AGG_ENABLED"), default=True)
    if os.getenv("ALERT_WS_LOCAL_AGG_WINDOWS"):
        data_feed["ws_local_agg_windows"] = _parse_csv_list(os.getenv("ALERT_WS_LOCAL_AGG_WINDOWS", ""))
    if os.getenv("ALERT_WS_LOCAL_AGG_SKIP_POLL_WINDOWS") is not None:
        data_feed["ws_local_agg_skip_poll_windows"] = _parse_bool(
            os.getenv("ALERT_WS_LOCAL_AGG_SKIP_POLL_WINDOWS"),
            default=True,
        )
    if os.getenv("ALERT_WS_LOCAL_AGG_BOOTSTRAP_CONCURRENCY"):
        data_feed["ws_local_agg_bootstrap_concurrency"] = _to_int(
            os.getenv("ALERT_WS_LOCAL_AGG_BOOTSTRAP_CONCURRENCY"),
            default=2,
        )
    if os.getenv("ALERT_WS_LOCAL_AGG_HISTORY_LIMIT"):
        data_feed["ws_local_agg_history_limit"] = _to_int(
            os.getenv("ALERT_WS_LOCAL_AGG_HISTORY_LIMIT"),
            default=200,
        )

    confidence = config.setdefault("confidence", {})
    if os.getenv("ALERT_CONFIDENCE_ENABLED") is not None:
        confidence["enabled"] = _parse_bool(os.getenv("ALERT_CONFIDENCE_ENABLED"), default=True)
    if os.getenv("ALERT_CONFIDENCE_MIN_PUSH_SCORE"):
        confidence["min_push_score"] = _to_float(os.getenv("ALERT_CONFIDENCE_MIN_PUSH_SCORE"), default=0.0)
    confidence_bands_raw = os.getenv("ALERT_CONFIDENCE_BANDS_JSON")
    if confidence_bands_raw:
        try:
            parsed = json.loads(confidence_bands_raw)
            if isinstance(parsed, list):
                confidence["bands"] = parsed
            else:
                logger.warning("ALERT_CONFIDENCE_BANDS_JSON must be a JSON list, ignored.")
        except json.JSONDecodeError:
            logger.warning("ALERT_CONFIDENCE_BANDS_JSON is not valid JSON, ignored.")

    alert_strategy = config.get("alert_strategy")
    if not isinstance(alert_strategy, dict):
        alert_strategy = {}
        config["alert_strategy"] = alert_strategy
    _merge_json_env("ALERT_STRATEGY_JSON", alert_strategy, "ALERT_STRATEGY_JSON")
    if os.getenv("ALERT_STRATEGY_ENABLED") is not None:
        alert_strategy["enabled"] = _parse_bool(os.getenv("ALERT_STRATEGY_ENABLED"), default=True)
    if os.getenv("ALERT_DIRECT_TG_ENABLED") is not None:
        alert_strategy["direct_tg_enabled"] = _parse_bool(os.getenv("ALERT_DIRECT_TG_ENABLED"), default=False)
    if os.getenv("ALERT_STRATEGY_RUNTIME_DIR"):
        alert_strategy["runtime_dir"] = str(os.getenv("ALERT_STRATEGY_RUNTIME_DIR")).strip()
    if os.getenv("ALERT_STRATEGY_MIN_WATCH_SCORE"):
        alert_strategy["min_watch_score"] = _to_float(os.getenv("ALERT_STRATEGY_MIN_WATCH_SCORE"), default=50.0)
    if os.getenv("ALERT_STRATEGY_MIN_ACTIONABLE_SCORE"):
        alert_strategy["min_actionable_score"] = _to_float(
            os.getenv("ALERT_STRATEGY_MIN_ACTIONABLE_SCORE"),
            default=75.0,
        )
    if os.getenv("ALERT_STRATEGY_MAX_WATCH_RISK"):
        alert_strategy["max_watch_risk"] = _to_float(os.getenv("ALERT_STRATEGY_MAX_WATCH_RISK"), default=60.0)
    if os.getenv("ALERT_STRATEGY_MAX_ACTIONABLE_RISK"):
        alert_strategy["max_actionable_risk"] = _to_float(
            os.getenv("ALERT_STRATEGY_MAX_ACTIONABLE_RISK"),
            default=45.0,
        )
    if os.getenv("ALERT_STRATEGY_RISK_ALERT_SCORE"):
        alert_strategy["risk_alert_score"] = _to_float(os.getenv("ALERT_STRATEGY_RISK_ALERT_SCORE"), default=70.0)
    if os.getenv("ALERT_STRATEGY_CANDIDATE_TTL_MINUTES"):
        alert_strategy["candidate_ttl_minutes"] = _to_int(
            os.getenv("ALERT_STRATEGY_CANDIDATE_TTL_MINUTES"),
            default=120,
        )
    if os.getenv("ALERT_STRATEGY_GLOBAL_MAX_10M"):
        alert_strategy["global_max_10m"] = _to_int(os.getenv("ALERT_STRATEGY_GLOBAL_MAX_10M"), default=5)
    if os.getenv("ALERT_STRATEGY_GLOBAL_MAX_HOUR"):
        alert_strategy["global_max_hour"] = _to_int(os.getenv("ALERT_STRATEGY_GLOBAL_MAX_HOUR"), default=20)
    if os.getenv("ALERT_STRATEGY_REQUIRE_OI_FOR_ACTIONABLE") is not None:
        alert_strategy["require_oi_for_actionable"] = _parse_bool(
            os.getenv("ALERT_STRATEGY_REQUIRE_OI_FOR_ACTIONABLE"),
            default=True,
        )
    if os.getenv("ALERT_STRATEGY_MIN_ACTIONABLE_OI_SIGNAL_LEVEL"):
        alert_strategy["min_actionable_oi_signal_level"] = str(
            os.getenv("ALERT_STRATEGY_MIN_ACTIONABLE_OI_SIGNAL_LEVEL")
        ).strip().upper()
    if os.getenv("ALERT_STRATEGY_STRONG_DIRECT_ENABLED") is not None:
        alert_strategy["strong_direct_enabled"] = _parse_bool(
            os.getenv("ALERT_STRATEGY_STRONG_DIRECT_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_STRATEGY_STRONG_DIRECT_WINDOWS"):
        alert_strategy["strong_direct_windows"] = _parse_csv_list(
            os.getenv("ALERT_STRATEGY_STRONG_DIRECT_WINDOWS", "")
        )
    strong_direct_change_raw = os.getenv("ALERT_STRATEGY_STRONG_DIRECT_CHANGE_BY_WINDOW_JSON")
    if strong_direct_change_raw:
        try:
            parsed = json.loads(strong_direct_change_raw)
            if isinstance(parsed, dict):
                alert_strategy["strong_direct_change_pct"] = {
                    str(k): _to_float(v, default=0.0) for k, v in parsed.items()
                }
            else:
                logger.warning("ALERT_STRATEGY_STRONG_DIRECT_CHANGE_BY_WINDOW_JSON must be a JSON object, ignored.")
        except json.JSONDecodeError:
            logger.warning("ALERT_STRATEGY_STRONG_DIRECT_CHANGE_BY_WINDOW_JSON is not valid JSON, ignored.")
    if os.getenv("ALERT_STRATEGY_STRONG_DIRECT_COOLDOWN_MINUTES"):
        alert_strategy["strong_direct_cooldown_minutes"] = _to_int(
            os.getenv("ALERT_STRATEGY_STRONG_DIRECT_COOLDOWN_MINUTES"),
            default=12,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_ENABLED") is not None:
        alert_strategy["event_dedupe_enabled"] = _parse_bool(
            os.getenv("ALERT_EVENT_DEDUPE_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_STATE_FILE"):
        alert_strategy["event_dedupe_state_file"] = str(os.getenv("ALERT_EVENT_DEDUPE_STATE_FILE")).strip()
    if os.getenv("ALERT_EVENT_DEDUPE_WINDOW_MINUTES"):
        alert_strategy["event_dedupe_window_minutes"] = _to_int(
            os.getenv("ALERT_EVENT_DEDUPE_WINDOW_MINUTES"),
            default=30,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_MIN_ABS_CHANGE_DELTA_PCT"):
        alert_strategy["event_dedupe_min_abs_change_delta_pct"] = _to_float(
            os.getenv("ALERT_EVENT_DEDUPE_MIN_ABS_CHANGE_DELTA_PCT"),
            default=3.0,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_MIN_RELATIVE_CHANGE_DELTA"):
        alert_strategy["event_dedupe_min_relative_change_delta"] = _to_float(
            os.getenv("ALERT_EVENT_DEDUPE_MIN_RELATIVE_CHANGE_DELTA"),
            default=0.35,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_MIN_SCORE_DELTA"):
        alert_strategy["event_dedupe_min_score_delta"] = _to_float(
            os.getenv("ALERT_EVENT_DEDUPE_MIN_SCORE_DELTA"),
            default=8.0,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_MIN_RISK_DELTA"):
        alert_strategy["event_dedupe_min_risk_delta"] = _to_float(
            os.getenv("ALERT_EVENT_DEDUPE_MIN_RISK_DELTA"),
            default=15.0,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_FACTOR_UPGRADE_ENABLED") is not None:
        alert_strategy["event_dedupe_factor_upgrade_enabled"] = _parse_bool(
            os.getenv("ALERT_EVENT_DEDUPE_FACTOR_UPGRADE_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_WINDOW_UPGRADE_ENABLED") is not None:
        alert_strategy["event_dedupe_window_upgrade_enabled"] = _parse_bool(
            os.getenv("ALERT_EVENT_DEDUPE_WINDOW_UPGRADE_ENABLED"),
            default=True,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_WINDOW_UPGRADE_REQUIRES_SCORE_DELTA") is not None:
        alert_strategy["event_dedupe_window_upgrade_requires_score_delta"] = _parse_bool(
            os.getenv("ALERT_EVENT_DEDUPE_WINDOW_UPGRADE_REQUIRES_SCORE_DELTA"),
            default=True,
        )
    if os.getenv("ALERT_EVENT_DEDUPE_REVERSAL_ENABLED") is not None:
        alert_strategy["event_dedupe_reversal_enabled"] = _parse_bool(
            os.getenv("ALERT_EVENT_DEDUPE_REVERSAL_ENABLED"),
            default=True,
        )
    factors = alert_strategy.setdefault("confirmation_factors", {})
    if os.getenv("ALERT_CONFIRMATION_FACTORS_ENABLED") is not None:
        factors["enabled"] = _parse_bool(os.getenv("ALERT_CONFIRMATION_FACTORS_ENABLED"), default=True)
    if os.getenv("ALERT_CONFIRMATION_MIN_BASE_SCORE"):
        factors["min_base_score"] = _to_float(os.getenv("ALERT_CONFIRMATION_MIN_BASE_SCORE"), default=40.0)
    if os.getenv("ALERT_CONFIRMATION_CACHE_TTL_SEC"):
        factors["cache_ttl_sec"] = _to_float(os.getenv("ALERT_CONFIRMATION_CACHE_TTL_SEC"), default=60.0)
    binance_factors = factors.setdefault("binance", {})
    if os.getenv("ALERT_BINANCE_FACTORS_ENABLED") is not None:
        binance_factors["enabled"] = _parse_bool(os.getenv("ALERT_BINANCE_FACTORS_ENABLED"), default=True)
    if os.getenv("ALERT_BINANCE_FACTOR_TIMEOUT_SEC"):
        binance_factors["timeout_sec"] = _to_float(os.getenv("ALERT_BINANCE_FACTOR_TIMEOUT_SEC"), default=3.0)
    if os.getenv("ALERT_BINANCE_FETCH_LIQUIDATIONS") is not None:
        binance_factors["fetch_liquidations"] = _parse_bool(
            os.getenv("ALERT_BINANCE_FETCH_LIQUIDATIONS"),
            default=False,
        )
    if os.getenv("ALERT_BINANCE_FETCH_OI_HIST") is not None:
        binance_factors["fetch_open_interest_hist"] = _parse_bool(
            os.getenv("ALERT_BINANCE_FETCH_OI_HIST"),
            default=True,
        )
    if os.getenv("ALERT_BINANCE_FETCH_TAKER_BUY_SELL") is not None:
        binance_factors["fetch_taker_buy_sell"] = _parse_bool(
            os.getenv("ALERT_BINANCE_FETCH_TAKER_BUY_SELL"),
            default=True,
        )
    return config


def _normalize_config(config: Dict[str, Any]) -> Dict[str, Any]:
    symbols = config.get("symbols", [])
    normalized_symbols: List[str] = []
    seen_symbols: set[str] = set()
    for item in symbols:
        if not item:
            continue
        normalized = _normalize_symbol(str(item))
        if not normalized or normalized in seen_symbols:
            continue
        seen_symbols.add(normalized)
        normalized_symbols.append(normalized)
    config["symbols"] = normalized_symbols

    windows = config.get("windows", [])
    cleaned_windows = [str(w).strip() for w in windows if str(w).strip() in VALID_WINDOWS]
    if not cleaned_windows:
        cleaned_windows = ["1m", "5m", "15m", "30m", "1h"]
    config["windows"] = cleaned_windows

    config["poll_interval_sec"] = max(1, _to_int(config.get("poll_interval_sec"), default=5))

    telegram = config.setdefault("telegram", {})
    telegram["enabled"] = _parse_bool(telegram.get("enabled"), default=True)
    telegram["send_low_priority"] = _parse_bool(telegram.get("send_low_priority"), default=False)
    telegram["token"] = str(telegram.get("token", "")).strip()
    telegram["chat_id"] = str(telegram.get("chat_id", "")).strip()

    atr = config.setdefault("atr", {})
    atr["enabled"] = _parse_bool(atr.get("enabled"), default=False)
    atr["period"] = max(1, _to_int(atr.get("period"), default=14))
    k_by_window = atr.get("k_by_window", {})
    if not isinstance(k_by_window, dict):
        k_by_window = {}
    atr["k_by_window"] = {
        window: _to_float(k_by_window.get(window, DEFAULT_CONFIG["atr"]["k_by_window"].get(window, 1.5)), 1.5)
        for window in VALID_WINDOW_LIST
    }

    indicators = config.setdefault("indicators", {})
    indicator_defaults = DEFAULT_CONFIG["indicators"]
    indicators["rsi_period"] = max(2, _to_int(indicators.get("rsi_period"), indicator_defaults["rsi_period"]))
    indicators["bb_period"] = max(5, _to_int(indicators.get("bb_period"), indicator_defaults["bb_period"]))
    indicators["bb_std"] = max(0.1, _to_float(indicators.get("bb_std"), indicator_defaults["bb_std"]))
    indicators["macd_fast"] = max(2, _to_int(indicators.get("macd_fast"), indicator_defaults["macd_fast"]))
    indicators["macd_slow"] = max(3, _to_int(indicators.get("macd_slow"), indicator_defaults["macd_slow"]))
    if indicators["macd_fast"] >= indicators["macd_slow"]:
        indicators["macd_fast"] = max(2, indicators["macd_slow"] - 1)
    indicators["macd_signal"] = max(2, _to_int(indicators.get("macd_signal"), indicator_defaults["macd_signal"]))
    indicators["ma_period"] = max(2, _to_int(indicators.get("ma_period"), indicator_defaults["ma_period"]))
    indicators["std_period"] = max(2, _to_int(indicators.get("std_period"), indicator_defaults["std_period"]))

    rules = config.setdefault("rules", {})
    for rule_name, rule_cfg in list(rules.items()):
        if not isinstance(rule_cfg, dict):
            rules[rule_name] = {}
            rule_cfg = rules[rule_name]

        rule_cfg["enabled"] = _parse_bool(rule_cfg.get("enabled"), default=True)

        rule_windows = rule_cfg.get("windows", [])
        clean_rule_windows = [str(w).strip() for w in rule_windows if str(w).strip() in VALID_WINDOWS]
        if not clean_rule_windows:
            clean_rule_windows = config["windows"]
        rule_cfg["windows"] = clean_rule_windows

        severity = str(rule_cfg.get("severity", "medium")).strip().lower()
        if severity not in VALID_LEVELS:
            severity = "medium"
        rule_cfg["severity"] = severity

        threshold_map = rule_cfg.get("price_change_pct", {})
        if not isinstance(threshold_map, dict):
            threshold_map = {}
        normalized_map: Dict[str, float] = {}
        for window in rule_cfg["windows"]:
            default_val = DEFAULT_CONFIG["rules"].get(rule_name, {}).get("price_change_pct", {}).get(window, 0.0)
            normalized_map[window] = _to_float(threshold_map.get(window, default_val), default=0.0)
        rule_cfg["price_change_pct"] = normalized_map

        min_change_map = rule_cfg.get("min_change_pct", {})
        if not isinstance(min_change_map, dict):
            min_change_map = {}
        normalized_min_map: Dict[str, float] = {}
        for window in rule_cfg["windows"]:
            default_min = DEFAULT_CONFIG["rules"].get(rule_name, {}).get("min_change_pct", {}).get(window, 0.0)
            normalized_min_map[window] = max(0.0, _to_float(min_change_map.get(window, default_min), default=0.0))
        rule_cfg["min_change_pct"] = normalized_min_map

        rule_cfg["atr_multiplier"] = max(0.1, _to_float(rule_cfg.get("atr_multiplier"), default=1.0))

        if "volume_multiplier" in rule_cfg:
            rule_cfg["volume_multiplier"] = max(0.0, _to_float(rule_cfg.get("volume_multiplier"), default=1.8))

        if "use_rsi_filter" in rule_cfg:
            rule_cfg["use_rsi_filter"] = _parse_bool(rule_cfg.get("use_rsi_filter"), default=False)
        if "rsi_upper" in rule_cfg:
            rule_cfg["rsi_upper"] = _to_float(rule_cfg.get("rsi_upper"), default=70.0)
        if "rsi_lower" in rule_cfg:
            rule_cfg["rsi_lower"] = _to_float(rule_cfg.get("rsi_lower"), default=30.0)
        if "force_trigger_pct" in rule_cfg:
            force_map = rule_cfg.get("force_trigger_pct", {})
            if not isinstance(force_map, dict):
                force_map = {}
            normalized_force_map: Dict[str, float] = {}
            for window in rule_cfg["windows"]:
                default_force = DEFAULT_CONFIG["rules"].get(rule_name, {}).get("force_trigger_pct", {}).get(window, 0.0)
                normalized_force_map[window] = max(0.0, _to_float(force_map.get(window, default_force), default=0.0))
            rule_cfg["force_trigger_pct"] = normalized_force_map
        default_force_rvol = _to_float(
            DEFAULT_CONFIG["rules"].get(rule_name, {}).get("force_trigger_min_rvol"),
            default=0.0,
        )
        if "force_trigger_min_rvol" in rule_cfg or default_force_rvol > 0:
            rule_cfg["force_trigger_min_rvol"] = max(
                0.0,
                _to_float(rule_cfg.get("force_trigger_min_rvol"), default=default_force_rvol),
            )
        if "force_trigger_severity" in rule_cfg:
            force_severity = str(rule_cfg.get("force_trigger_severity", "high")).strip().lower()
            if force_severity not in VALID_LEVELS:
                force_severity = "high"
            rule_cfg["force_trigger_severity"] = force_severity

        if "use_bb_breakout" in rule_cfg:
            rule_cfg["use_bb_breakout"] = _parse_bool(rule_cfg.get("use_bb_breakout"), default=False)
        if "use_macd_confirm" in rule_cfg:
            rule_cfg["use_macd_confirm"] = _parse_bool(rule_cfg.get("use_macd_confirm"), default=False)

        if "use_ma_sd_filter" in rule_cfg:
            rule_cfg["use_ma_sd_filter"] = _parse_bool(rule_cfg.get("use_ma_sd_filter"), default=True)
        if "ma_period" in rule_cfg:
            rule_cfg["ma_period"] = max(2, _to_int(rule_cfg.get("ma_period"), default=indicators["ma_period"]))
        if "std_period" in rule_cfg:
            rule_cfg["std_period"] = max(2, _to_int(rule_cfg.get("std_period"), default=indicators["std_period"]))
        if "sd_multiplier" in rule_cfg:
            rule_cfg["sd_multiplier"] = max(0.1, _to_float(rule_cfg.get("sd_multiplier"), default=2.0))

    cooldown = config.setdefault("cooldown_minutes", {})
    default_cd = max(1, _to_int(cooldown.get("default"), default=5))
    by_window = cooldown.get("by_window", {})
    if not isinstance(by_window, dict):
        by_window = {}

    normalized_by_window: Dict[str, int] = {}
    for window in config["windows"]:
        normalized_by_window[window] = max(1, _to_int(by_window.get(window), default=default_cd))

    cooldown["default"] = default_cd
    cooldown["high_priority_extra_minutes"] = max(
        0,
        _to_int(cooldown.get("high_priority_extra_minutes"), default=0),
    )
    cooldown["by_window"] = normalized_by_window

    confidence = config.setdefault("confidence", {})
    confidence["enabled"] = _parse_bool(confidence.get("enabled"), default=True)
    confidence["min_push_score"] = max(0.0, min(100.0, _to_float(confidence.get("min_push_score"), default=0.0)))
    raw_bands = confidence.get("bands", [])
    if not isinstance(raw_bands, list):
        raw_bands = []
    normalized_bands: List[Dict[str, Any]] = []
    for band in raw_bands:
        if not isinstance(band, dict):
            continue
        label = str(band.get("label", "")).strip().upper()
        if not label:
            continue
        min_score = max(0.0, min(100.0, _to_float(band.get("min_score"), default=0.0)))
        cooldown_adjust = _to_int(band.get("cooldown_adjust_minutes"), default=0)
        normalized_bands.append(
            {
                "label": label,
                "min_score": min_score,
                "cooldown_adjust_minutes": cooldown_adjust,
            }
        )
    if not normalized_bands:
        default_bands = DEFAULT_CONFIG.get("confidence", {}).get("bands", [])
        for band in default_bands:
            if isinstance(band, dict):
                normalized_bands.append(
                    {
                        "label": str(band.get("label", "N/A")).strip().upper(),
                        "min_score": max(0.0, min(100.0, _to_float(band.get("min_score"), default=0.0))),
                        "cooldown_adjust_minutes": _to_int(band.get("cooldown_adjust_minutes"), default=0),
                    }
                )
    normalized_bands.sort(key=lambda b: float(b.get("min_score", 0.0)), reverse=True)
    confidence["bands"] = normalized_bands

    alert_strategy = config.setdefault("alert_strategy", {})
    if not isinstance(alert_strategy, dict):
        alert_strategy = {}
        config["alert_strategy"] = alert_strategy
    strategy_defaults = DEFAULT_CONFIG["alert_strategy"]
    alert_strategy["enabled"] = _parse_bool(alert_strategy.get("enabled"), default=True)
    alert_strategy["direct_tg_enabled"] = _parse_bool(alert_strategy.get("direct_tg_enabled"), default=False)
    alert_strategy["runtime_dir"] = str(alert_strategy.get("runtime_dir", strategy_defaults["runtime_dir"])).strip()
    alert_strategy["raw_event_file"] = str(
        alert_strategy.get("raw_event_file", strategy_defaults["raw_event_file"])
    ).strip() or strategy_defaults["raw_event_file"]
    alert_strategy["candidate_file"] = str(
        alert_strategy.get("candidate_file", strategy_defaults["candidate_file"])
    ).strip() or strategy_defaults["candidate_file"]
    alert_strategy["policy_state_file"] = str(
        alert_strategy.get("policy_state_file", strategy_defaults["policy_state_file"])
    ).strip() or strategy_defaults["policy_state_file"]
    alert_strategy["candidate_ttl_minutes"] = max(
        1,
        _to_int(alert_strategy.get("candidate_ttl_minutes"), strategy_defaults["candidate_ttl_minutes"]),
    )
    alert_strategy["min_watch_score"] = max(
        0.0,
        min(100.0, _to_float(alert_strategy.get("min_watch_score"), strategy_defaults["min_watch_score"])),
    )
    alert_strategy["min_actionable_score"] = max(
        alert_strategy["min_watch_score"],
        min(
            100.0,
            _to_float(alert_strategy.get("min_actionable_score"), strategy_defaults["min_actionable_score"]),
        ),
    )
    alert_strategy["max_watch_risk"] = max(
        0.0,
        min(100.0, _to_float(alert_strategy.get("max_watch_risk"), strategy_defaults["max_watch_risk"])),
    )
    alert_strategy["max_actionable_risk"] = max(
        0.0,
        min(
            alert_strategy["max_watch_risk"],
            _to_float(alert_strategy.get("max_actionable_risk"), strategy_defaults["max_actionable_risk"]),
        ),
    )
    alert_strategy["risk_alert_score"] = max(
        0.0,
        min(100.0, _to_float(alert_strategy.get("risk_alert_score"), strategy_defaults["risk_alert_score"])),
    )
    alert_strategy["watch_cooldown_minutes"] = max(
        1,
        _to_int(alert_strategy.get("watch_cooldown_minutes"), strategy_defaults["watch_cooldown_minutes"]),
    )
    alert_strategy["actionable_cooldown_minutes"] = max(
        1,
        _to_int(
            alert_strategy.get("actionable_cooldown_minutes"),
            strategy_defaults["actionable_cooldown_minutes"],
        ),
    )
    alert_strategy["risk_cooldown_minutes"] = max(
        1,
        _to_int(alert_strategy.get("risk_cooldown_minutes"), strategy_defaults["risk_cooldown_minutes"]),
    )
    alert_strategy["global_max_10m"] = max(
        1,
        _to_int(alert_strategy.get("global_max_10m"), strategy_defaults["global_max_10m"]),
    )
    alert_strategy["global_max_hour"] = max(
        alert_strategy["global_max_10m"],
        _to_int(alert_strategy.get("global_max_hour"), strategy_defaults["global_max_hour"]),
    )
    alert_strategy["require_oi_for_actionable"] = _parse_bool(
        alert_strategy.get("require_oi_for_actionable"),
        default=bool(strategy_defaults.get("require_oi_for_actionable", True)),
    )
    min_oi_level = str(
        alert_strategy.get(
            "min_actionable_oi_signal_level",
            strategy_defaults.get("min_actionable_oi_signal_level", "L2"),
        )
    ).strip().upper()
    if min_oi_level not in {"L0", "L1", "L2", "L3", "NONE"}:
        min_oi_level = "L2"
    alert_strategy["min_actionable_oi_signal_level"] = min_oi_level
    alert_strategy["strong_direct_enabled"] = _parse_bool(
        alert_strategy.get("strong_direct_enabled"),
        default=bool(strategy_defaults.get("strong_direct_enabled", True)),
    )
    strong_windows = alert_strategy.get("strong_direct_windows", [])
    if not isinstance(strong_windows, list):
        strong_windows = []
    normalized_strong_windows = [
        str(w).strip()
        for w in strong_windows
        if str(w).strip() in VALID_WINDOWS
    ]
    if not normalized_strong_windows and alert_strategy["strong_direct_enabled"]:
        default_strong_windows = strategy_defaults.get("strong_direct_windows", ["1m", "5m"])
        normalized_strong_windows = [
            str(w).strip()
            for w in default_strong_windows
            if str(w).strip() in VALID_WINDOWS
        ]
    alert_strategy["strong_direct_windows"] = normalized_strong_windows
    strong_change = alert_strategy.get("strong_direct_change_pct", {})
    if not isinstance(strong_change, dict):
        strong_change = {}
    default_strong_change = strategy_defaults.get("strong_direct_change_pct", {})
    alert_strategy["strong_direct_change_pct"] = {
        window: max(
            0.0,
            _to_float(
                strong_change.get(window),
                default=_to_float(default_strong_change.get(window), default=0.0),
            ),
        )
        for window in config["windows"]
    }
    alert_strategy["strong_direct_cooldown_minutes"] = max(
        1,
        _to_int(
            alert_strategy.get("strong_direct_cooldown_minutes"),
            default=_to_int(strategy_defaults.get("strong_direct_cooldown_minutes"), default=12),
        ),
    )
    alert_strategy["event_dedupe_enabled"] = _parse_bool(
        alert_strategy.get("event_dedupe_enabled"),
        default=bool(strategy_defaults.get("event_dedupe_enabled", True)),
    )
    alert_strategy["event_dedupe_state_file"] = str(
        alert_strategy.get(
            "event_dedupe_state_file",
            strategy_defaults.get("event_dedupe_state_file", "event_dedupe_state.json"),
        )
    ).strip() or "event_dedupe_state.json"
    alert_strategy["event_dedupe_window_minutes"] = max(
        1,
        _to_int(
            alert_strategy.get("event_dedupe_window_minutes"),
            default=_to_int(strategy_defaults.get("event_dedupe_window_minutes"), default=30),
        ),
    )
    alert_strategy["event_dedupe_min_abs_change_delta_pct"] = max(
        0.0,
        _to_float(
            alert_strategy.get("event_dedupe_min_abs_change_delta_pct"),
            default=_to_float(strategy_defaults.get("event_dedupe_min_abs_change_delta_pct"), default=3.0),
        ),
    )
    alert_strategy["event_dedupe_min_relative_change_delta"] = max(
        0.0,
        _to_float(
            alert_strategy.get("event_dedupe_min_relative_change_delta"),
            default=_to_float(strategy_defaults.get("event_dedupe_min_relative_change_delta"), default=0.35),
        ),
    )
    alert_strategy["event_dedupe_min_score_delta"] = max(
        0.0,
        _to_float(
            alert_strategy.get("event_dedupe_min_score_delta"),
            default=_to_float(strategy_defaults.get("event_dedupe_min_score_delta"), default=8.0),
        ),
    )
    alert_strategy["event_dedupe_min_risk_delta"] = max(
        0.0,
        _to_float(
            alert_strategy.get("event_dedupe_min_risk_delta"),
            default=_to_float(strategy_defaults.get("event_dedupe_min_risk_delta"), default=15.0),
        ),
    )
    alert_strategy["event_dedupe_factor_upgrade_enabled"] = _parse_bool(
        alert_strategy.get("event_dedupe_factor_upgrade_enabled"),
        default=bool(strategy_defaults.get("event_dedupe_factor_upgrade_enabled", True)),
    )
    alert_strategy["event_dedupe_window_upgrade_enabled"] = _parse_bool(
        alert_strategy.get("event_dedupe_window_upgrade_enabled"),
        default=bool(strategy_defaults.get("event_dedupe_window_upgrade_enabled", True)),
    )
    alert_strategy["event_dedupe_window_upgrade_requires_score_delta"] = _parse_bool(
        alert_strategy.get("event_dedupe_window_upgrade_requires_score_delta"),
        default=bool(strategy_defaults.get("event_dedupe_window_upgrade_requires_score_delta", True)),
    )
    alert_strategy["event_dedupe_reversal_enabled"] = _parse_bool(
        alert_strategy.get("event_dedupe_reversal_enabled"),
        default=bool(strategy_defaults.get("event_dedupe_reversal_enabled", True)),
    )
    factors = alert_strategy.setdefault("confirmation_factors", {})
    if not isinstance(factors, dict):
        factors = {}
        alert_strategy["confirmation_factors"] = factors
    factor_defaults = strategy_defaults["confirmation_factors"]
    factors["enabled"] = _parse_bool(factors.get("enabled"), default=True)
    factors["cache_ttl_sec"] = max(1.0, _to_float(factors.get("cache_ttl_sec"), factor_defaults["cache_ttl_sec"]))
    factors["min_base_score"] = max(
        0.0,
        min(100.0, _to_float(factors.get("min_base_score"), factor_defaults["min_base_score"])),
    )
    factors["min_event_count"] = max(1, _to_int(factors.get("min_event_count"), factor_defaults["min_event_count"]))

    binance_factors = factors.setdefault("binance", {})
    if not isinstance(binance_factors, dict):
        binance_factors = {}
        factors["binance"] = binance_factors
    binance_defaults = factor_defaults["binance"]
    binance_factors["enabled"] = _parse_bool(binance_factors.get("enabled"), default=True)
    binance_factors["base_url"] = str(binance_factors.get("base_url", binance_defaults["base_url"])).strip() or binance_defaults["base_url"]
    binance_factors["timeout_sec"] = max(
        0.5,
        _to_float(binance_factors.get("timeout_sec"), binance_defaults["timeout_sec"]),
    )
    binance_factors["depth_limit"] = max(5, _to_int(binance_factors.get("depth_limit"), binance_defaults["depth_limit"]))
    binance_factors["depth_levels"] = max(1, _to_int(binance_factors.get("depth_levels"), binance_defaults["depth_levels"]))
    for key in (
        "fetch_open_interest",
        "fetch_open_interest_hist",
        "fetch_funding",
        "fetch_long_short_ratio",
        "fetch_taker_buy_sell",
        "fetch_orderbook",
        "fetch_liquidations",
    ):
        binance_factors[key] = _parse_bool(binance_factors.get(key), default=bool(binance_defaults.get(key)))
    binance_factors["liquidation_lookback_minutes"] = max(
        1,
        _to_int(
            binance_factors.get("liquidation_lookback_minutes"),
            binance_defaults["liquidation_lookback_minutes"],
        ),
    )
    binance_factors["open_interest_hist_period"] = str(
        binance_factors.get("open_interest_hist_period", binance_defaults.get("open_interest_hist_period", "5m"))
    ).strip() or "5m"
    binance_factors["open_interest_hist_limit"] = max(
        2,
        _to_int(
            binance_factors.get("open_interest_hist_limit"),
            _to_int(binance_defaults.get("open_interest_hist_limit"), default=576),
        ),
    )
    binance_factors["taker_buy_sell_period"] = str(
        binance_factors.get("taker_buy_sell_period", binance_defaults.get("taker_buy_sell_period", "5m"))
    ).strip() or "5m"
    oi_history = binance_factors.setdefault("oi_history", {})
    if not isinstance(oi_history, dict):
        oi_history = {}
        binance_factors["oi_history"] = oi_history
    oi_defaults = binance_defaults.get("oi_history", {}) if isinstance(binance_defaults.get("oi_history"), dict) else {}
    oi_history["enabled"] = _parse_bool(oi_history.get("enabled"), default=bool(oi_defaults.get("enabled", True)))
    oi_history["history_file"] = str(oi_history.get("history_file", oi_defaults.get("history_file", "oi_history.json"))).strip() or "oi_history.json"
    oi_history["max_samples_per_symbol"] = max(
        50,
        _to_int(oi_history.get("max_samples_per_symbol"), _to_int(oi_defaults.get("max_samples_per_symbol"), 1200)),
    )
    oi_history["bootstrap_ttl_sec"] = max(
        30,
        _to_int(oi_history.get("bootstrap_ttl_sec"), _to_int(oi_defaults.get("bootstrap_ttl_sec"), 300)),
    )
    oi_history["min_bootstrap_samples"] = max(
        2,
        _to_int(oi_history.get("min_bootstrap_samples"), _to_int(oi_defaults.get("min_bootstrap_samples"), 12)),
    )
    oi_history["min_zscore_samples"] = max(
        8,
        _to_int(oi_history.get("min_zscore_samples"), _to_int(oi_defaults.get("min_zscore_samples"), 20)),
    )
    if not isinstance(oi_history.get("signal_thresholds"), dict):
        oi_history["signal_thresholds"] = dict(oi_defaults.get("signal_thresholds") or {})

    microstructure = binance_factors.setdefault("microstructure", {})
    if not isinstance(microstructure, dict):
        microstructure = {}
        binance_factors["microstructure"] = microstructure
    micro_defaults = (
        binance_defaults.get("microstructure", {})
        if isinstance(binance_defaults.get("microstructure"), dict)
        else {}
    )
    microstructure["enabled"] = _parse_bool(microstructure.get("enabled"), default=bool(micro_defaults.get("enabled", True)))
    microstructure["ws_url"] = str(
        microstructure.get("ws_url", micro_defaults.get("ws_url", "wss://fstream.binance.com/ws"))
    ).strip() or "wss://fstream.binance.com/ws"
    microstructure["track_ttl_sec"] = max(
        60,
        _to_int(microstructure.get("track_ttl_sec"), _to_int(micro_defaults.get("track_ttl_sec"), 900)),
    )
    microstructure["subscription_refresh_sec"] = max(
        1,
        _to_int(
            microstructure.get("subscription_refresh_sec"),
            _to_int(micro_defaults.get("subscription_refresh_sec"), 5),
        ),
    )
    microstructure["reconnect_sec"] = max(
        1,
        _to_int(microstructure.get("reconnect_sec"), _to_int(micro_defaults.get("reconnect_sec"), 3)),
    )
    microstructure["max_active_symbols"] = max(
        1,
        _to_int(microstructure.get("max_active_symbols"), _to_int(micro_defaults.get("max_active_symbols"), 40)),
    )
    microstructure["max_trades_per_symbol"] = max(
        500,
        _to_int(
            microstructure.get("max_trades_per_symbol"),
            _to_int(micro_defaults.get("max_trades_per_symbol"), 5000),
        ),
    )
    microstructure["max_liquidations_per_symbol"] = max(
        100,
        _to_int(
            microstructure.get("max_liquidations_per_symbol"),
            _to_int(micro_defaults.get("max_liquidations_per_symbol"), 1000),
        ),
    )
    raw_micro_windows = microstructure.get("windows_sec", micro_defaults.get("windows_sec", [10, 30, 60, 180, 300]))
    if not isinstance(raw_micro_windows, list):
        raw_micro_windows = [10, 30, 60, 180, 300]
    normalized_micro_windows: List[int] = []
    for item in raw_micro_windows:
        seconds = _to_int(item, 0)
        if seconds > 0 and seconds not in normalized_micro_windows:
            normalized_micro_windows.append(seconds)
    if not normalized_micro_windows:
        normalized_micro_windows = [10, 30, 60, 180, 300]
    microstructure["windows_sec"] = normalized_micro_windows
    merged_micro_thresholds = dict(micro_defaults.get("signal_thresholds") or {})
    if isinstance(microstructure.get("signal_thresholds"), dict):
        merged_micro_thresholds.update(microstructure["signal_thresholds"])
    microstructure["signal_thresholds"] = merged_micro_thresholds

    accumulation_factors = factors.setdefault("accumulation_pool", {})
    if not isinstance(accumulation_factors, dict):
        accumulation_factors = {}
        factors["accumulation_pool"] = accumulation_factors
    accumulation_defaults = factor_defaults.get("accumulation_pool", {})
    if not isinstance(accumulation_defaults, dict):
        accumulation_defaults = {}
    accumulation_factors["enabled"] = _parse_bool(
        accumulation_factors.get("enabled"),
        default=bool(accumulation_defaults.get("enabled", True)),
    )
    accumulation_factors["runtime_dir"] = str(
        accumulation_factors.get("runtime_dir", accumulation_defaults.get("runtime_dir", ""))
    ).strip()
    accumulation_factors["pool_file"] = str(
        accumulation_factors.get("pool_file", accumulation_defaults.get("pool_file", "accumulation_pool.json"))
    ).strip() or "accumulation_pool.json"
    accumulation_factors["history_file"] = str(
        accumulation_factors.get(
            "history_file",
            accumulation_defaults.get("history_file", "accumulation_pool_history.jsonl"),
        )
    ).strip() or "accumulation_pool_history.jsonl"
    accumulation_factors["max_age_hours"] = max(
        1.0,
        _to_float(accumulation_factors.get("max_age_hours"), _to_float(accumulation_defaults.get("max_age_hours"), 36)),
    )
    accumulation_factors["cache_ttl_sec"] = max(
        5.0,
        _to_float(accumulation_factors.get("cache_ttl_sec"), _to_float(accumulation_defaults.get("cache_ttl_sec"), 60)),
    )
    accumulation_scan = accumulation_factors.setdefault("scan", {})
    if not isinstance(accumulation_scan, dict):
        accumulation_scan = {}
        accumulation_factors["scan"] = accumulation_scan
    accumulation_scan_defaults = accumulation_defaults.get("scan", {}) if isinstance(accumulation_defaults.get("scan"), dict) else {}
    accumulation_scan["enabled"] = _parse_bool(
        accumulation_scan.get("enabled"),
        default=bool(accumulation_scan_defaults.get("enabled", True)),
    )
    accumulation_scan["base_url"] = str(
        accumulation_scan.get("base_url", accumulation_scan_defaults.get("base_url", "https://fapi.binance.com"))
    ).strip().rstrip("/") or "https://fapi.binance.com"
    for key, default_value in {
        "lookback_days": 210,
        "min_data_days": 60,
        "min_sideways_days": 45,
        "max_symbols": 0,
        "concurrency": 2,
    }.items():
        accumulation_scan[key] = max(0, _to_int(accumulation_scan.get(key), _to_int(accumulation_scan_defaults.get(key), default_value)))
    for key, default_value in {
        "max_range_pct": 80.0,
        "max_slope_pct": 20.0,
        "min_avg_quote_vol_usdt": 500000.0,
        "max_avg_quote_vol_usdt": 20000000.0,
        "warming_vol_ratio": 1.5,
        "ready_vol_ratio": 2.5,
        "max_recent_pump_pct": 250.0,
        "min_score": 45.0,
        "request_delay_sec": 0.15,
    }.items():
        accumulation_scan[key] = max(
            0.0,
            _to_float(accumulation_scan.get(key), _to_float(accumulation_scan_defaults.get(key), default_value)),
        )
    accumulation_scan["concurrency"] = max(1, accumulation_scan["concurrency"])
    accumulation_scan["market_cap_api_enabled"] = _parse_bool(
        accumulation_scan.get("market_cap_api_enabled"),
        default=bool(accumulation_scan_defaults.get("market_cap_api_enabled", True)),
    )

    data_feed = config.setdefault("data_feed", {})
    data_feed["kline_limit"] = max(30, _to_int(data_feed.get("kline_limit"), default=200))
    data_feed["volume_lookback"] = max(1, _to_int(data_feed.get("volume_lookback"), default=20))
    data_feed["close_candle_only"] = _parse_bool(data_feed.get("close_candle_only"), default=True)
    data_feed["max_concurrent_requests"] = max(
        1,
        _to_int(data_feed.get("max_concurrent_requests"), default=8),
    )
    data_feed["universe_refresh_sec"] = max(
        0,
        _to_int(data_feed.get("universe_refresh_sec"), default=60),
    )
    universe_mode = str(data_feed.get("symbol_universe_mode", "configured")).strip().lower()
    if universe_mode not in {"configured", "all_usdt_perp"}:
        universe_mode = "configured"
    data_feed["symbol_universe_mode"] = universe_mode

    include_symbols = data_feed.get("include_symbols", [])
    exclude_symbols = data_feed.get("exclude_symbols", [])
    if not isinstance(include_symbols, list):
        include_symbols = []
    if not isinstance(exclude_symbols, list):
        exclude_symbols = []

    def _norm_list(items: List[Any]) -> List[str]:
        out: List[str] = []
        seen: set[str] = set()
        for item in items:
            normalized = _normalize_symbol(str(item))
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            out.append(normalized)
        return out

    data_feed["include_symbols"] = _norm_list(include_symbols)
    data_feed["exclude_symbols"] = _norm_list(exclude_symbols)
    data_feed["min_quote_volume_usdt"] = max(0.0, _to_float(data_feed.get("min_quote_volume_usdt"), default=0.0))
    data_feed["max_quote_volume_usdt"] = max(0.0, _to_float(data_feed.get("max_quote_volume_usdt"), default=0.0))
    if data_feed["max_quote_volume_usdt"] > 0 and data_feed["max_quote_volume_usdt"] < data_feed["min_quote_volume_usdt"]:
        data_feed["max_quote_volume_usdt"] = data_feed["min_quote_volume_usdt"]
    data_feed["max_symbols"] = max(0, _to_int(data_feed.get("max_symbols"), default=0))
    data_feed["window_schedule_enabled"] = _parse_bool(data_feed.get("window_schedule_enabled"), default=True)
    data_feed["symbol_batch_size"] = max(0, _to_int(data_feed.get("symbol_batch_size"), default=25))
    data_feed["exclude_top_quote_volume_enabled"] = _parse_bool(
        data_feed.get("exclude_top_quote_volume_enabled"),
        default=False,
    )
    data_feed["exclude_top_quote_volume_n"] = max(
        0,
        _to_int(data_feed.get("exclude_top_quote_volume_n"), default=0),
    )

    data_feed["ws_realtime_enabled"] = _parse_bool(data_feed.get("ws_realtime_enabled"), default=True)
    ws_windows = data_feed.get("ws_realtime_windows", [])
    if not isinstance(ws_windows, list):
        ws_windows = []
    normalized_ws_windows = [str(w).strip() for w in ws_windows if str(w).strip() in VALID_WINDOWS]
    if not normalized_ws_windows:
        normalized_ws_windows = ["1m"]
    data_feed["ws_realtime_windows"] = normalized_ws_windows
    data_feed["ws_realtime_skip_poll_windows"] = _parse_bool(
        data_feed.get("ws_realtime_skip_poll_windows"),
        default=True,
    )

    ws_prefilter = data_feed.get("ws_realtime_prefilter_pct", {})
    normalized_ws_prefilter: Dict[str, float] = {}
    if isinstance(ws_prefilter, dict):
        for window in data_feed["ws_realtime_windows"]:
            default_ws_pref = _to_float(
                DEFAULT_CONFIG["data_feed"].get("ws_realtime_prefilter_pct", {}).get(window),
                default=1.0,
            )
            normalized_ws_prefilter[window] = max(
                0.0,
                _to_float(ws_prefilter.get(window), default=default_ws_pref),
            )
    else:
        scalar_pref = max(0.0, _to_float(ws_prefilter, default=1.0))
        for window in data_feed["ws_realtime_windows"]:
            normalized_ws_prefilter[window] = scalar_pref
    data_feed["ws_realtime_prefilter_pct"] = normalized_ws_prefilter

    data_feed["ws_realtime_recheck_interval_sec"] = max(
        0.5,
        _to_float(data_feed.get("ws_realtime_recheck_interval_sec"), default=6.0),
    )
    data_feed["ws_realtime_reconnect_sec"] = max(
        1,
        _to_int(data_feed.get("ws_realtime_reconnect_sec"), default=3),
    )
    data_feed["ws_realtime_no_message_reconnect_sec"] = max(
        0.0,
        _to_float(
            data_feed.get("ws_realtime_no_message_reconnect_sec"),
            default=_to_float(
                DEFAULT_CONFIG["data_feed"].get("ws_realtime_no_message_reconnect_sec"),
                default=45.0,
            ),
        ),
    )
    data_feed["ws_realtime_subscription_chunk_size"] = max(
        1,
        _to_int(data_feed.get("ws_realtime_subscription_chunk_size"), default=180),
    )
    data_feed["ws_realtime_symbol_refresh_sec"] = max(
        5,
        _to_int(data_feed.get("ws_realtime_symbol_refresh_sec"), default=60),
    )
    data_feed["ws_realtime_url"] = str(
        data_feed.get(
            "ws_realtime_url",
            DEFAULT_CONFIG["data_feed"].get("ws_realtime_url", "wss://fstream.binance.com/ws"),
        )
    ).strip()

    data_feed["ws_local_agg_enabled"] = _parse_bool(
        data_feed.get("ws_local_agg_enabled"),
        default=bool(DEFAULT_CONFIG["data_feed"].get("ws_local_agg_enabled", True)),
    )
    local_agg_windows = data_feed.get("ws_local_agg_windows", [])
    if not isinstance(local_agg_windows, list):
        local_agg_windows = []
    normalized_local_agg_windows = [
        str(w).strip()
        for w in local_agg_windows
        if str(w).strip() in {"5m", "15m", "30m", "1h"}
    ]
    if not normalized_local_agg_windows and data_feed["ws_local_agg_enabled"]:
        default_local_agg_windows = DEFAULT_CONFIG["data_feed"].get("ws_local_agg_windows", ["15m", "30m", "1h"])
        if not isinstance(default_local_agg_windows, list):
            default_local_agg_windows = ["15m", "30m", "1h"]
        normalized_local_agg_windows = [
            str(w).strip()
            for w in default_local_agg_windows
            if str(w).strip() in {"5m", "15m", "30m", "1h"}
        ]
    data_feed["ws_local_agg_windows"] = normalized_local_agg_windows
    data_feed["ws_local_agg_skip_poll_windows"] = _parse_bool(
        data_feed.get("ws_local_agg_skip_poll_windows"),
        default=bool(DEFAULT_CONFIG["data_feed"].get("ws_local_agg_skip_poll_windows", True)),
    )
    data_feed["ws_local_agg_bootstrap_concurrency"] = max(
        1,
        _to_int(
            data_feed.get("ws_local_agg_bootstrap_concurrency"),
            default=_to_int(DEFAULT_CONFIG["data_feed"].get("ws_local_agg_bootstrap_concurrency"), default=2),
        ),
    )
    data_feed["ws_local_agg_history_limit"] = max(
        80,
        _to_int(
            data_feed.get("ws_local_agg_history_limit"),
            default=_to_int(DEFAULT_CONFIG["data_feed"].get("ws_local_agg_history_limit"), default=200),
        ),
    )

    full_scan_windows = data_feed.get("full_scan_windows", [])
    if not isinstance(full_scan_windows, list):
        full_scan_windows = []
    data_feed["full_scan_windows"] = [str(w).strip() for w in full_scan_windows if str(w).strip() in VALID_WINDOWS]

    data_feed["priority_scan_enabled"] = _parse_bool(
        data_feed.get("priority_scan_enabled"),
        default=bool(DEFAULT_CONFIG["data_feed"].get("priority_scan_enabled", True)),
    )
    data_feed["priority_scan_top_n"] = max(
        0,
        _to_int(
            data_feed.get("priority_scan_top_n"),
            default=_to_int(DEFAULT_CONFIG["data_feed"].get("priority_scan_top_n"), default=50),
        ),
    )
    priority_windows = data_feed.get("priority_scan_windows", [])
    if not isinstance(priority_windows, list):
        priority_windows = []
    normalized_priority_windows = [
        str(w).strip()
        for w in priority_windows
        if str(w).strip() in VALID_WINDOWS
    ]
    if not normalized_priority_windows and data_feed["priority_scan_enabled"]:
        normalized_priority_windows = ["1m", "5m"]
    data_feed["priority_scan_windows"] = normalized_priority_windows
    data_feed["priority_scan_min_24h_change_pct"] = _to_float(
        data_feed.get("priority_scan_min_24h_change_pct"),
        default=0.0,
    )
    data_feed["scan_coverage_file"] = str(
        data_feed.get(
            "scan_coverage_file",
            DEFAULT_CONFIG["data_feed"].get("scan_coverage_file", "scan_coverage.json"),
        )
    ).strip()
    data_feed["scan_coverage_flush_sec"] = max(
        1.0,
        _to_float(
            data_feed.get("scan_coverage_flush_sec"),
            default=_to_float(DEFAULT_CONFIG["data_feed"].get("scan_coverage_flush_sec"), default=15.0),
        ),
    )
    data_feed["ws_gap_detection_enabled"] = _parse_bool(
        data_feed.get("ws_gap_detection_enabled"),
        default=bool(DEFAULT_CONFIG["data_feed"].get("ws_gap_detection_enabled", True)),
    )
    ws_gap_window = str(data_feed.get("ws_gap_window", "1m")).strip()
    data_feed["ws_gap_window"] = ws_gap_window if ws_gap_window in VALID_WINDOWS else "1m"
    data_feed["ws_gap_min_silence_sec"] = max(
        5.0,
        _to_float(
            data_feed.get("ws_gap_min_silence_sec"),
            default=_to_float(DEFAULT_CONFIG["data_feed"].get("ws_gap_min_silence_sec"), default=90.0),
        ),
    )
    ws_gap_min_change = data_feed.get("ws_gap_min_change_pct", {})
    if not isinstance(ws_gap_min_change, dict):
        ws_gap_min_change = {}
    default_ws_gap_min_change = DEFAULT_CONFIG["data_feed"].get("ws_gap_min_change_pct", {})
    data_feed["ws_gap_min_change_pct"] = {
        window: max(
            0.0,
            _to_float(
                ws_gap_min_change.get(window),
                default=_to_float(default_ws_gap_min_change.get(window), default=0.0),
            ),
        )
        for window in config["windows"]
    }

    batch_by_window = data_feed.get("symbol_batch_size_by_window", {})
    if not isinstance(batch_by_window, dict):
        batch_by_window = {}
    default_batch_map = DEFAULT_CONFIG["data_feed"].get("symbol_batch_size_by_window", {})
    normalized_batch_by_window: Dict[str, int] = {}
    for window in config["windows"]:
        default_batch = _to_int(default_batch_map.get(window), default=data_feed["symbol_batch_size"])
        normalized_batch_by_window[window] = max(
            0,
            _to_int(batch_by_window.get(window), default=default_batch),
        )
    data_feed["symbol_batch_size_by_window"] = normalized_batch_by_window

    config["log_level"] = str(config.get("log_level", "INFO")).upper()
    config["timezone"] = str(config.get("timezone", "Asia/Shanghai")).strip()
    return config


def load_alert_config(config_path: Optional[str] = None) -> AlertConfig:
    env_files = _load_dotenv_files()
    resolved_config_path = _resolve_config_path(config_path)

    merged = copy.deepcopy(DEFAULT_CONFIG)
    file_config = _load_json_file(resolved_config_path)
    if file_config:
        _deep_merge(merged, file_config)

    _apply_env_overrides(merged)
    _normalize_config(merged)

    logger.info("Alert config loaded from: %s", resolved_config_path)
    if env_files:
        logger.info("dotenv files loaded: %s", ", ".join(env_files))

    return AlertConfig(
        symbols=merged["symbols"],
        windows=merged["windows"],
        poll_interval_sec=merged["poll_interval_sec"],
        timezone=merged["timezone"],
        log_level=merged["log_level"],
        telegram=merged["telegram"],
        atr=merged["atr"],
        indicators=merged["indicators"],
        rules=merged["rules"],
        cooldown_minutes=merged["cooldown_minutes"],
        confidence=merged["confidence"],
        alert_strategy=merged["alert_strategy"],
        data_feed=merged["data_feed"],
        config_path=str(resolved_config_path),
        loaded_env_files=env_files,
        raw=merged,
    )


def load_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Backward-compatible helper for scripts expecting dict output.
    """
    return load_alert_config(config_path=config_path).raw
