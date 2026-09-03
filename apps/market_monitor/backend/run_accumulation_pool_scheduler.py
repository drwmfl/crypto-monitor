from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import aiohttp
except Exception:  # pragma: no cover
    aiohttp = None

try:
    from alert_config import load_alert_config
    from candidates.storage_paths import resolve_runtime_dir
except ModuleNotFoundError:
    from apps.market_monitor.backend.alert_config import load_alert_config
    from apps.market_monitor.backend.candidates.storage_paths import resolve_runtime_dir


logger = logging.getLogger(__name__)
BEIJING_TZ = timezone(timedelta(hours=8))
READY_STATUSES = {"ready", "warming"}
STATUS_LABELS = {
    "ready": "放量",
    "warming": "升温",
    "dormant": "沉淀",
}
FOCUS_DISPLAY_LIMIT = 10
RISK_DISPLAY_LIMIT = 5
MIN_FOCUS_SIDEWAYS_DAYS = 80
MAX_FOCUS_VOL_RATIO = 5.0
MAX_FOCUS_RANGE_POSITION = 1.0


def _configure_stdio() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8")
            except (OSError, ValueError):
                pass


def _env_int(name: str, default: int) -> int:
    try:
        value = os.getenv(name)
        return int(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    try:
        value = os.getenv(name)
        return float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the daily accumulation-pool scan and Telegram summary.")
    parser.add_argument("--config-path", type=str, default=os.getenv("ALERT_CONFIG_PATH", ""))
    parser.add_argument("--run-once", action="store_true", help="Run one scan cycle immediately, then exit.")
    parser.add_argument("--summary-only", action="store_true", help="Read the latest pool and build/send summary only.")
    parser.add_argument("--no-send", action="store_true", help="Print the summary but do not send Telegram.")
    parser.add_argument("--scan-hour", type=int, default=_env_int("ACCUMULATION_SCANNER_HOUR", 21))
    parser.add_argument("--scan-minute", type=int, default=_env_int("ACCUMULATION_SCANNER_MINUTE", 0))
    parser.add_argument("--max-attempts", type=int, default=_env_int("ACCUMULATION_SCANNER_MAX_ATTEMPTS", 3))
    parser.add_argument(
        "--retry-delay-sec",
        type=float,
        default=_env_float("ACCUMULATION_SCANNER_RETRY_DELAY_SEC", 1800.0),
    )
    parser.add_argument("--summary-limit", type=int, default=_env_int("ACCUMULATION_SCANNER_SUMMARY_LIMIT", 20))
    parser.add_argument("--concurrency", type=int, default=_env_int("ACCUMULATION_SCANNER_CONCURRENCY", 1))
    parser.add_argument("--scan-log-level", type=str, default=os.getenv("ACCUMULATION_SCANNER_SCAN_LOG_LEVEL", "INFO"))
    parser.add_argument("--log-level", type=str, default=os.getenv("ACCUMULATION_SCANNER_LOG_LEVEL", "INFO"))
    return parser.parse_args()


async def main_async() -> int:
    _configure_stdio()
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level or "INFO").upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    args.max_attempts = max(1, int(args.max_attempts))
    args.retry_delay_sec = max(1.0, float(args.retry_delay_sec))
    args.summary_limit = max(1, int(args.summary_limit))
    args.concurrency = max(1, int(args.concurrency))
    args.scan_hour = max(0, min(23, int(args.scan_hour)))
    args.scan_minute = max(0, min(59, int(args.scan_minute)))

    if args.summary_only:
        payload, pool_path = _load_pool_payload(args.config_path)
        message = _build_summary_message(payload, pool_path, args.summary_limit)
        print(message)
        if not args.no_send:
            sent = await _send_telegram(message)
            if sent:
                _record_sent_report(payload, pool_path, args.summary_limit, message, args.config_path)
            return 0 if sent else 1
        return 0

    if args.run_once:
        return 0 if await _run_cycle(args) else 1

    logger.info(
        "Accumulation pool scheduler started: daily=%02d:%02d UTC+8 attempts=%s retry_delay=%.0fs",
        args.scan_hour,
        args.scan_minute,
        args.max_attempts,
        args.retry_delay_sec,
    )
    while True:
        target = _next_run_at(args.scan_hour, args.scan_minute)
        sleep_sec = max(1.0, (target - datetime.now(BEIJING_TZ)).total_seconds())
        logger.info("Next accumulation scan scheduled at %s", target.strftime("%Y-%m-%d %H:%M:%S UTC+8"))
        await asyncio.sleep(sleep_sec)
        try:
            await _run_cycle(args)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unexpected accumulation scheduler cycle error.")


async def _run_cycle(args: argparse.Namespace) -> bool:
    cycle_started_at = datetime.now(timezone.utc)
    last_error = ""
    for attempt in range(1, int(args.max_attempts) + 1):
        logger.info("Starting accumulation scan attempt %s/%s", attempt, args.max_attempts)
        ok, detail = await _run_scan_subprocess(args)
        if ok:
            payload, pool_path = _load_pool_payload(args.config_path)
            valid, validation_error = _validate_fresh_payload(payload, cycle_started_at)
            if valid:
                message = _build_summary_message(payload, pool_path, args.summary_limit)
                print(message)
                if args.no_send:
                    logger.info("Telegram send skipped by --no-send.")
                    return True
                sent = await _send_telegram(message)
                if sent:
                    _record_sent_report(payload, pool_path, args.summary_limit, message, args.config_path)
                    logger.info("Accumulation summary sent successfully.")
                    return True
                last_error = "telegram_send_failed"
            else:
                last_error = validation_error
        else:
            last_error = detail

        logger.warning("Accumulation scan attempt failed: %s", last_error)
        if attempt < int(args.max_attempts):
            logger.info("Retrying accumulation scan in %.0f seconds.", args.retry_delay_sec)
            await asyncio.sleep(float(args.retry_delay_sec))

    failure_message = _build_failure_message(args.max_attempts, last_error)
    if args.no_send:
        print(failure_message)
    else:
        await _send_telegram(failure_message)
    return False


async def _run_scan_subprocess(args: argparse.Namespace) -> Tuple[bool, str]:
    scan_path = Path(__file__).with_name("scan_accumulation_pool.py")
    cmd = [
        sys.executable,
        str(scan_path),
        "--concurrency",
        str(args.concurrency),
        "--log-level",
        str(args.scan_log_level or "INFO"),
    ]
    if args.config_path:
        cmd.extend(["--config-path", str(args.config_path)])

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(Path(__file__).resolve().parent),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await proc.communicate()
    except Exception as exc:
        logger.exception("Failed to start accumulation scanner.")
        return False, f"start_failed={exc}"

    output = stdout.decode("utf-8", errors="replace").strip() if stdout else ""
    if output:
        logger.info("Accumulation scanner output tail:\n%s", output[-6000:])

    if proc.returncode == 0:
        return True, "ok"
    return False, f"exit_code={proc.returncode}"


def _load_pool_payload(config_path: str = "") -> Tuple[Dict[str, Any], Path]:
    factor_settings = _accumulation_settings(config_path)
    runtime_dir = resolve_runtime_dir(factor_settings)
    pool_file = str(factor_settings.get("pool_file") or "accumulation_pool.json")
    pool_path = Path(pool_file)
    if not pool_path.is_absolute():
        pool_path = runtime_dir / pool_path
    if not pool_path.exists():
        return {}, pool_path
    try:
        payload = json.loads(pool_path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to read accumulation pool: %s", pool_path)
        return {}, pool_path
    return payload if isinstance(payload, dict) else {}, pool_path


def _validate_fresh_payload(payload: Dict[str, Any], cycle_started_at: datetime) -> Tuple[bool, str]:
    if not payload:
        return False, "pool_missing"
    generated_at = _parse_datetime(str(payload.get("generated_at") or ""))
    if generated_at is None:
        return False, "pool_generated_at_missing"
    if generated_at < cycle_started_at - timedelta(minutes=5):
        return False, "pool_not_refreshed"
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        return False, "pool_symbols_invalid"
    if int(payload.get("count") or 0) <= 0:
        return False, "pool_empty"
    return True, ""


def _select_summary_items(payload: Dict[str, Any], limit: int) -> Dict[str, Any]:
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        symbols = {}

    display_cap = max(1, int(limit))
    items = [item for item in symbols.values() if _status(item) in READY_STATUSES]
    items.sort(key=_summary_sort_key)
    status_counts = _status_counts(symbols.values())

    focus_candidates = [item for item in items if _is_focus_observation(item)]
    risk_candidates = [item for item in items if _is_risk_observation(item)]
    focus_symbols = {_symbol_key(item) for item in focus_candidates}
    risk_symbols = {_symbol_key(item) for item in risk_candidates}
    backup_candidates = [
        item
        for item in items
        if _symbol_key(item) not in focus_symbols and _symbol_key(item) not in risk_symbols
    ]

    focus_items = focus_candidates[: min(FOCUS_DISPLAY_LIMIT, display_cap)]
    remaining_cap = max(0, display_cap - len(focus_items))
    risk_cap = min(RISK_DISPLAY_LIMIT, len(risk_candidates), remaining_cap)
    backup_cap = max(0, remaining_cap - risk_cap)
    backup_items = backup_candidates[:backup_cap]
    risk_items = risk_candidates[:risk_cap]
    return {
        "symbols": symbols,
        "items": items,
        "status_counts": status_counts,
        "focus_items": focus_items,
        "backup_items": backup_items,
        "risk_items": risk_items,
        "hidden_count": max(0, len(items) - len(focus_items) - len(backup_items) - len(risk_items)),
    }


def _build_summary_message(payload: Dict[str, Any], pool_path: Path, limit: int) -> str:
    selection = _select_summary_items(payload, limit)
    symbols = selection["symbols"]
    items = selection["items"]
    status_counts = selection["status_counts"]
    focus_items = selection["focus_items"]
    backup_items = selection["backup_items"]
    risk_items = selection["risk_items"]

    generated_at = _format_beijing_time(str(payload.get("generated_at") or ""))
    total_count = int(payload.get("count") or len(symbols))
    lines = [
        f"<b>📋 吸筹池日报 | 重点观察 Top{len(focus_items)}</b>",
        f"🕒 时间：{_html(generated_at)}",
        (
            f"📊 概览：入池 <b>{total_count}</b> | "
            f"放量 <b>{status_counts.get('ready', 0)}</b> | "
            f"升温 <b>{status_counts.get('warming', 0)}</b> | "
            f"沉淀 <b>{status_counts.get('dormant', 0)}</b>"
        ),
        "🎯 筛选：区间内、量能不过热、横盘>=80天优先；突破/爆量只列风险观察",
        f"📁 文件：<code>{_html(pool_path.name)}</code>",
        "",
    ]

    if not items:
        lines.append("ℹ️ 暂无放量/升温标的。")
        return "\n".join(lines)

    lines.append("<b>🎯 重点观察</b>")
    if focus_items:
        for idx, item in enumerate(focus_items, start=1):
            if idx > 1:
                lines.append("")
            _append_focus_item(lines, idx, item)
    else:
        lines.append("ℹ️ 暂无符合重点条件，今日只保留备选/风险观察。")

    if backup_items:
        lines.extend(["", "<b>👀 备选观察</b>"])
        for idx, item in enumerate(backup_items, start=1):
            lines.append(_compact_item_line(idx, item, include_flags=False))

    if risk_items:
        lines.extend(["", "<b>🚨 风险观察</b>"])
        for idx, item in enumerate(risk_items, start=1):
            lines.append(_compact_item_line(idx, item, include_flags=True))

    hidden_count = selection["hidden_count"]
    if hidden_count:
        lines.extend(["", f"ℹ️ 其余 {hidden_count} 个放量/升温标的已收起，避免日报噪音过多。"])
    return "\n".join(lines)


def _accumulation_settings(config_path: str = "") -> Dict[str, Any]:
    config = load_alert_config(config_path=config_path or None).raw
    factor_settings = (
        config.get("alert_strategy", {})
        .get("confirmation_factors", {})
        .get("accumulation_pool", {})
    )
    return factor_settings if isinstance(factor_settings, dict) else {}


def _record_sent_report(
    payload: Dict[str, Any],
    pool_path: Path,
    limit: int,
    message: str,
    config_path: str = "",
) -> bool:
    try:
        settings = _accumulation_settings(config_path)
        runtime_dir = resolve_runtime_dir(settings)
        ledger_file = str(settings.get("report_ledger_file") or "accumulation_pool_report_ledger.jsonl")
        ledger_path = Path(ledger_file)
        if not ledger_path.is_absolute():
            ledger_path = runtime_dir / ledger_path
        row = _build_report_record(payload, pool_path, limit, message)
        appended = _append_report_ledger(ledger_path, row)
        if appended:
            logger.info("Recorded sent accumulation report: %s report_id=%s", ledger_path, row["report_id"])
        else:
            logger.info("Accumulation report already recorded: report_id=%s", row["report_id"])
        return True
    except Exception:
        logger.exception("Failed to record sent accumulation report; Telegram delivery remains successful.")
        return False


def _build_report_record(
    payload: Dict[str, Any],
    pool_path: Path,
    limit: int,
    message: str,
    *,
    sent_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    selection = _select_summary_items(payload, limit)
    sent_time = sent_at or datetime.now(timezone.utc)
    generated_at = str(payload.get("generated_at") or "")
    message_hash = hashlib.sha256(message.encode("utf-8")).hexdigest()
    report_id = hashlib.sha256(f"{generated_at}|{message_hash}".encode("utf-8")).hexdigest()[:24]
    eligible_ranks = {
        _symbol_key(item): rank
        for rank, item in enumerate(selection["items"], start=1)
    }

    displayed: List[Dict[str, Any]] = []
    display_order = 0
    for group, group_items in (
        ("focus", selection["focus_items"]),
        ("backup", selection["backup_items"]),
        ("risk", selection["risk_items"]),
    ):
        for group_rank, item in enumerate(group_items, start=1):
            display_order += 1
            displayed.append(
                _report_item(
                    item,
                    group=group,
                    group_rank=group_rank,
                    display_order=display_order,
                    legacy_rank=eligible_ranks.get(_symbol_key(item)),
                )
            )

    shadow_summary = payload.get("shadow") if isinstance(payload.get("shadow"), dict) else {}
    return {
        "report_id": report_id,
        "policy_version": str(shadow_summary.get("version") or "accumulation-pool-shadow-v1"),
        "send_status": "sent",
        "generated_at": generated_at,
        "sent_at": sent_time.astimezone(timezone.utc).isoformat(),
        "report_date_beijing": sent_time.astimezone(BEIJING_TZ).date().isoformat(),
        "source_pool_file": pool_path.name,
        "source_payload_version": payload.get("version"),
        "daily_data_cutoff_at": _latest_item_timestamp(displayed, "daily_data_cutoff_at"),
        "market_data_cutoff_at": _latest_item_timestamp(displayed, "market_data_cutoff_at"),
        "shadow_observed_at": str(shadow_summary.get("observed_at") or ""),
        "summary_limit": max(1, int(limit)),
        "pool_count": int(payload.get("count") or len(selection["symbols"])),
        "eligible_count": len(selection["items"]),
        "displayed_count": len(displayed),
        "hidden_count": selection["hidden_count"],
        "group_counts": {
            "focus": len(selection["focus_items"]),
            "backup": len(selection["backup_items"]),
            "risk": len(selection["risk_items"]),
        },
        "status_counts": selection["status_counts"],
        "displayed_symbols": [_symbol_key(item) for item in displayed],
        "items": displayed,
        "message_sha256": message_hash,
        "message_html": message,
    }


def _report_item(
    item: Dict[str, Any],
    *,
    group: str,
    group_rank: int,
    display_order: int,
    legacy_rank: Optional[int],
) -> Dict[str, Any]:
    components = item.get("component_scores") if isinstance(item.get("component_scores"), dict) else {}
    shadow = item.get("shadow") if isinstance(item.get("shadow"), dict) else {}
    return {
        "symbol": _symbol_key(item),
        "base_asset": str(item.get("base_asset") or _base_from_symbol(_symbol_key(item)) or "").upper(),
        "group": group,
        "group_rank": group_rank,
        "display_order": display_order,
        "legacy_rank": legacy_rank,
        "status": _status(item),
        "score": _safe_float(item.get("score")),
        "component_scores": dict(components),
        "sideways_days": _safe_int(item.get("sideways_days")),
        "range_pct": _safe_float(item.get("range_pct")),
        "range_position": _safe_float(item.get("range_position")),
        "recent_vol_ratio_7d": _safe_float(item.get("recent_vol_ratio_7d")),
        "market_cap": _safe_float(item.get("market_cap")),
        "data_quality": str(item.get("data_quality") or ""),
        "daily_data_cutoff_at": str(item.get("daily_data_cutoff_at") or shadow.get("daily_data_cutoff_at") or ""),
        "market_data_cutoff_at": str(shadow.get("market_data_cutoff_at") or ""),
        "shadow": dict(shadow),
    }


def _latest_item_timestamp(items: Sequence[Dict[str, Any]], key: str) -> str:
    timestamps = [
        parsed
        for parsed in (_parse_datetime(str(item.get(key) or "")) for item in items)
        if parsed is not None
    ]
    return max(timestamps).isoformat() if timestamps else ""


def _append_report_ledger(path: Path, row: Dict[str, Any]) -> bool:
    report_id = str(row.get("report_id") or "")
    if not report_id:
        raise ValueError("report_id is required")
    if path.exists():
        with path.open("r", encoding="utf-8") as existing:
            for line in existing:
                try:
                    prior = json.loads(line)
                except (json.JSONDecodeError, TypeError):
                    continue
                if isinstance(prior, dict) and str(prior.get("report_id") or "") == report_id:
                    return False

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as output:
        output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    return True


def _summary_sort_key(item: Any) -> tuple[float, float, float, str]:
    if not isinstance(item, dict):
        return (1.0, 0.0, 0.0, "")
    status_priority = 0.0 if _status(item) == "ready" else 0.2
    return (
        -_safe_float(item.get("score")),
        status_priority,
        -_safe_float(item.get("sideways_days")),
        _symbol_key(item),
    )


def _symbol_key(item: Any) -> str:
    if not isinstance(item, dict):
        return ""
    return str(item.get("symbol") or "").upper()


def _is_focus_observation(item: Dict[str, Any]) -> bool:
    if _status(item) not in READY_STATUSES:
        return False
    range_position = _safe_float(item.get("range_position"))
    vol_ratio = _safe_float(item.get("recent_vol_ratio_7d"))
    sideways_days = _safe_int(item.get("sideways_days"))
    return (
        0.0 <= range_position <= MAX_FOCUS_RANGE_POSITION
        and vol_ratio <= MAX_FOCUS_VOL_RATIO
        and sideways_days >= MIN_FOCUS_SIDEWAYS_DAYS
    )


def _is_risk_observation(item: Dict[str, Any]) -> bool:
    flags = _observation_flags(item)
    return any(flag in flags for flag in ("已突破", "位置过高", "跌破区间", "爆量过热", "量能过热"))


def _observation_flags(item: Dict[str, Any]) -> List[str]:
    flags: List[str] = []
    range_position = _safe_float(item.get("range_position"))
    vol_ratio = _safe_float(item.get("recent_vol_ratio_7d"))
    sideways_days = _safe_int(item.get("sideways_days"))
    data_quality = str(item.get("data_quality") or "").strip().lower()

    if range_position > 1.2:
        flags.append("位置过高")
    elif range_position > 1.0:
        flags.append("已突破")
    elif range_position < 0.0:
        flags.append("跌破区间")
    if vol_ratio > 8.0:
        flags.append("爆量过热")
    elif vol_ratio > MAX_FOCUS_VOL_RATIO:
        flags.append("量能过热")
    if 0 < sideways_days < MIN_FOCUS_SIDEWAYS_DAYS:
        flags.append("横盘不足")
    if data_quality == "estimated":
        flags.append("市值估算")
    return flags


def _append_focus_item(lines: List[str], idx: int, item: Dict[str, Any]) -> None:
    symbol = str(item.get("symbol") or "").upper()
    base = str(item.get("base_asset") or _base_from_symbol(symbol) or symbol).upper()
    symbol_label = _symbol_link(symbol=symbol, label=base)
    status_label = _status_label(_status(item))
    score = _safe_float(item.get("score"))
    sideways_days = _safe_int(item.get("sideways_days"))
    range_pct = _safe_float(item.get("range_pct"))
    range_position = _safe_float(item.get("range_position")) * 100.0
    vol_ratio = _safe_float(item.get("recent_vol_ratio_7d"))
    market_cap = _fmt_usd(item.get("market_cap"))
    flags = [flag for flag in _observation_flags(item) if flag == "市值估算"]
    suffix = f" | {'/'.join(flags)}" if flags else ""
    lines.extend(
        [
            f"🪙 {idx}. {symbol_label}  {status_label}  评分 <b>{score:.1f}</b>{_html(suffix)}",
            f"   📐 横盘：{sideways_days}天 | 区间：{range_pct:.1f}% | 位置：{range_position:.0f}%",
            f"   📊 量能：Vol {vol_ratio:.1f}x | 市值：{_html(market_cap)}",
        ]
    )


def _compact_item_line(idx: int, item: Dict[str, Any], *, include_flags: bool) -> str:
    symbol = str(item.get("symbol") or "").upper()
    base = str(item.get("base_asset") or _base_from_symbol(symbol) or symbol).upper()
    symbol_label = _symbol_link(symbol=symbol, label=base)
    status_label = _status_label(_status(item))
    score = _safe_float(item.get("score"))
    range_position = _safe_float(item.get("range_position")) * 100.0
    vol_ratio = _safe_float(item.get("recent_vol_ratio_7d"))
    text = (
        f"🪙 {idx}. {symbol_label} {status_label} | 评 {score:.1f} | "
        f"位 {range_position:.0f}% | Vol {vol_ratio:.1f}x"
    )
    if include_flags:
        flags = _observation_flags(item)
        if flags:
            text += f" | {'/'.join(flags)}"
    return text


def _build_failure_message(max_attempts: int, last_error: str) -> str:
    error_text = _html(str(last_error or "unknown")[:300])
    return "\n".join(
        [
            "<b>❌ 吸筹池日报失败</b>",
            f"🕒 时间：{_html(datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S UTC+8'))}",
            f"🔁 尝试：{max_attempts} 次",
            f"🧾 最后错误：{error_text}",
        ]
    )


async def _send_telegram(message: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        logger.warning("Telegram token/chat_id missing; message was not sent.")
        return False
    if aiohttp is None:
        logger.error("aiohttp is not installed; Telegram message was not sent.")
        return False

    max_attempts = max(1, _env_int("TELEGRAM_SEND_MAX_ATTEMPTS", 3))
    retry_delay = max(0.2, _env_float("TELEGRAM_SEND_RETRY_DELAY_SEC", 1.0))
    connect_timeout = max(3.0, _env_float("TELEGRAM_SEND_CONNECT_TIMEOUT_SEC", 15.0))
    read_timeout = max(3.0, _env_float("TELEGRAM_SEND_READ_TIMEOUT_SEC", 20.0))
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    timeout = aiohttp.ClientTimeout(total=connect_timeout + read_timeout + 5.0, connect=connect_timeout, sock_read=read_timeout)
    proxy = _proxy_url()

    for attempt in range(1, max_attempts + 1):
        try:
            async with aiohttp.ClientSession(timeout=timeout, trust_env=False) as session:
                async with session.post(url, json=payload, proxy=proxy) as resp:
                    text = await resp.text()
                    if resp.status == 200:
                        logger.info("Telegram summary sent successfully (attempt=%s).", attempt)
                        return True
                    logger.warning(
                        "Telegram API returned non-200 status (attempt=%s/%s status=%s body=%s).",
                        attempt,
                        max_attempts,
                        resp.status,
                        text[:300],
                    )
        except Exception as exc:
            logger.warning("Telegram direct send failed (attempt=%s/%s): %s", attempt, max_attempts, exc)

        if attempt < max_attempts:
            await asyncio.sleep(retry_delay * attempt)
    return False


def _proxy_url() -> Optional[str]:
    for name in (
        "ALERT_HTTPS_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "ALERT_HTTP_PROXY",
        "HTTP_PROXY",
        "http_proxy",
    ):
        value = os.getenv(name, "").strip()
        if value:
            return value
    return None


def _next_run_at(hour: int, minute: int) -> datetime:
    now = datetime.now(BEIJING_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def _parse_datetime(value: str) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _format_beijing_time(value: str) -> str:
    dt = _parse_datetime(value)
    if dt is None:
        return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S UTC+8")
    return dt.astimezone(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S UTC+8")


def _status(item: Any) -> str:
    if not isinstance(item, dict):
        return "unknown"
    return str(item.get("status") or "unknown").strip().lower()


def _status_label(status: str) -> str:
    key = str(status or "").strip().lower()
    return STATUS_LABELS.get(key, key or "unknown")


def _html(value: Any) -> str:
    return html.escape(str(value), quote=False)


def _html_attr(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _symbol_link(*, symbol: str, label: str) -> str:
    clean_symbol = str(symbol or "").strip().upper()
    clean_label = _html(str(label or clean_symbol or "UNKNOWN").strip().upper())
    if not clean_symbol:
        return f"<b>{clean_label}</b>"
    url = f"https://www.binance.com/futures/{_html_attr(clean_symbol)}"
    return f'<b><a href="{url}">{clean_label}</a></b>'


def _status_counts(items: Sequence[Any]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for item in items:
        status = _status(item)
        counts[status] = counts.get(status, 0) + 1
    return counts


def _base_from_symbol(symbol: str) -> str:
    text = str(symbol or "").upper()
    for suffix in ("USDT", "USDC", "BUSD", "USD"):
        if text.endswith(suffix):
            return text[: -len(suffix)]
    return text


def _safe_float(value: Any) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        if value is None:
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _fmt_usd(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric >= 1_000_000_000:
        return f"${numeric / 1_000_000_000:.2f}B"
    if numeric >= 1_000_000:
        return f"${numeric / 1_000_000:.1f}M"
    if numeric >= 1_000:
        return f"${numeric / 1_000:.0f}K"
    if numeric > 0:
        return f"${numeric:.0f}"
    return "N/A"


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
