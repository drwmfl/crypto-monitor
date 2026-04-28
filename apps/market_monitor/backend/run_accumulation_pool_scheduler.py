from __future__ import annotations

import argparse
import asyncio
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
    "dormant": "沉睡",
}


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
    config = load_alert_config(config_path=config_path or None).raw
    factor_settings = (
        config.get("alert_strategy", {})
        .get("confirmation_factors", {})
        .get("accumulation_pool", {})
    )
    if not isinstance(factor_settings, dict):
        factor_settings = {}
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


def _build_summary_message(payload: Dict[str, Any], pool_path: Path, limit: int) -> str:
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        symbols = {}

    items = [item for item in symbols.values() if _status(item) in READY_STATUSES]
    items.sort(key=lambda item: float(item.get("score") or 0.0), reverse=True)
    status_counts = _status_counts(symbols.values())

    generated_at = _format_beijing_time(str(payload.get("generated_at") or ""))
    lines = [
        "**收筹池日报 | 放量/升温 Top20**",
        f"时间：{generated_at}",
        (
            f"入池：{int(payload.get('count') or len(symbols))} | "
            f"放量 {status_counts.get('ready', 0)} | "
            f"升温 {status_counts.get('warming', 0)} | "
            f"沉睡 {status_counts.get('dormant', 0)}"
        ),
        f"文件：{pool_path.name}",
        "",
    ]

    if not items:
        lines.append("暂无放量/升温标的。")
        return "\n".join(lines)

    for idx, item in enumerate(items[:limit], start=1):
        symbol = str(item.get("symbol") or "").upper()
        base = str(item.get("base_asset") or _base_from_symbol(symbol) or symbol).upper()
        status = _status(item)
        status_label = _status_label(status)
        score = _safe_float(item.get("score"))
        sideways_days = _safe_int(item.get("sideways_days"))
        range_pct = _safe_float(item.get("range_pct"))
        vol_ratio = _safe_float(item.get("recent_vol_ratio_7d"))
        range_position = _safe_float(item.get("range_position")) * 100.0
        market_cap = _fmt_usd(item.get("market_cap"))
        lines.append(
            f"{idx}. **{base}** {status_label} {score:.1f} | "
            f"横盘{sideways_days}天 | 区间{range_pct:.1f}% | "
            f"Vol {vol_ratio:.1f}x | 位置{range_position:.0f}% | 市值{market_cap}"
        )
    return "\n".join(lines)


def _build_failure_message(max_attempts: int, last_error: str) -> str:
    error_text = str(last_error or "unknown")[:300]
    return "\n".join(
        [
            "**收筹池日报失败**",
            f"时间：{datetime.now(BEIJING_TZ).strftime('%Y-%m-%d %H:%M:%S UTC+8')}",
            f"尝试：{max_attempts} 次",
            f"最后错误：{error_text}",
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
