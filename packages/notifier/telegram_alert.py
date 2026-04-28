from __future__ import annotations

import asyncio
import logging
import os
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)
_APP_CACHE: Dict[str, object] = {}
_APP_INIT_LOCK: Optional[asyncio.Lock] = None


def _escape_markdown_v2(text: str) -> str:
    """Escape Telegram MarkdownV2 special characters."""
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!\\])", r"\\\1", text)


def _to_markdown_v2(message: str) -> str:
    """
    Convert plain text / **bold** message into safe MarkdownV2 text.
    - Normal text will be escaped.
    - **bold** segments are converted to MarkdownV2 *bold*.
    """
    if not message:
        return ""

    bold_pattern = re.compile(r"\*\*(.+?)\*\*")
    parts = []
    last_end = 0

    for match in bold_pattern.finditer(message):
        normal_text = message[last_end:match.start()]
        bold_text = match.group(1)

        if normal_text:
            parts.append(_escape_markdown_v2(normal_text))
        parts.append(f"*{_escape_markdown_v2(bold_text)}*")

        last_end = match.end()

    tail = message[last_end:]
    if tail:
        parts.append(_escape_markdown_v2(tail))

    return "".join(parts)


async def send_telegram_alert(token: str, chat_id: str, message: str) -> bool:
    """
    Send a Telegram alert message asynchronously.
    Returns True when sent successfully, otherwise False.
    """
    token = (token or "").strip()
    chat_id = (chat_id or "").strip()

    if not token or not chat_id:
        logger.error("Telegram token/chat_id is missing.")
        return False

    try:
        from telegram.error import TelegramError
        from telegram.ext import Application
    except ModuleNotFoundError:
        logger.error("python-telegram-bot is not installed in current runtime.")
        return False

    from telegram.error import TimedOut

    max_attempts = max(1, _safe_int(os.getenv("TELEGRAM_SEND_MAX_ATTEMPTS"), 3))
    base_retry_delay = max(0.1, _safe_float(os.getenv("TELEGRAM_SEND_RETRY_DELAY_SEC"), 1.0))
    connect_timeout = max(3.0, _safe_float(os.getenv("TELEGRAM_SEND_CONNECT_TIMEOUT_SEC"), 15.0))
    read_timeout = max(3.0, _safe_float(os.getenv("TELEGRAM_SEND_READ_TIMEOUT_SEC"), 20.0))
    write_timeout = max(3.0, _safe_float(os.getenv("TELEGRAM_SEND_WRITE_TIMEOUT_SEC"), 20.0))
    pool_timeout = max(3.0, _safe_float(os.getenv("TELEGRAM_SEND_POOL_TIMEOUT_SEC"), 15.0))
    payload = _to_markdown_v2(message)

    for attempt in range(1, max_attempts + 1):
        try:
            app = await _get_or_create_application(token, Application)
            await app.bot.send_message(
                chat_id=chat_id,
                text=payload,
                parse_mode="MarkdownV2",
                disable_web_page_preview=True,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
                pool_timeout=pool_timeout,
            )
            logger.info("Telegram alert sent successfully (attempt=%s).", attempt)
            return True
        except TimedOut as exc:
            logger.warning("Telegram timeout (attempt=%s/%s): %s", attempt, max_attempts, exc)
            await _reset_application(token)
        except TelegramError as exc:
            logger.exception("Telegram API error while sending alert (attempt=%s/%s): %s", attempt, max_attempts, exc)
            await _reset_application(token)
        except Exception as exc:
            logger.exception("Unexpected error while sending Telegram alert (attempt=%s/%s): %s", attempt, max_attempts, exc)
            await _reset_application(token)

        if attempt < max_attempts:
            await asyncio.sleep(base_retry_delay * attempt)

    return False


async def _get_or_create_application(token: str, application_cls):
    global _APP_INIT_LOCK
    app = _APP_CACHE.get(token)
    if app is not None:
        return app

    if _APP_INIT_LOCK is None:
        _APP_INIT_LOCK = asyncio.Lock()

    async with _APP_INIT_LOCK:
        app = _APP_CACHE.get(token)
        if app is not None:
            return app

        app = application_cls.builder().token(token).build()
        await app.initialize()
        _APP_CACHE[token] = app
        return app


async def _reset_application(token: str) -> None:
    app = _APP_CACHE.pop(token, None)
    if app is None:
        return

    try:
        await app.shutdown()
    except Exception:
        logger.exception("Failed to shutdown Telegram application cleanly.")


def _safe_int(value: Optional[str], default: int) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _safe_float(value: Optional[str], default: float) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default
