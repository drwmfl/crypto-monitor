"""Shared notifier adapters."""

from .telegram_alert import send_telegram_alert

__all__ = ["send_telegram_alert"]
