from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bot_token: str
    db_path: str
    default_currency: str
    auto_category_threshold: float
    default_timezone: str
    reminder_check_interval_sec: int
    webapp_url: str
    tg_app_url_template: str



def load_settings() -> Settings:
    token = os.getenv("BOT_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BOT_TOKEN is not set")

    db_path = os.getenv("DB_PATH", "data/fin_tracker.db").strip() or "data/fin_tracker.db"
    default_currency = os.getenv("DEFAULT_CURRENCY", "RUB").strip().upper() or "RUB"

    raw_threshold = os.getenv("AUTO_CATEGORY_THRESHOLD", "0.82").strip()
    try:
        threshold = float(raw_threshold)
    except ValueError:
        threshold = 0.82
    threshold = max(0.0, min(1.0, threshold))

    default_timezone = os.getenv("DEFAULT_TIMEZONE", "Europe/Moscow").strip() or "Europe/Moscow"
    raw_interval = os.getenv("REMINDER_CHECK_INTERVAL_SEC", "60").strip()
    try:
        reminder_interval = int(raw_interval)
    except ValueError:
        reminder_interval = 60
    reminder_interval = max(15, min(3600, reminder_interval))
    webapp_url = os.getenv("WEBAPP_URL", "http://localhost:8089").strip() or "http://localhost:8089"
    tg_app_url_template = os.getenv("TG_APP_URL_TEMPLATE", "").strip()

    return Settings(
        bot_token=token,
        db_path=db_path,
        default_currency=default_currency,
        auto_category_threshold=threshold,
        default_timezone=default_timezone,
        reminder_check_interval_sec=reminder_interval,
        webapp_url=webapp_url,
        tg_app_url_template=tg_app_url_template,
    )
