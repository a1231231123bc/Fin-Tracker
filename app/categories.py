from __future__ import annotations

CATEGORIES: list[tuple[str, str]] = [
    ("food", "Еда"),
    ("transport", "Транспорт"),
    ("home", "Дом"),
    ("health", "Здоровье"),
    ("fun", "Развлечения"),
    ("shopping", "Покупки"),
    ("subscriptions", "Подписки"),
    ("other", "Другое"),
]

CATEGORY_LABEL = dict(CATEGORIES)

CATEGORY_ALIASES: dict[str, str] = {
    "еда": "food",
    "food": "food",
    "transport": "transport",
    "транспорт": "transport",
    "дом": "home",
    "home": "home",
    "здоровье": "health",
    "health": "health",
    "развлечения": "fun",
    "fun": "fun",
    "покупки": "shopping",
    "shopping": "shopping",
    "подписки": "subscriptions",
    "subscriptions": "subscriptions",
    "другое": "other",
    "other": "other",
}


def parse_category(raw: str | None) -> str | None:
    if not raw:
        return None
    return CATEGORY_ALIASES.get(raw.strip().lower())
