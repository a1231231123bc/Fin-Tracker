from __future__ import annotations

from dataclasses import dataclass

# Top-level categories stay unchanged for current MVP reports.
CATEGORY_LABEL: dict[str, str] = {
    "food": "Еда",
    "transport": "Транспорт",
    "home": "Дом",
    "health": "Здоровье",
    "fun": "Развлечения",
    "shopping": "Покупки",
    "subscriptions": "Подписки",
    "other": "Другое",
}


@dataclass(frozen=True)
class SubcategoryDef:
    key: str
    label: str
    category: str
    keywords: tuple[str, ...]


SUBCATEGORIES: tuple[SubcategoryDef, ...] = (
    SubcategoryDef(
        key="food_out",
        label="Еда вне дома",
        category="food",
        keywords=(
            "кофе",
            "капучино",
            "латте",
            "чай",
            "обед",
            "ужин",
            "завтрак",
            "кафе",
            "ресторан",
            "столовая",
            "фастфуд",
            "пицца",
            "бургер",
            "суши",
            "шаурма",
        ),
    ),
    SubcategoryDef(
        key="food_groceries",
        label="Продукты",
        category="food",
        keywords=(
            "продукты",
            "магазин",
            "супермаркет",
            "гипермаркет",
            "еда домой",
            "овощи",
            "фрукты",
            "молоко",
            "хлеб",
            "мясо",
            "крупа",
        ),
    ),
    SubcategoryDef(
        key="transport_taxi",
        label="Такси",
        category="transport",
        keywords=("такси", "поездка", "трансфер"),
    ),
    SubcategoryDef(
        key="transport_public",
        label="Общественный транспорт",
        category="transport",
        keywords=("метро", "автобус", "трамвай", "электричка", "поезд", "проезд"),
    ),
    SubcategoryDef(
        key="transport_car",
        label="Авто расходы",
        category="transport",
        keywords=("бензин", "топливо", "заправка", "парковка", "шиномонтаж", "масло"),
    ),
    SubcategoryDef(
        key="home_rent",
        label="Жилье и аренда",
        category="home",
        keywords=("аренда", "ипотека", "квартира", "жилье"),
    ),
    SubcategoryDef(
        key="home_utilities",
        label="Коммунальные",
        category="home",
        keywords=("жкх", "коммуналка", "электричество", "вода", "газ", "интернет"),
    ),
    SubcategoryDef(
        key="health_medicine",
        label="Лекарства",
        category="health",
        keywords=("аптека", "лекарства", "таблетки", "витамины"),
    ),
    SubcategoryDef(
        key="health_doctors",
        label="Врачи и анализы",
        category="health",
        keywords=("врач", "клиника", "анализы", "стоматолог", "мрт", "узи"),
    ),
    SubcategoryDef(
        key="fun_events",
        label="Развлечения",
        category="fun",
        keywords=("кино", "театр", "концерт", "бар", "клуб", "игры"),
    ),
    SubcategoryDef(
        key="shopping_clothes",
        label="Одежда и обувь",
        category="shopping",
        keywords=("одежда", "обувь", "куртка", "джинсы", "кроссовки"),
    ),
    SubcategoryDef(
        key="shopping_other",
        label="Покупки прочее",
        category="shopping",
        keywords=("покупка", "товар", "заказ"),
    ),
    SubcategoryDef(
        key="subscriptions_digital",
        label="Цифровые подписки",
        category="subscriptions",
        keywords=("подписка", "премиум", "музыка", "видео", "облако"),
    ),
)

SUBCATEGORY_LABEL: dict[str, str] = {x.key: x.label for x in SUBCATEGORIES}
SUBCATEGORY_TO_CATEGORY: dict[str, str] = {x.key: x.category for x in SUBCATEGORIES}


def category_label(key: str) -> str:
    return CATEGORY_LABEL.get(key, key)


def subcategory_label(key: str) -> str:
    return SUBCATEGORY_LABEL.get(key, key)
