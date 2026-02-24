from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

from .config import load_settings
from .db import Database
from .taxonomy import category_label, subcategory_label

app = FastAPI(title="FinTracker WebApp", version="1.0.0")
settings = load_settings()
db = Database(settings.db_path)


@app.on_event("startup")
async def startup() -> None:
    await db.init()


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(Path(__file__).parent / "static" / "fintracker.html")


@app.get("/api/dashboard")
async def api_dashboard(group_id: int = Query(...)) -> dict:
    group = await db.get_group_settings(group_id)
    if group is None:
        raise HTTPException(status_code=404, detail="Group not found")

    currency = group.get("currency") or settings.default_currency
    today_total, today_rows = await db.today_summary(group_id)
    month_total, month_rows = await db.month_summary(group_id)
    last_rows = await db.last_expenses(group_id, limit=20)

    today_categories = [
        {"key": k, "label": category_label(k), "amount": v} for k, v in today_rows
    ]
    month_categories = [
        {"key": k, "label": category_label(k), "amount": v} for k, v in month_rows
    ]

    transactions = []
    for expense_id, amount, category, subcategory, note, user_id in last_rows:
        transactions.append(
            {
                "id": expense_id,
                "amount": amount,
                "category": category,
                "category_label": category_label(category),
                "subcategory": subcategory,
                "subcategory_label": subcategory_label(subcategory) if subcategory else "",
                "note": note,
                "user_id": user_id,
            }
        )

    return {
        "group": {
            "id": group_id,
            "title": group.get("title") or "FinTracker",
            "currency": currency,
            "timezone": group.get("timezone") or settings.default_timezone,
        },
        "totals": {
            "today": today_total,
            "month": month_total,
        },
        "today_categories": today_categories,
        "month_categories": month_categories,
        "transactions": transactions,
    }
