from __future__ import annotations

import os
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import aiosqlite

from .taxonomy import category_label, subcategory_label


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def _add_column_if_missing(self, conn: aiosqlite.Connection, table: str, col_def: str) -> None:
        col_name = col_def.split()[0]
        cur = await conn.execute(f"PRAGMA table_info({table})")
        rows = await cur.fetchall()
        existing = {row[1] for row in rows}
        if col_name not in existing:
            await conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")

    async def init(self) -> None:
        path = Path(self.db_path)
        if path.parent:
            os.makedirs(path.parent, exist_ok=True)

        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute("PRAGMA foreign_keys = ON")
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL DEFAULT '',
                    first_name TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS group_settings (
                    group_id INTEGER PRIMARY KEY,
                    title TEXT NOT NULL DEFAULT '',
                    currency TEXT NOT NULL DEFAULT 'RUB',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS expenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    category TEXT NOT NULL,
                    amount REAL NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    spent_at TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    source_message_id INTEGER NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(user_id)
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pending_expenses (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    source_message_id INTEGER NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )

            await self._add_column_if_missing(conn, "group_settings", "reminder_enabled INTEGER NOT NULL DEFAULT 1")
            await self._add_column_if_missing(conn, "group_settings", "reminder_time TEXT NOT NULL DEFAULT '21:00'")
            await self._add_column_if_missing(conn, "group_settings", "timezone TEXT NOT NULL DEFAULT 'Europe/Moscow'")
            await self._add_column_if_missing(conn, "group_settings", "last_reminder_date TEXT NOT NULL DEFAULT ''")

            await self._add_column_if_missing(conn, "expenses", "subcategory TEXT NOT NULL DEFAULT ''")
            await self._add_column_if_missing(conn, "expenses", "auto_category TEXT NOT NULL DEFAULT ''")
            await self._add_column_if_missing(conn, "expenses", "auto_subcategory TEXT NOT NULL DEFAULT ''")
            await self._add_column_if_missing(conn, "expenses", "auto_confidence REAL NOT NULL DEFAULT 0")
            await self._add_column_if_missing(conn, "expenses", "is_auto_applied INTEGER NOT NULL DEFAULT 0")
            await self._add_column_if_missing(conn, "expenses", "raw_comment_normalized TEXT NOT NULL DEFAULT ''")

            await self._add_column_if_missing(conn, "pending_expenses", "predicted_category TEXT NOT NULL DEFAULT ''")
            await self._add_column_if_missing(conn, "pending_expenses", "predicted_subcategory TEXT NOT NULL DEFAULT ''")
            await self._add_column_if_missing(conn, "pending_expenses", "predicted_confidence REAL NOT NULL DEFAULT 0")
            await self._add_column_if_missing(conn, "pending_expenses", "raw_comment_normalized TEXT NOT NULL DEFAULT ''")

            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS category_feedback (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    raw_comment_normalized TEXT NOT NULL,
                    predicted_subcategory TEXT NOT NULL,
                    chosen_subcategory TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS merchant_aliases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_id INTEGER NOT NULL,
                    normalized_pattern TEXT NOT NULL,
                    subcategory TEXT NOT NULL,
                    confidence REAL NOT NULL DEFAULT 0.9,
                    source TEXT NOT NULL DEFAULT 'feedback',
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    UNIQUE(group_id, normalized_pattern)
                )
                """
            )
            # Backward-compatible migrations for older table versions.
            await self._add_column_if_missing(
                conn,
                "category_feedback",
                "predicted_subcategory TEXT NOT NULL DEFAULT ''",
            )
            await self._add_column_if_missing(
                conn,
                "category_feedback",
                "chosen_subcategory TEXT NOT NULL DEFAULT ''",
            )
            await self._add_column_if_missing(
                conn,
                "merchant_aliases",
                "subcategory TEXT NOT NULL DEFAULT ''",
            )

            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_expenses_group_date ON expenses(group_id, spent_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_expenses_group_category_date ON expenses(group_id, category, spent_at)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_aliases_group_pattern ON merchant_aliases(group_id, normalized_pattern)"
            )
            # If table had legacy `category`, copy it into new `subcategory` only when empty.
            cur = await conn.execute("PRAGMA table_info(merchant_aliases)")
            cols = {row[1] for row in await cur.fetchall()}
            if "category" in cols and "subcategory" in cols:
                await conn.execute(
                    """
                    UPDATE merchant_aliases
                    SET subcategory = category
                    WHERE subcategory = '' AND category <> ''
                    """
                )
            await conn.commit()

    async def upsert_user(self, user_id: int, username: str, first_name: str) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                INSERT INTO users (user_id, username, first_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                  username=excluded.username,
                  first_name=excluded.first_name
                """,
                (user_id, username, first_name),
            )
            await conn.commit()

    async def ensure_group(self, group_id: int, title: str, default_currency: str, default_tz: str) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                INSERT INTO group_settings (group_id, title, currency, timezone)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(group_id) DO UPDATE SET
                  title=excluded.title,
                  updated_at=datetime('now')
                """,
                (group_id, title, default_currency, default_tz),
            )
            await conn.commit()

    async def setup_group(
        self,
        group_id: int,
        title: str,
        currency: str,
        tz_name: str | None = None,
    ) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            if tz_name:
                await conn.execute(
                    """
                    INSERT INTO group_settings (group_id, title, currency, timezone)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(group_id) DO UPDATE SET
                      title=excluded.title,
                      currency=excluded.currency,
                      timezone=excluded.timezone,
                      updated_at=datetime('now')
                    """,
                    (group_id, title, currency.upper(), tz_name),
                )
            else:
                await conn.execute(
                    """
                    INSERT INTO group_settings (group_id, title, currency)
                    VALUES (?, ?, ?)
                    ON CONFLICT(group_id) DO UPDATE SET
                      title=excluded.title,
                      currency=excluded.currency,
                      updated_at=datetime('now')
                    """,
                    (group_id, title, currency.upper()),
                )
            await conn.commit()

    async def get_group_currency(self, group_id: int, default_currency: str) -> str:
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                "SELECT currency FROM group_settings WHERE group_id = ?",
                (group_id,),
            )
            row = await cur.fetchone()
            return row[0] if row else default_currency

    async def get_group_settings(self, group_id: int) -> dict | None:
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                """
                SELECT group_id, title, currency, timezone, reminder_enabled, reminder_time, last_reminder_date
                FROM group_settings
                WHERE group_id = ?
                """,
                (group_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def set_reminder_enabled(self, group_id: int, enabled: bool) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                UPDATE group_settings
                SET reminder_enabled = ?, updated_at = datetime('now')
                WHERE group_id = ?
                """,
                (1 if enabled else 0, group_id),
            )
            await conn.commit()

    async def set_reminder_time(self, group_id: int, reminder_time: str) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                UPDATE group_settings
                SET reminder_time = ?, updated_at = datetime('now')
                WHERE group_id = ?
                """,
                (reminder_time, group_id),
            )
            await conn.commit()

    async def set_group_timezone(self, group_id: int, tz_name: str) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                UPDATE group_settings
                SET timezone = ?, updated_at = datetime('now')
                WHERE group_id = ?
                """,
                (tz_name, group_id),
            )
            await conn.commit()

    async def mark_group_reminded(self, group_id: int, local_date: str) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                UPDATE group_settings
                SET last_reminder_date = ?, updated_at = datetime('now')
                WHERE group_id = ?
                """,
                (local_date, group_id),
            )
            await conn.commit()

    async def _has_expenses_on_local_date(
        self,
        conn: aiosqlite.Connection,
        group_id: int,
        tz_name: str,
        local_day: date,
    ) -> bool:
        tz = ZoneInfo(tz_name)
        start_local = datetime.combine(local_day, time.min, tzinfo=tz)
        end_local = start_local + timedelta(days=1)
        start_utc = start_local.astimezone(timezone.utc).isoformat()
        end_utc = end_local.astimezone(timezone.utc).isoformat()

        cur = await conn.execute(
            """
            SELECT COUNT(*)
            FROM expenses
            WHERE group_id = ? AND spent_at >= ? AND spent_at < ?
            """,
            (group_id, start_utc, end_utc),
        )
        row = await cur.fetchone()
        return int(row[0] or 0) > 0

    async def list_groups_due_for_reminder(self, now_utc: datetime) -> list[tuple[int, str, str]]:
        due: list[tuple[int, str, str]] = []
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cur = await conn.execute(
                """
                SELECT group_id, title, timezone, reminder_time, last_reminder_date
                FROM group_settings
                WHERE reminder_enabled = 1
                """
            )
            rows = await cur.fetchall()

            for row in rows:
                group_id = int(row["group_id"])
                title = row["title"] or ""
                tz_name = row["timezone"] or "Europe/Moscow"
                reminder_time = row["reminder_time"] or "21:00"
                last_reminder_date = row["last_reminder_date"] or ""

                try:
                    tz = ZoneInfo(tz_name)
                    target_h, target_m = [int(x) for x in reminder_time.split(":", 1)]
                except Exception:
                    continue

                local_now = now_utc.astimezone(tz)
                local_date = local_now.date().isoformat()

                if last_reminder_date == local_date:
                    continue

                if (local_now.hour, local_now.minute) < (target_h, target_m):
                    continue

                has_expenses = await self._has_expenses_on_local_date(conn, group_id, tz_name, local_now.date())
                if has_expenses:
                    # Mark to avoid repeated checks for the rest of day.
                    await conn.execute(
                        "UPDATE group_settings SET last_reminder_date = ?, updated_at = datetime('now') WHERE group_id = ?",
                        (local_date, group_id),
                    )
                    continue

                due.append((group_id, title, local_date))

            await conn.commit()

        return due

    async def get_alias_subcategory(self, group_id: int, normalized_note: str) -> str | None:
        if not normalized_note:
            return None
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                SELECT subcategory
                FROM merchant_aliases
                WHERE group_id = ? AND normalized_pattern = ?
                LIMIT 1
                """,
                (group_id, normalized_note),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def create_pending_expense(
        self,
        group_id: int,
        user_id: int,
        amount: float,
        note: str,
        source_message_id: int,
        predicted_category: str = "",
        predicted_subcategory: str = "",
        predicted_confidence: float = 0.0,
        raw_comment_normalized: str = "",
    ) -> int:
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                INSERT INTO pending_expenses (
                    group_id, user_id, amount, note, source_message_id,
                    predicted_category, predicted_subcategory, predicted_confidence, raw_comment_normalized
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    user_id,
                    amount,
                    note,
                    source_message_id,
                    predicted_category,
                    predicted_subcategory,
                    predicted_confidence,
                    raw_comment_normalized,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def get_pending(
        self, pending_id: int
    ) -> tuple[int, int, float, str, int, str, str, float, str] | None:
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                SELECT
                  group_id, user_id, amount, note, source_message_id,
                  predicted_category, predicted_subcategory, predicted_confidence, raw_comment_normalized
                FROM pending_expenses
                WHERE id = ?
                """,
                (pending_id,),
            )
            row = await cur.fetchone()
            return tuple(row) if row else None

    async def finalize_pending(self, pending_id: int, category: str, subcategory: str = "") -> int | None:
        pending = await self.get_pending(pending_id)
        if pending is None:
            return None

        (
            group_id,
            user_id,
            amount,
            note,
            source_message_id,
            predicted_category,
            predicted_subcategory,
            predicted_confidence,
            raw_comment_normalized,
        ) = pending
        spent_at = datetime.now(timezone.utc).isoformat()

        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                INSERT INTO expenses (
                    group_id, user_id, category, subcategory, amount, note, spent_at, source_message_id,
                    auto_category, auto_subcategory, auto_confidence, is_auto_applied, raw_comment_normalized
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    user_id,
                    category,
                    subcategory,
                    amount,
                    note,
                    spent_at,
                    source_message_id,
                    predicted_category,
                    predicted_subcategory,
                    predicted_confidence,
                    0,
                    raw_comment_normalized,
                ),
            )
            await conn.execute("DELETE FROM pending_expenses WHERE id = ?", (pending_id,))
            await conn.commit()
            return int(cur.lastrowid)

    async def add_expense(
        self,
        group_id: int,
        user_id: int,
        category: str,
        subcategory: str,
        amount: float,
        note: str,
        source_message_id: int,
        auto_category: str = "",
        auto_subcategory: str = "",
        auto_confidence: float = 0.0,
        is_auto_applied: bool = False,
        raw_comment_normalized: str = "",
    ) -> int:
        spent_at = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                INSERT INTO expenses (
                    group_id, user_id, category, subcategory, amount, note, spent_at, source_message_id,
                    auto_category, auto_subcategory, auto_confidence, is_auto_applied, raw_comment_normalized
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    group_id,
                    user_id,
                    category,
                    subcategory,
                    amount,
                    note,
                    spent_at,
                    source_message_id,
                    auto_category,
                    auto_subcategory,
                    auto_confidence,
                    1 if is_auto_applied else 0,
                    raw_comment_normalized,
                ),
            )
            await conn.commit()
            return int(cur.lastrowid)

    async def record_feedback(
        self,
        group_id: int,
        raw_comment_normalized: str,
        predicted_subcategory: str,
        chosen_subcategory: str,
    ) -> None:
        if not raw_comment_normalized:
            return

        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                INSERT INTO category_feedback (group_id, raw_comment_normalized, predicted_subcategory, chosen_subcategory)
                VALUES (?, ?, ?, ?)
                """,
                (group_id, raw_comment_normalized, predicted_subcategory, chosen_subcategory),
            )

            cur = await conn.execute(
                """
                SELECT
                  COUNT(*) AS total,
                  SUM(CASE WHEN chosen_subcategory = ? THEN 1 ELSE 0 END) AS chosen_total
                FROM category_feedback
                WHERE group_id = ? AND raw_comment_normalized = ?
                """,
                (chosen_subcategory, group_id, raw_comment_normalized),
            )
            row = await cur.fetchone()
            total = int(row[0] or 0)
            chosen_total = int(row[1] or 0)

            if total >= 3 and chosen_total / total >= 0.8:
                await conn.execute(
                    """
                    INSERT INTO merchant_aliases (group_id, normalized_pattern, subcategory, confidence, source)
                    VALUES (?, ?, ?, ?, 'feedback')
                    ON CONFLICT(group_id, normalized_pattern) DO UPDATE SET
                      subcategory=excluded.subcategory,
                      confidence=excluded.confidence,
                      source='feedback'
                    """,
                    (group_id, raw_comment_normalized, chosen_subcategory, 0.97),
                )

            await conn.commit()

    async def today_summary(self, group_id: int) -> tuple[float, list[tuple[str, float]]]:
        start = datetime.now().strftime("%Y-%m-%d")
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE group_id = ? AND date(spent_at) = date(?)
                """,
                (group_id, start),
            )
            total_row = await cur.fetchone()

            cur = await conn.execute(
                """
                SELECT category, COALESCE(SUM(amount), 0) AS total
                FROM expenses
                WHERE group_id = ? AND date(spent_at) = date(?)
                GROUP BY category
                ORDER BY total DESC
                """,
                (group_id, start),
            )
            rows = await cur.fetchall()

        total = float(total_row[0] if total_row else 0)
        return total, [(row[0], float(row[1])) for row in rows]

    async def month_summary(self, group_id: int) -> tuple[float, list[tuple[str, float]]]:
        month = datetime.now().strftime("%Y-%m")
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                SELECT COALESCE(SUM(amount), 0)
                FROM expenses
                WHERE group_id = ? AND strftime('%Y-%m', spent_at) = ?
                """,
                (group_id, month),
            )
            total_row = await cur.fetchone()

            cur = await conn.execute(
                """
                SELECT category, COALESCE(SUM(amount), 0) AS total
                FROM expenses
                WHERE group_id = ? AND strftime('%Y-%m', spent_at) = ?
                GROUP BY category
                ORDER BY total DESC
                """,
                (group_id, month),
            )
            rows = await cur.fetchall()

        total = float(total_row[0] if total_row else 0)
        return total, [(row[0], float(row[1])) for row in rows]

    async def last_expenses(self, group_id: int, limit: int = 10) -> list[tuple[int, float, str, str, str, int]]:
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                SELECT id, amount, category, subcategory, note, user_id
                FROM expenses
                WHERE group_id = ?
                ORDER BY id DESC
                LIMIT ?
                """,
                (group_id, limit),
            )
            rows = await cur.fetchall()

        return [(int(row[0]), float(row[1]), row[2], row[3], row[4], int(row[5])) for row in rows]

    async def undo_last_by_user(self, group_id: int, user_id: int) -> int | None:
        async with aiosqlite.connect(self.db_path) as conn:
            cur = await conn.execute(
                """
                SELECT id
                FROM expenses
                WHERE group_id = ? AND user_id = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (group_id, user_id),
            )
            row = await cur.fetchone()
            if row is None:
                return None

            expense_id = int(row[0])
            await conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
            await conn.commit()
            return expense_id


def format_category_lines(rows: list[tuple[str, float]]) -> str:
    if not rows:
        return "Нет расходов"

    lines = []
    for category, total in rows:
        lines.append(f"- {category_label(category)}: {total:.2f}")
    return "\n".join(lines)


def format_last_line(
    expense_id: int,
    amount: float,
    category: str,
    subcategory: str,
    note: str,
    user_id: int,
    currency: str,
) -> str:
    note_part = f" | {note}" if note else ""
    sub = f"/{subcategory_label(subcategory)}" if subcategory else ""
    return f"#{expense_id} {amount:.2f} {currency} | {category_label(category)}{sub} | user:{user_id}{note_part}"
