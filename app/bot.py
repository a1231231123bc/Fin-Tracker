from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message, WebAppInfo
from dotenv import load_dotenv

from .categories import CATEGORIES, parse_category
from .classifier import normalize_text, predict_category
from .config import load_settings
from .db import Database, format_category_lines, format_last_line
from .taxonomy import SUBCATEGORY_TO_CATEGORY, category_label, subcategory_label

AMOUNT_RE = re.compile(r"^\d+(?:[\.,]\d{1,2})?$")
TIME_RE = re.compile(r"^(?:[01]\d|2[0-3]):[0-5]\d$")
BASE_CATEGORIES = {"food", "transport", "home", "health", "fun", "shopping", "subscriptions", "other"}
logger = logging.getLogger(__name__)


def parse_amount(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = raw.replace(",", ".").strip()
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None

    if value <= 0:
        return None
    return float(value)


def parse_amount_token(token: str) -> float | None:
    cleaned = token.strip().lower()
    cleaned = cleaned.replace("₽", "").replace("rub", "").replace("rur", "")
    cleaned = cleaned.replace("$", "").replace("usd", "")
    cleaned = cleaned.replace("€", "").replace("eur", "")
    cleaned = cleaned.strip(".,:;!?()[]{}")
    if not cleaned or not AMOUNT_RE.match(cleaned):
        return None
    return parse_amount(cleaned)


def extract_expense_parts(text: str) -> tuple[float, str | None, str] | None:
    tokens = text.split()
    if not tokens:
        return None

    amount_idx = None
    amount = None
    for idx, token in enumerate(tokens):
        candidate = parse_amount_token(token)
        if candidate is not None:
            amount_idx = idx
            amount = candidate
            break

    if amount is None or amount_idx is None:
        return None

    rest_tokens = tokens[:amount_idx] + tokens[amount_idx + 1 :]
    if not rest_tokens:
        return amount, None, ""

    quick_category = parse_category(rest_tokens[0])
    if quick_category:
        note = " ".join(rest_tokens[1:]).strip()
        return amount, quick_category, note

    note = " ".join(rest_tokens).strip()
    return amount, None, note


def category_keyboard(pending_id: int) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=label, callback_data=f"cat:{pending_id}:{key}")]
        for key, label in CATEGORIES
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def ensure_group_chat(message: Message) -> bool:
    return message.chat.type in {"group", "supergroup"}


def parse_timezone(raw: str) -> str | None:
    try:
        ZoneInfo(raw)
    except Exception:
        return None
    return raw


def build_group_app_url(settings, group_id: int) -> str:
    if settings.tg_app_url_template:
        return settings.tg_app_url_template.replace("{group_id}", str(group_id))
    return f"{settings.webapp_url}?group_id={group_id}"


def settings_keyboard(group_id: int, enabled: bool) -> InlineKeyboardMarkup:
    status_label = "Отключить" if enabled else "Включить"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🔔 {status_label} напоминания", callback_data=f"cfg:t:{group_id}")],
            [
                InlineKeyboardButton(text="🕘 09:00", callback_data=f"cfg:h:{group_id}:09-00"),
                InlineKeyboardButton(text="🕛 12:00", callback_data=f"cfg:h:{group_id}:12-00"),
                InlineKeyboardButton(text="🌙 21:00", callback_data=f"cfg:h:{group_id}:21-00"),
            ],
        ]
    )


def webapp_keyboard(url: str, use_webapp_button: bool) -> InlineKeyboardMarkup:
    if use_webapp_button:
        button = InlineKeyboardButton(text="Open App", web_app=WebAppInfo(url=url))
    else:
        button = InlineKeyboardButton(text="Open App", url=url)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [button]
        ]
    )


async def main() -> None:
    load_dotenv()
    logging.basicConfig(level=logging.INFO)
    settings = load_settings()
    db = Database(settings.db_path)
    await db.init()

    bot = Bot(settings.bot_token)
    dp = Dispatcher()

    async def process_expense_text(message: Message, raw_text: str, reply_on_error: bool) -> bool:
        parsed = extract_expense_parts(raw_text)
        if parsed is None:
            if reply_on_error:
                await message.answer("Не понял расход. Пример: `450 кофе`", parse_mode="Markdown")
            return False

        amount, quick_category, note = parsed

        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username or "",
            first_name=message.from_user.first_name or "",
        )
        await db.ensure_group(
            message.chat.id,
            message.chat.title or "",
            settings.default_currency,
            settings.default_timezone,
        )

        currency = await db.get_group_currency(message.chat.id, settings.default_currency)
        normalized_note = normalize_text(note)

        if quick_category:
            expense_id = await db.add_expense(
                group_id=message.chat.id,
                user_id=message.from_user.id,
                category=quick_category,
                subcategory="",
                amount=amount,
                note=note,
                source_message_id=message.message_id,
                auto_category=quick_category,
                auto_subcategory="",
                auto_confidence=1.0,
                is_auto_applied=False,
                raw_comment_normalized=normalized_note,
            )
            await message.answer(
                f"Добавлено #{expense_id}: {amount:.2f} {currency} -> {category_label(quick_category)}"
            )
            return True

        prediction = None
        if note:
            alias_subcategory = await db.get_alias_subcategory(message.chat.id, normalized_note)
            prediction = predict_category(note, alias_subcategory=alias_subcategory)

        if prediction and prediction.confidence >= settings.auto_category_threshold:
            expense_id = await db.add_expense(
                group_id=message.chat.id,
                user_id=message.from_user.id,
                category=prediction.category,
                subcategory=prediction.subcategory,
                amount=amount,
                note=note,
                source_message_id=message.message_id,
                auto_category=prediction.category,
                auto_subcategory=prediction.subcategory,
                auto_confidence=prediction.confidence,
                is_auto_applied=True,
                raw_comment_normalized=normalized_note,
            )
            logger.info(
                "auto-category applied: group=%s user=%s expense=%s category=%s subcategory=%s confidence=%.2f note=%r",
                message.chat.id,
                message.from_user.id,
                expense_id,
                prediction.category,
                prediction.subcategory,
                prediction.confidence,
                note,
            )
            await message.answer(
                f"Добавлено #{expense_id}: {amount:.2f} {currency} -> "
                f"{subcategory_label(prediction.subcategory)}"
            )
            return True

        # Even with low confidence or no prediction, always save the amount.
        fallback_category = prediction.category if prediction else "other"
        fallback_subcategory = prediction.subcategory if prediction else ""
        expense_id = await db.add_expense(
            group_id=message.chat.id,
            user_id=message.from_user.id,
            category=fallback_category,
            subcategory=fallback_subcategory,
            amount=amount,
            note=note,
            source_message_id=message.message_id,
            auto_category=prediction.category if prediction else "",
            auto_subcategory=prediction.subcategory if prediction else "",
            auto_confidence=prediction.confidence if prediction else 0.0,
            is_auto_applied=True,
            raw_comment_normalized=normalized_note,
        )

        if prediction:
            logger.info(
                "low-confidence category fallback: group=%s user=%s expense=%s category=%s subcategory=%s confidence=%.2f note=%r",
                message.chat.id,
                message.from_user.id,
                expense_id,
                fallback_category,
                fallback_subcategory,
                prediction.confidence,
                note,
            )
            await message.answer(
                f"Добавлено #{expense_id}: {amount:.2f} {currency} -> "
                f"{subcategory_label(fallback_subcategory)}"
            )
        else:
            logger.info(
                "no-category fallback to other: group=%s user=%s expense=%s amount=%.2f note=%r",
                message.chat.id,
                message.from_user.id,
                expense_id,
                amount,
                note,
            )
            await message.answer(
                f"Добавлено #{expense_id}: {amount:.2f} {currency} -> {category_label('other')}"
            )
        return True

    async def render_settings(chat_id: int) -> tuple[str, InlineKeyboardMarkup]:
        group = await db.get_group_settings(chat_id)
        if group is None:
            await db.ensure_group(chat_id, "", settings.default_currency, settings.default_timezone)
            group = await db.get_group_settings(chat_id)
            assert group is not None

        enabled = bool(group["reminder_enabled"])
        status = "включены" if enabled else "выключены"
        text = (
            "⚙️ Настройки группы\n"
            f"- Валюта: {group['currency']}\n"
            f"- Timezone: {group['timezone']}\n"
            f"- Напоминания: {status}\n"
            f"- Время напоминаний: {group['reminder_time']}\n\n"
            "Команды:\n"
            "- `/remind HH:MM`\n"
            "- `/tz Europe/Moscow`"
        )
        return text, settings_keyboard(chat_id, enabled)

    async def reminder_worker() -> None:
        while True:
            now_utc = datetime.now(timezone.utc)
            due_groups = await db.list_groups_due_for_reminder(now_utc)
            for group_id, _title, local_date in due_groups:
                try:
                    await bot.send_message(
                        chat_id=group_id,
                        text=(
                            "📝 Кажется, вы еще не записывали расходы сегодня. Не забудьте сделать это.\n\n"
                            "Отключить напоминания или изменить время можно в /settings"
                        ),
                    )
                    await db.mark_group_reminded(group_id, local_date)
                except Exception:
                    # Skip failures to avoid blocking all groups.
                    continue

            await asyncio.sleep(settings.reminder_check_interval_sec)

    @dp.message(Command("start"))
    async def cmd_start(message: Message) -> None:
        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username or "",
            first_name=message.from_user.first_name or "",
        )

        if ensure_group_chat(message):
            await db.ensure_group(
                message.chat.id,
                message.chat.title or "",
                settings.default_currency,
                settings.default_timezone,
            )
            app_url = build_group_app_url(settings, message.chat.id)
            await message.answer(
                "Бот активен в группе. Просто пиши расход текстом: `450 кофе`.\n"
                "Команды статистики: /today /month /last /undo\n"
                "Настройки: /settings\n"
                "Открыть приложение: /app",
                parse_mode="Markdown",
            )
            await message.answer("Открыть TG App:", reply_markup=webapp_keyboard(app_url, use_webapp_button=False))
            return

        await message.answer(
            "Привет. Добавь меня в группу и пиши расходы обычным текстом: `450 кофе`.\n"
            "Команды: /today, /month, /last, /undo, /settings, /app",
            parse_mode="Markdown",
        )

    @dp.message(Command("app"))
    async def cmd_app(message: Message) -> None:
        if ensure_group_chat(message):
            await db.ensure_group(
                message.chat.id,
                message.chat.title or "",
                settings.default_currency,
                settings.default_timezone,
            )
            app_url = build_group_app_url(settings, message.chat.id)
            await message.answer("Открыть TG App:", reply_markup=webapp_keyboard(app_url, use_webapp_button=False))
            return

        app_url = settings.webapp_url
        await message.answer("Открыть TG App:", reply_markup=webapp_keyboard(app_url, use_webapp_button=True))

    @dp.message(Command("setup"))
    async def cmd_setup(message: Message, command: CommandObject) -> None:
        if not ensure_group_chat(message):
            await message.answer("/setup работает только в группе")
            return

        currency = settings.default_currency
        tz_name: str | None = None

        if command.args:
            args = command.args.split()
            if args:
                c = args[0].strip().upper()
                if 2 <= len(c) <= 6 and c.isalpha():
                    currency = c
            if len(args) > 1:
                tz_name = parse_timezone(args[1].strip())

        await db.upsert_user(
            user_id=message.from_user.id,
            username=message.from_user.username or "",
            first_name=message.from_user.first_name or "",
        )
        await db.setup_group(message.chat.id, message.chat.title or "", currency, tz_name=tz_name)
        response = f"Группа настроена. Валюта: {currency}"
        if tz_name:
            response += f"\nTimezone: {tz_name}"
        await message.answer(response)

    @dp.message(Command("settings"))
    async def cmd_settings(message: Message) -> None:
        if not ensure_group_chat(message):
            await message.answer("/settings работает только в группе")
            return
        await db.ensure_group(
            message.chat.id,
            message.chat.title or "",
            settings.default_currency,
            settings.default_timezone,
        )
        text, keyboard = await render_settings(message.chat.id)
        await message.answer(text, parse_mode="Markdown", reply_markup=keyboard)

    @dp.message(Command("remind"))
    async def cmd_remind(message: Message, command: CommandObject) -> None:
        if not ensure_group_chat(message):
            await message.answer("/remind работает только в группе")
            return
        if not command.args or not TIME_RE.match(command.args.strip()):
            await message.answer("Формат: /remind HH:MM")
            return

        reminder_time = command.args.strip()
        await db.set_reminder_time(message.chat.id, reminder_time)
        await message.answer(f"Время напоминаний обновлено: {reminder_time}")

    @dp.message(Command("tz"))
    async def cmd_tz(message: Message, command: CommandObject) -> None:
        if not ensure_group_chat(message):
            await message.answer("/tz работает только в группе")
            return
        if not command.args:
            await message.answer("Формат: /tz Europe/Moscow")
            return

        tz_name = parse_timezone(command.args.strip())
        if tz_name is None:
            await message.answer("Некорректный timezone. Пример: Europe/Moscow")
            return

        await db.set_group_timezone(message.chat.id, tz_name)
        await message.answer(f"Timezone обновлен: {tz_name}")

    @dp.callback_query(F.data.startswith("cfg:"))
    async def on_settings_callback(query: CallbackQuery) -> None:
        data = (query.data or "").split(":")
        if len(data) < 3:
            await query.answer("Некорректная команда", show_alert=True)
            return

        action = data[1]
        target_group_id = int(data[2])
        if query.message.chat.id != target_group_id:
            await query.answer("Эти настройки для другого чата", show_alert=True)
            return

        if action == "t":
            group = await db.get_group_settings(target_group_id)
            enabled = bool(group["reminder_enabled"]) if group else True
            await db.set_reminder_enabled(target_group_id, not enabled)
            await query.answer("Настройки обновлены")
        elif action == "h" and len(data) == 4:
            hhmm = data[3].replace("-", ":")
            if TIME_RE.match(hhmm):
                await db.set_reminder_time(target_group_id, hhmm)
                await query.answer(f"Время: {hhmm}")
            else:
                await query.answer("Некорректное время", show_alert=True)
                return
        else:
            await query.answer("Неизвестное действие", show_alert=True)
            return

        text, keyboard = await render_settings(target_group_id)
        await query.message.edit_text(text, parse_mode="Markdown", reply_markup=keyboard)

    @dp.message(Command("add"))
    async def cmd_add(message: Message, command: CommandObject) -> None:
        if not ensure_group_chat(message):
            await message.answer("/add работает только в группе")
            return

        if not command.args:
            await message.answer("Формат: /add <сумма> [заметка или категория]")
            return

        await process_expense_text(message, command.args, reply_on_error=True)

    @dp.message(F.text)
    async def on_plain_text(message: Message) -> None:
        if not ensure_group_chat(message):
            return
        if not message.from_user or message.from_user.is_bot:
            return

        text = (message.text or "").strip()
        if not text:
            return

        if text.startswith("/"):
            head = text[1:].split(maxsplit=1)[0].lower()
            cmd = head.split("@", 1)[0]

            if cmd == "today":
                currency = await db.get_group_currency(message.chat.id, settings.default_currency)
                total, rows = await db.today_summary(message.chat.id)
                await message.answer(f"Сегодня: {total:.2f} {currency}\n\n{format_category_lines(rows)}")
                return

            if cmd == "month":
                currency = await db.get_group_currency(message.chat.id, settings.default_currency)
                total, rows = await db.month_summary(message.chat.id)
                await message.answer(f"За месяц: {total:.2f} {currency}\n\n{format_category_lines(rows)}")
                return

            if cmd == "last":
                currency = await db.get_group_currency(message.chat.id, settings.default_currency)
                rows = await db.last_expenses(message.chat.id, limit=10)
                if not rows:
                    await message.answer("Расходов пока нет")
                else:
                    lines = [
                        format_last_line(expense_id, amount, category, subcategory, note, user_id, currency)
                        for expense_id, amount, category, subcategory, note, user_id in rows
                    ]
                    await message.answer("Последние 10 расходов:\n" + "\n".join(lines))
                return

            if cmd == "undo":
                removed_id = await db.undo_last_by_user(message.chat.id, message.from_user.id)
                if removed_id is None:
                    await message.answer("У тебя нет расходов для отмены")
                else:
                    await message.answer(f"Удален последний расход: #{removed_id}")
                return

            # Unknown slash-command: ignore in plain-text fallback.
            return

        await process_expense_text(message, text, reply_on_error=False)

    @dp.callback_query(F.data.startswith("cat:"))
    async def on_category_chosen(query: CallbackQuery) -> None:
        parts = (query.data or "").split(":")
        if len(parts) != 3:
            await query.answer("Некорректная кнопка", show_alert=True)
            return

        pending_id = int(parts[1])
        category = parts[2]
        if category not in BASE_CATEGORIES:
            await query.answer("Неизвестная категория", show_alert=True)
            return

        pending = await db.get_pending(pending_id)
        if pending is None:
            # Graceful handling for stale inline buttons.
            try:
                await query.message.edit_reply_markup(reply_markup=None)
            except Exception:
                pass
            await query.answer("Запись уже обработана", show_alert=True)
            return

        (
            group_id,
            user_id,
            amount,
            note,
            _msg_id,
            _predicted_category,
            predicted_subcategory,
            predicted_confidence,
            raw_comment_normalized,
        ) = pending

        if query.from_user.id != user_id:
            await query.answer("Только автор может выбрать категорию", show_alert=True)
            return

        chosen_subcategory = ""
        if predicted_subcategory and SUBCATEGORY_TO_CATEGORY.get(predicted_subcategory) == category:
            chosen_subcategory = predicted_subcategory

        expense_id = await db.finalize_pending(pending_id, category, chosen_subcategory)
        if expense_id is None:
            await query.answer("Не удалось сохранить", show_alert=True)
            return

        if predicted_subcategory and chosen_subcategory:
            await db.record_feedback(
                group_id=group_id,
                raw_comment_normalized=raw_comment_normalized,
                predicted_subcategory=predicted_subcategory,
                chosen_subcategory=chosen_subcategory,
            )

        currency = await db.get_group_currency(group_id, settings.default_currency)
        chosen_label = subcategory_label(chosen_subcategory) if chosen_subcategory else category_label(category)
        text = f"Добавлено #{expense_id}: {amount:.2f} {currency} -> {chosen_label}"
        if predicted_subcategory:
            logger.info(
                "manual category chosen: group=%s user=%s expense=%s chosen=%s predicted=%s confidence=%.2f",
                group_id,
                user_id,
                expense_id,
                chosen_label,
                subcategory_label(predicted_subcategory),
                predicted_confidence,
            )
        if note:
            text += f"\nЗаметка: {note}"
        # Always remove keyboard first so user sees completion even if edit_text fails in some clients.
        try:
            await query.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        try:
            await query.message.edit_text(text)
        except Exception:
            await query.message.answer(text)
        await query.answer("Сохранено")

    @dp.message(Command("today"))
    async def cmd_today(message: Message) -> None:
        if not ensure_group_chat(message):
            await message.answer("/today работает только в группе")
            return

        currency = await db.get_group_currency(message.chat.id, settings.default_currency)
        total, rows = await db.today_summary(message.chat.id)
        await message.answer(
            f"Сегодня: {total:.2f} {currency}\n\n{format_category_lines(rows)}"
        )

    @dp.message(Command("month"))
    async def cmd_month(message: Message) -> None:
        if not ensure_group_chat(message):
            await message.answer("/month работает только в группе")
            return

        currency = await db.get_group_currency(message.chat.id, settings.default_currency)
        total, rows = await db.month_summary(message.chat.id)
        await message.answer(
            f"За месяц: {total:.2f} {currency}\n\n{format_category_lines(rows)}"
        )

    @dp.message(Command("last"))
    async def cmd_last(message: Message) -> None:
        if not ensure_group_chat(message):
            await message.answer("/last работает только в группе")
            return

        currency = await db.get_group_currency(message.chat.id, settings.default_currency)
        rows = await db.last_expenses(message.chat.id, limit=10)
        if not rows:
            await message.answer("Расходов пока нет")
            return

        lines = [
            format_last_line(expense_id, amount, category, subcategory, note, user_id, currency)
            for expense_id, amount, category, subcategory, note, user_id in rows
        ]
        await message.answer("Последние 10 расходов:\n" + "\n".join(lines))

    @dp.message(Command("undo"))
    async def cmd_undo(message: Message) -> None:
        if not ensure_group_chat(message):
            await message.answer("/undo работает только в группе")
            return

        removed_id = await db.undo_last_by_user(message.chat.id, message.from_user.id)
        if removed_id is None:
            await message.answer("У тебя нет расходов для отмены")
            return

        await message.answer(f"Удален последний расход: #{removed_id}")

    reminder_task = asyncio.create_task(reminder_worker())
    try:
        await dp.start_polling(bot)
    finally:
        reminder_task.cancel()
        try:
            await reminder_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    asyncio.run(main())
