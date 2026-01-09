import asyncio
import json
import logging
import os
from html import escape
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.exceptions import TelegramNetworkError
from aiohttp.client_exceptions import ClientConnectorError, ClientOSError, ClientConnectorSSLError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("tg_scheduler")


WEEKDAY_ALIASES: Dict[str, str] = {
    "mon": "mon",
    "monday": "mon",
    "пн": "mon",
    "пон": "mon",
    "tue": "tue",
    "tuesday": "tue",
    "вт": "tue",
    "ср": "wed",
    "wed": "wed",
    "wednesday": "wed",
    "чт": "thu",
    "thu": "thu",
    "thursday": "thu",
    "пт": "fri",
    "fri": "fri",
    "friday": "fri",
    "сб": "sat",
    "sat": "sat",
    "saturday": "sat",
    "вс": "sun",
    "sun": "sun",
    "sunday": "sun",
}


MEDIA_SENDERS = {
    "photo": "send_photo",
    "video": "send_video",
    "document": "send_document",
}


class TaskCreationStates(StatesGroup):
    """Состояния для создания задачи через интерактивную клавиатуру."""
    waiting_for_chat = State()
    waiting_for_chat_id_manual = State()  # Состояние для ввода chat_id вручную
    waiting_for_time = State()
    waiting_for_message = State()
    waiting_for_weekdays = State()
    waiting_for_monthday = State()
    waiting_for_media_type = State()
    waiting_for_media_url = State()
    confirming = State()


class DeleteTaskStates(StatesGroup):
    """Состояния для удаления задачи."""
    waiting_for_task_number = State()


class ChatStorage:
    """Хранилище известных чатов/групп."""
    def __init__(self, file_path: str = "chats.json"):
        self.file_path = Path(file_path)
        self.chats: Dict[str, dict] = {}  # chat_id -> {title, type}
        self.load()

    def load(self) -> None:
        """Загрузить список чатов из файла."""
        if not self.file_path.exists():
            self.save()
            return
        
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                self.chats = json.load(f)
            logger.info("Loaded %d chats from %s", len(self.chats), self.file_path)
        except Exception as exc:
            logger.error("Failed to load chats: %s", exc)
            self.chats = {}

    def save(self) -> None:
        """Сохранить список чатов в файл."""
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.chats, f, ensure_ascii=False, indent=2)
        except Exception as exc:
            logger.error("Failed to save chats: %s", exc)

    def add_chat(self, chat_id: str, title: str = "", chat_type: str = "") -> None:
        """Добавить чат в список."""
        self.chats[str(chat_id)] = {
            "title": title or f"Чат {chat_id}",
            "type": chat_type or "unknown"
        }
        self.save()

    def get_chat_title(self, chat_id: str) -> str:
        """Получить название чата."""
        return self.chats.get(str(chat_id), {}).get("title", f"Чат {chat_id}")

    def get_all_chats(self) -> Dict[str, dict]:
        """Получить все чаты."""
        return self.chats.copy()


@dataclass
class Config:
    telegram_token: str
    tasks_file: str = "tasks.json"
    admins_file: str = "admins.json"
    timezone: str = "Europe/Moscow"
    parse_mode: str = "HTML"

    @staticmethod
    def from_env() -> "Config":
        return Config(
            telegram_token=os.environ["TELEGRAM_BOT_TOKEN"],
            tasks_file=os.environ.get("TASKS_FILE", "tasks.json"),
            admins_file=os.environ.get("ADMINS_FILE", "admins.json"),
            timezone=os.environ.get("TZ", "Europe/Moscow"),
            parse_mode=os.environ.get("DEFAULT_PARSE_MODE", "HTML"),
        )


@dataclass
class Task:
    task_id: str
    chat_id: str
    time_str: str
    weekdays: Optional[List[str]]
    monthday: Optional[int]
    message: str
    media_type: Optional[str]
    media_url: Optional[str]
    parse_mode: str
    enabled: bool = True

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        return cls(**data)


class AdminManager:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.admins: set[str] = set()
        self.load()

    def load(self) -> None:
        """Загрузить список админов из файла."""
        if not self.file_path.exists():
            # Если файла нет, создаем пустой список
            self.save()
            logger.warning("Admins file not found. Created empty admins list. Add admins using /add_admin command.")
            return
        
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.admins = set(str(admin_id) for admin_id in data.get("admins", []))
            logger.info("Loaded %d admins from %s", len(self.admins), self.file_path)
        except Exception as exc:
            logger.error("Failed to load admins: %s", exc)
            self.admins = set()

    def save(self) -> None:
        """Сохранить список админов в файл."""
        try:
            data = {"admins": list(self.admins)}
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info("Saved %d admins to %s", len(self.admins), self.file_path)
        except Exception as exc:
            logger.error("Failed to save admins: %s", exc)

    def is_admin(self, user_id: int) -> bool:
        """Проверить, является ли пользователь админом."""
        return str(user_id) in self.admins

    def add_admin(self, user_id: int) -> bool:
        """Добавить админа."""
        user_id_str = str(user_id)
        if user_id_str not in self.admins:
            self.admins.add(user_id_str)
            self.save()
            return True
        return False

    def remove_admin(self, user_id: int) -> bool:
        """Удалить админа."""
        user_id_str = str(user_id)
        if user_id_str in self.admins:
            self.admins.remove(user_id_str)
            self.save()
            return True
        return False

    def get_all_admins(self) -> List[str]:
        """Получить список всех админов."""
        return list(self.admins)


class TaskStorage:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        self.tasks: Dict[str, Task] = {}
        self.load()

    def load(self) -> None:
        """Загрузить задачи из файла."""
        if not self.file_path.exists():
            self.save()
            return
        
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                self.tasks = {
                    task_id: Task.from_dict(task_data)
                    for task_id, task_data in data.items()
                }
            logger.info("Loaded %d tasks from %s", len(self.tasks), self.file_path)
        except Exception as exc:
            logger.error("Failed to load tasks: %s", exc)
            self.tasks = {}

    def save(self) -> None:
        """Сохранить задачи в файл."""
        try:
            data = {task_id: task.to_dict() for task_id, task in self.tasks.items()}
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            logger.info("Saved %d tasks to %s", len(self.tasks), self.file_path)
        except Exception as exc:
            logger.error("Failed to save tasks: %s", exc)

    def add_task(self, task: Task) -> None:
        """Добавить задачу."""
        self.tasks[task.task_id] = task
        self.save()

    def remove_task(self, task_id: str) -> bool:
        """Удалить задачу."""
        if task_id in self.tasks:
            del self.tasks[task_id]
            self.save()
            return True
        return False

    def get_task(self, task_id: str) -> Optional[Task]:
        """Получить задачу по ID."""
        return self.tasks.get(task_id)

    def get_all_tasks(self) -> List[Task]:
        """Получить все задачи."""
        return list(self.tasks.values())

    def get_enabled_tasks(self) -> List[Task]:
        """Получить только активные задачи."""
        return [task for task in self.tasks.values() if task.enabled]


class TaskScheduler:
    def __init__(self, bot: Bot, config: Config, storage: TaskStorage):
        self.bot = bot
        self.config = config
        self.storage = storage
        self.tz = ZoneInfo(config.timezone)
        self.scheduler = AsyncIOScheduler(timezone=self.tz)

    async def start(self) -> None:
        await self.refresh_jobs()
        self.scheduler.start()
        logger.info("Scheduler started; jobs loaded.")

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)

    async def refresh_jobs(self) -> None:
        """Обновить задачи в планировщике."""
        tasks = self.storage.get_enabled_tasks()
        desired_ids = {task.task_id for task in tasks}
        current_ids = {job.id for job in self.scheduler.get_jobs()}

        for job_id in current_ids - desired_ids:
            self.scheduler.remove_job(job_id)
            logger.info("Removed outdated job %s", job_id)

        for task in tasks:
            self._schedule_task(task)

        logger.info("Jobs synced: %s active", len(desired_ids))

    def _schedule_task(self, task: Task) -> None:
        """Запланировать задачу."""
        hour, minute = parse_time(task.time_str)
        trigger = CronTrigger(
            hour=hour,
            minute=minute,
            day=task.monthday or "*",
            day_of_week=",".join(task.weekdays) if task.weekdays else "*",
            timezone=self.tz,
        )
        self.scheduler.add_job(
            self._wrap_coro(self._send_task, task),
            trigger=trigger,
            id=task.task_id,
            replace_existing=True,
            misfire_grace_time=300,
        )
        logger.info(
            "Scheduled job %s -> chat %s at %s (weekdays=%s monthday=%s)",
            task.task_id,
            task.chat_id,
            task.time_str,
            task.weekdays,
            task.monthday,
        )

    async def _send_task(self, task: Task) -> None:
        """Отправить сообщение по задаче."""
        await send_message(
            bot=self.bot,
            chat_id=task.chat_id,
            text=task.message,
            media_type=task.media_type,
            media_url=task.media_url,
            parse_mode=task.parse_mode,
        )

    @staticmethod
    def _wrap_coro(coro_func, *args, **kwargs):
        async def runner():
            try:
                await coro_func(*args, **kwargs)
            except Exception as exc:
                logger.exception("Job failed: %s", exc)

        return runner


def parse_time(value: str) -> Tuple[int, int]:
    parts = value.strip().split(":")
    if len(parts) != 2:
        raise ValueError("Time must be HH:MM")
    hour, minute = int(parts[0]), int(parts[1])
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Time out of range")
    return hour, minute


def parse_weekdays(value: str) -> Optional[List[str]]:
    if value is None or str(value).strip() == "":
        return None
    tokens = str(value).replace(";", ",").split(",")
    result = []
    for token in tokens:
        key = token.strip().lower()
        if not key:
            continue
        mapped = WEEKDAY_ALIASES.get(key)
        if not mapped:
            raise ValueError(f"Unknown weekday: {token}")
        result.append(mapped)
    return result or None


def parse_monthday(value: str) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    md = int(value)
    if not 1 <= md <= 31:
        raise ValueError("monthday must be 1-31")
    return md


def parse_media(media_type: str, media_url: str) -> Tuple[Optional[str], Optional[str]]:
    mtype = str(media_type or "").strip().lower()
    url = str(media_url or "").strip()
    if not mtype:
        return None, None
    if mtype not in MEDIA_SENDERS:
        raise ValueError(f"Unsupported media_type {mtype}")
    if not url:
        raise ValueError("media_url required when media_type set")
    return mtype, url


async def send_message(
    bot: Bot,
    chat_id: str,
    text: str,
    media_type: Optional[str],
    media_url: Optional[str],
    parse_mode: str,
) -> None:
    codex/fix-tg_schedu-request-size-issue-ezoxb6
    max_text_length = 3500
    max_caption_length = 1024
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            if media_type and media_url:
                sender_name = MEDIA_SENDERS[media_type]
                sender = getattr(bot, sender_name)
                if text and len(text) > max_caption_length:
                    await sender(chat_id=chat_id, **{media_type: media_url})
                    await send_text_chunks(
                        bot,
                        chat_id,
                        text,
                        parse_mode=parse_mode,
                        max_length=max_text_length,
                    )
                else:
                    await sender(
                        chat_id=chat_id,
                        caption=text or None,
                        parse_mode=parse_mode if text else None,
                        **{media_type: media_url},
                    )
            else:
                await send_text_chunks(
                    bot,
                    chat_id,
                    text,
                    parse_mode=parse_mode,
                    max_length=max_text_length,
                    disable_web_page_preview=True,
                )
            return
        except Exception as exc:
            if attempt == attempts:
                logger.exception("Failed to send to %s after %s attempts: %s", chat_id, attempt, exc)
                return
            sleep_for = attempt * 2
            logger.warning("Send failed (attempt %s/%s), retry in %ss: %s", attempt, attempts, sleep_for, exc)
            await asyncio.sleep(sleep_for)


def split_text(text: str, max_length: int) -> List[str]:
    if not text:
        return [""]
    lines = text.split("\n")
    chunks: List[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= max_length:
            current = candidate
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(line) <= max_length:
            current = line
            continue
        start = 0
        while start < len(line):
            end = start + max_length
            chunks.append(line[start:end])
            start = end
    if current or not chunks:
        chunks.append(current)
    return chunks


async def send_text_chunks(
    bot: Bot,
    chat_id: str,
    text: str,
    parse_mode: Optional[str],
    max_length: int,
    **kwargs,
) -> None:
    for chunk in split_text(text, max_length):
        await bot.send_message(
            chat_id=chat_id,
            text=chunk,
            parse_mode=parse_mode,
            **kwargs,
        )


async def safe_reply(
    message: Message,
    text: str,
    parse_mode: Optional[str] = None,
    reply_markup = None,
    **kwargs
) -> Optional[Message]:
    """
    Безопасный ответ на сообщение с повторными попытками при сетевых ошибках.
    
    Args:
        message: Сообщение, на которое нужно ответить
        text: Текст ответа
        parse_mode: Режим парсинга (HTML, Markdown и т.д.)
        reply_markup: Клавиатура для ответа
        **kwargs: Дополнительные параметры для message.reply()
    
    Returns:
        Message объект при успехе, None при неудаче после всех попыток
    """
    codex/fix-tg_schedu-request-size-issue-ezoxb6
    max_text_length = 3500
 

    async def reply_once(chunk: str, *, include_markup: bool) -> Optional[Message]:
        attempts = 3
        for attempt in range(1, attempts + 1):
            try:
                return await message.reply(
                    chunk,
                    parse_mode=parse_mode,
                    reply_markup=reply_markup if include_markup else None,
                    **kwargs
                )
            except (TelegramNetworkError, OSError, ClientConnectorError, ClientOSError, ClientConnectorSSLError) as exc:
                if attempt == attempts:
                    logger.exception(
                        "Failed to reply to message %s after %s attempts: %s",
                        message.message_id,
                        attempts,
                        exc
                    )
                    return None
                sleep_for = attempt * 2
                logger.warning(
                    "Reply failed (attempt %s/%s), retry in %ss: %s",
                    attempt,
                    attempts,
                    sleep_for,
                    exc
                )
                await asyncio.sleep(sleep_for)
            except Exception as exc:
                logger.exception("Unexpected error while replying: %s", exc)
                return None

    chunks = split_text(text, max_text_length)
    if len(chunks) == 1:
        return await reply_once(chunks[0], include_markup=True)

    last_message: Optional[Message] = None
    for index, chunk in enumerate(chunks):
        last_message = await reply_once(chunk, include_markup=index == len(chunks) - 1)
        if last_message is None:
            return None
    return last_message


async def safe_answer(
    callback: CallbackQuery,
    text: Optional[str] = None,
    show_alert: bool = False,
    **kwargs
) -> bool:
    """
    Безопасный ответ на callback с повторными попытками при сетевых ошибках.
    
    Args:
        callback: CallbackQuery для ответа
        text: Текст ответа (опционально)
        show_alert: Показать как alert (опционально)
        **kwargs: Дополнительные параметры для callback.answer()
    
    Returns:
        True при успехе, False при неудаче после всех попыток
    """
    attempts = 3
    for attempt in range(1, attempts + 1):
        try:
            await callback.answer(text=text, show_alert=show_alert, **kwargs)
            return True
        except (TelegramNetworkError, OSError, ClientConnectorError, ClientOSError, ClientConnectorSSLError) as exc:
            if attempt == attempts:
                logger.warning(
                    "Failed to answer callback %s after %s attempts: %s",
                    callback.id,
                    attempts,
                    exc
                )
                return False
            sleep_for = attempt * 2
            logger.warning(
                "Answer failed (attempt %s/%s), retry in %ss: %s",
                attempt,
                attempts,
                sleep_for,
                exc
            )
            await asyncio.sleep(sleep_for)
        except Exception as exc:
            # Для других ошибок не повторяем попытки
            logger.warning("Unexpected error while answering callback: %s", exc)
            return False


def generate_task_id(chat_id: str, time_str: str, weekdays: Optional[List[str]], monthday: Optional[int], message: str = "") -> str:
    """Генерировать уникальный ID задачи."""
    import hashlib
    weekday_str = ",".join(sorted(weekdays)) if weekdays else "any"
    monthday_str = str(monthday) if monthday else "any"
    # Добавляем хеш сообщения для уникальности
    msg_hash = hashlib.md5(message.encode()).hexdigest()[:8]
    return f"{chat_id}-{time_str}-{weekday_str}-{monthday_str}-{msg_hash}"


def get_main_menu_keyboard() -> ReplyKeyboardMarkup:
    """Создать клавиатуру главного меню."""
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Список задач"), KeyboardButton(text="➕ Добавить задачу")],
            [KeyboardButton(text="✏️ Редактировать задачу"), KeyboardButton(text="🗑️ Удалить задачу")],
            [KeyboardButton(text="💬 ID чата"), KeyboardButton(text="❓ Помощь")],
        ],
        resize_keyboard=True,
        persistent=True
    )
    return keyboard


async def main() -> None:
    load_dotenv(dotenv_path=Path(".env"))
    config = Config.from_env()
    logger.info("Starting bot scheduler")

    storage = TaskStorage(config.tasks_file)
    admin_manager = AdminManager(config.admins_file)
    chat_storage = ChatStorage()

    # Инициализация админов из переменной окружения при первом запуске
    initial_admins = os.environ.get("ADMIN_IDS", "")
    if initial_admins and not admin_manager.get_all_admins():
        admin_ids = [aid.strip() for aid in initial_admins.split(",") if aid.strip()]
        for admin_id_str in admin_ids:
            try:
                admin_id = int(admin_id_str)
                admin_manager.add_admin(admin_id)
                logger.info("Added initial admin: %s", admin_id)
            except ValueError:
                logger.warning("Invalid admin ID in ADMIN_IDS: %s", admin_id_str)
        if admin_manager.get_all_admins():
            logger.info("Initialized %d admins from ADMIN_IDS", len(admin_manager.get_all_admins()))

    # Если админов все еще нет, предупреждаем
    if not admin_manager.get_all_admins():
        logger.warning("⚠️  ВНИМАНИЕ: Список админов пуст! Добавьте первого админа через переменную окружения ADMIN_IDS или создайте файл admins.json вручную.")

    # Инициализация бота
    # Обработка сетевых ошибок выполняется через функцию safe_reply()
    async with Bot(token=config.telegram_token) as bot:
        dp = Dispatcher(storage=MemoryStorage())
        scheduler = TaskScheduler(bot=bot, config=config, storage=storage)
        await scheduler.start()

        # Декоратор для проверки прав админа
        def admin_only(func):
            import inspect
            import functools
            
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                # Определяем тип первого аргумента
                if args and isinstance(args[0], Message):
                    message = args[0]
                    user_id = message.from_user.id if message.from_user else None
                elif args and isinstance(args[0], CallbackQuery):
                    callback = args[0]
                    user_id = callback.from_user.id if callback.from_user else None
                else:
                    user_id = None
                
                if not user_id or not admin_manager.is_admin(user_id):
                    if args and isinstance(args[0], Message):
                        await safe_reply(
                            args[0],
                            "❌ У вас нет прав администратора для выполнения этой команды.",
                            reply_markup=get_main_menu_keyboard()
                        )
                    elif args and isinstance(args[0], CallbackQuery):
                        await safe_answer(args[0], "❌ У вас нет прав администратора.", show_alert=True)
                    return
                
                # Удаляем служебные аргументы aiogram, которые не нужны функциям
                # Эти аргументы передаются aiogram автоматически, но функции их не ожидают
                filtered_kwargs = {k: v for k, v in kwargs.items() 
                                 if k not in ('dispatcher', 'event', 'raw_state', 'bot')}
                
                # Всегда передаем только отфильтрованные kwargs
                # Все обработчики меню имеют **kwargs в сигнатуре, поэтому это безопасно
                return await func(*args, **filtered_kwargs)
            return wrapper

        @dp.message(Command("cancel"))
        @admin_only
        async def cancel_handler(message: Message, state: FSMContext, **kwargs) -> None:
            """Отменить текущую операцию и вернуться в главное меню."""
            current_state = await state.get_state()
            await state.clear()  # Очищаем состояние в любом случае
            if current_state:
                await safe_reply(
                    message,
                    "❌ Операция отменена.",
                    reply_markup=get_main_menu_keyboard()
                )
            else:
                await safe_reply(
                    message,
                    "ℹ️ Нет активных операций для отмены.",
                    reply_markup=get_main_menu_keyboard()
                )

        @dp.message(Command("start"))
        async def start_handler(message: Message) -> None:
            user_id = message.from_user.id if message.from_user else None
            is_admin = user_id and admin_manager.is_admin(user_id)
            
            if not is_admin:
                await safe_reply(message, "❌ У вас нет прав администратора. Обратитесь к администратору бота.")
                return
            
            admin_text = "\n🔐 Админ-команды:\n/add_admin - добавить админа\n/remove_admin - удалить админа\n/list_admins - список админов\n" if is_admin else ""
            
            await safe_reply(
                message,
                "👋 Привет! Я бот для планирования сообщений.\n\n"
                "Используйте кнопки меню ниже для управления задачами.\n\n"
                "Доступные команды:\n"
                "/chat_id - узнать ID чата\n"
                "/add_task - добавить задачу\n"
                "/list_tasks - список задач\n"
                "/delete_task - удалить задачу\n"
                "/edit_task - редактировать задачу\n"
                "/help - помощь" + admin_text,
                reply_markup=get_main_menu_keyboard()
            )

        @dp.message(Command("help"))
        @admin_only
        async def help_handler(message: Message) -> None:
            help_text = """
📖 Помощь по командам:

/add_task - Добавить новую задачу
Формат: /add_task <время> <сообщение> [chat_id] [параметры]

Параметры:
• <время> - время в формате HH:MM (например: 09:00)
• <сообщение> - текст сообщения
• [chat_id] - опционально, ID группы/чата (если не указан - текущий чат)
• [дни недели] - пн,вт,ср,чт,пт,сб,вс (через запятую)
• [число месяца] - 1-31
• [тип медиа] - photo, video, document
• [ссылка медиа] - URL или file_id

Примеры:
• /add_task 09:00 Доброе утро!
• /add_task 12:00 Обед пн,ср,пт
• /add_task 09:00 Привет -1001234567890 (для конкретной группы)
• /add_task 18:00 Фото пн,ср,пт photo https://example.com/image.jpg

/list_tasks - Показать все задачи

/delete_task <ID> - Удалить задачу по ID

/edit_task <ID> - Редактировать задачу (интерактивно)

/chat_id - Узнать ID текущего чата

💡 Используйте кнопки меню для быстрого доступа к функциям!
"""
            await safe_reply(message, help_text, reply_markup=get_main_menu_keyboard())

        @dp.message(Command("chat_id"))
        @admin_only
        async def chat_id_handler(message: Message) -> None:
            try:
                chat_id = str(message.chat.id)
                chat_title = message.chat.title or "Без названия"
                chat_type = message.chat.type or "unknown"
                
                # Добавляем чат в хранилище
                chat_storage.add_chat(chat_id, chat_title, chat_type)
                
                # Формируем информативное сообщение
                chat_info = f"📋 <b>Информация о чате:</b>\n\n"
                chat_info += f"🆔 <b>ID чата:</b> <code>{escape(chat_id)}</code>\n"
                chat_info += f"📝 <b>Название:</b> {escape(chat_title)}\n"
                chat_info += f"📂 <b>Тип:</b> {escape(chat_type)}\n\n"
                chat_info += f"✅ Чат добавлен в список для выбора при создании задач."
                
                await safe_reply(
                    message,
                    chat_info,
                    parse_mode="HTML",
                    reply_markup=get_main_menu_keyboard()
                )
            except Exception as e:
                logger.exception("Error in chat_id_handler")
                await safe_reply(
                    message,
                    f"❌ Произошла ошибка при получении ID чата: {e}",
                    reply_markup=get_main_menu_keyboard()
                )

        @dp.message(Command("add_task"))
        @admin_only
        async def add_task_handler(message: Message, state: FSMContext) -> None:
            
            # Добавляем текущий чат в список, если его там нет
            current_chat_id = str(message.chat.id)
            chat_storage.add_chat(current_chat_id, message.chat.title or "", message.chat.type or "")
            
            # Показываем клавиатуру для выбора чата
            chats = chat_storage.get_all_chats()
            keyboard = []
            
            # Добавляем кнопку "Текущий чат"
            keyboard.append([InlineKeyboardButton(
                text=f"📱 Текущий чат ({message.chat.title or current_chat_id})",
                callback_data=f"select_chat_{current_chat_id}"
            )])
            
            # Добавляем другие известные чаты
            for chat_id, chat_info in chats.items():
                if chat_id != current_chat_id:
                    title = chat_info.get("title", chat_id)
                    keyboard.append([InlineKeyboardButton(
                        text=f"💬 {title}",
                        callback_data=f"select_chat_{chat_id}"
                    )])
            
            # Кнопка для добавления нового чата вручную
            keyboard.append([InlineKeyboardButton(
                text="➕ Добавить чат по ID",
                callback_data="add_chat_manual"
            )])
            
            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_task"
            )])
            
            await safe_reply(
                message,
                "📋 <b>Создание новой задачи</b>\n\n"
                "Шаг 1/7: Выберите чат/группу для отправки сообщений:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode="HTML"
            )
            await state.set_state(TaskCreationStates.waiting_for_chat)

        # Обработчик для добавления чата вручную
        @dp.callback_query(F.data == "add_chat_manual", StateFilter(TaskCreationStates.waiting_for_chat))
        @admin_only
        async def add_chat_manual_callback(callback: CallbackQuery, state: FSMContext) -> None:
            await callback.message.edit_text(
                "💬 <b>Добавление чата по ID</b>\n\n"
                "Введите ID чата/группы для добавления.\n\n"
                "💡 <b>Как узнать ID:</b>\n"
                "• Для группы: добавьте бота в группу и отправьте /chat_id\n"
                "• ID группы обычно начинается с -100 (например: -1001234567890)\n"
                "• ID личного чата - это просто число (ваш Telegram ID)\n\n"
                "Или отправьте /cancel для отмены.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")
                ]])
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_chat_id_manual)
        
        # Обработчик ввода chat_id вручную
        @dp.message(StateFilter(TaskCreationStates.waiting_for_chat_id_manual))
        @admin_only
        async def process_chat_id_manual(message: Message, state: FSMContext) -> None:
            # Проверяем, что это текстовое сообщение
            if not message.text:
                await safe_reply(
                    message,
                    "❌ Пожалуйста, отправьте текстовое сообщение с ID чата.\n\n"
                    "Или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            chat_id_input = message.text.strip()
            
            # Проверяем команду отмены
            if chat_id_input.lower() in ['/cancel', 'отмена', 'cancel']:
                await safe_reply(
                    message,
                    "❌ Добавление чата отменено.",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                return
            
            # Валидация chat_id (должен быть числом, может быть отрицательным для групп)
            try:
                # Проверяем, что это число (может быть отрицательным)
                chat_id = str(int(chat_id_input))
            except ValueError:
                await safe_reply(
                    message,
                    f"❌ Неверный формат ID. ID должен быть числом.\n\n"
                    f"Примеры:\n"
                    f"• Для группы: -1001234567890\n"
                    f"• Для личного чата: 123456789\n\n"
                    f"Попробуйте еще раз или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            # Добавляем чат в хранилище
            chat_storage.add_chat(chat_id, f"Чат {chat_id}", "unknown")
            
            # Сохраняем chat_id в состоянии и переходим к выбору времени
            await state.update_data(chat_id=chat_id)
            
            # Показываем клавиатуру для выбора времени
            keyboard = []
            popular_times = ["09:00", "12:00", "15:00", "18:00", "21:00"]
            row = []
            for time in popular_times:
                row.append(InlineKeyboardButton(text=time, callback_data=f"select_time_{time}"))
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton(
                text="✏️ Ввести время вручную",
                callback_data="enter_time_manual"
            )])
            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_task"
            )])
            
            chat_title = chat_storage.get_chat_title(chat_id)
            await safe_reply(
                message,
                f"✅ Чат добавлен: {chat_title} (ID: <code>{escape(chat_id)}</code>)\n\n"
                f"Шаг 2/7: Выберите время отправки (HH:MM):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode="HTML"
            )
            await state.set_state(TaskCreationStates.waiting_for_time)

        # Обработчик выбора чата
        @dp.callback_query(F.data.startswith("select_chat_"), StateFilter(TaskCreationStates.waiting_for_chat))
        @admin_only
        async def select_chat_callback(callback: CallbackQuery, state: FSMContext) -> None:
            chat_id = callback.data.replace("select_chat_", "")
            await state.update_data(chat_id=chat_id)
            
            # Показываем клавиатуру для выбора времени
            keyboard = []
            # Популярные времена
            popular_times = ["09:00", "12:00", "15:00", "18:00", "21:00"]
            row = []
            for time in popular_times:
                row.append(InlineKeyboardButton(text=time, callback_data=f"select_time_{time}"))
                if len(row) == 2:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton(
                text="✏️ Ввести время вручную",
                callback_data="enter_time_manual"
            )])
            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_task"
            )])
            
            chat_title = chat_storage.get_chat_title(chat_id)
            await callback.message.edit_text(
                f"✅ Чат выбран: {chat_title}\n\n"
                f"Шаг 2/7: Выберите время отправки (HH:MM):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_time)

        # Обработчик ввода времени вручную
        @dp.callback_query(F.data == "enter_time_manual", StateFilter(TaskCreationStates.waiting_for_time))
        @admin_only
        async def enter_time_manual_callback(callback: CallbackQuery, state: FSMContext) -> None:
            await callback.message.edit_text(
                "⏰ Введите время в формате HH:MM (например: 09:30):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")
                ]])
            )
            await callback.answer()

        # Обработчик выбора времени из кнопок
        @dp.callback_query(F.data.startswith("select_time_"), StateFilter(TaskCreationStates.waiting_for_time))
        @admin_only
        async def select_time_callback(callback: CallbackQuery, state: FSMContext) -> None:
            time_str = callback.data.replace("select_time_", "")
            await state.update_data(time_str=time_str)
            
            await callback.message.edit_text(
                f"✅ Время выбрано: {time_str}\n\n"
                f"Шаг 3/7: Введите текст сообщения:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")
                ]])
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_message)

        # Обработчик ввода текста сообщения
        @dp.message(StateFilter(TaskCreationStates.waiting_for_time))
        @admin_only
        async def process_time_input(message: Message, state: FSMContext) -> None:
            # Проверяем, что это текстовое сообщение
            if not message.text:
                await safe_reply(
                    message,
                    "❌ Пожалуйста, отправьте текстовое сообщение с временем (HH:MM).\n\n"
                    "Или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            try:
                time_str = message.text.strip()
                parse_time(time_str)  # Валидация
                await state.update_data(time_str=time_str)
                
                await safe_reply(
                    message,
                    f"✅ Время установлено: {time_str}\n\n"
                    f"Шаг 3/7: Введите текст сообщения:",
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                        InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")
                    ]])
                )
                await state.set_state(TaskCreationStates.waiting_for_message)
            except ValueError:
                await safe_reply(message, "❌ Неверный формат времени. Используйте HH:MM (например: 09:30)")

        # Обработчик ввода сообщения
        @dp.message(StateFilter(TaskCreationStates.waiting_for_message))
        async def process_message_input(message: Message, state: FSMContext) -> None:
            # Проверяем, что это текстовое сообщение
            if not message.text:
                await safe_reply(
                    message,
                    "❌ Пожалуйста, отправьте текстовое сообщение.\n\n"
                    "Или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            message_text = message.text.strip()
            if not message_text:
                await safe_reply(
                    message,
                    "❌ Текст сообщения не может быть пустым.\n\n"
                    "Попробуйте еще раз или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            await state.update_data(message_text=message_text)
            
            # Показываем клавиатуру для выбора дней недели
            keyboard = []
            weekdays_list = [
                ("Пн", "mon"), ("Вт", "tue"), ("Ср", "wed"),
                ("Чт", "thu"), ("Пт", "fri"), ("Сб", "sat"), ("Вс", "sun")
            ]
            row = []
            for ru_name, en_name in weekdays_list:
                row.append(InlineKeyboardButton(text=ru_name, callback_data=f"toggle_day_{en_name}"))
                if len(row) == 4:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton(
                text="✅ Пропустить (каждый день)",
                callback_data="skip_weekdays"
            )])
            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_task"
            )])
            
            await safe_reply(
                message,
                f"✅ Сообщение: {message_text[:50]}...\n\n"
                f"Шаг 4/7: Выберите дни недели (можно несколько):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await state.update_data(selected_days=[])
            await state.set_state(TaskCreationStates.waiting_for_weekdays)

        # Обработчик выбора дней недели
        @dp.callback_query(F.data.startswith("toggle_day_"), StateFilter(TaskCreationStates.waiting_for_weekdays))
        @admin_only
        async def toggle_day_callback(callback: CallbackQuery, state: FSMContext) -> None:
            day = callback.data.replace("toggle_day_", "")
            data = await state.get_data()
            selected_days = data.get("selected_days", [])
            
            if day in selected_days:
                selected_days.remove(day)
            else:
                selected_days.append(day)
            
            await state.update_data(selected_days=selected_days)
            
            # Обновляем клавиатуру
            keyboard = []
            weekdays_list = [
                ("Пн", "mon"), ("Вт", "tue"), ("Ср", "wed"),
                ("Чт", "thu"), ("Пт", "fri"), ("Сб", "sat"), ("Вс", "sun")
            ]
            row = []
            for ru_name, en_name in weekdays_list:
                prefix = "✅ " if en_name in selected_days else ""
                row.append(InlineKeyboardButton(text=f"{prefix}{ru_name}", callback_data=f"toggle_day_{en_name}"))
                if len(row) == 4:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton(
                text="➡️ Далее",
                callback_data="confirm_weekdays"
            )])
            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_task"
            )])
            
            days_text = ", ".join([d for d in selected_days]) if selected_days else "не выбрано"
            await callback.message.edit_reply_markup(
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer(f"Выбрано: {days_text}")

        # Подтверждение дней недели
        @dp.callback_query(F.data == "confirm_weekdays", StateFilter(TaskCreationStates.waiting_for_weekdays))
        @admin_only
        async def confirm_weekdays_callback(callback: CallbackQuery, state: FSMContext) -> None:
            data = await state.get_data()
            selected_days = data.get("selected_days", [])
            weekdays = selected_days if selected_days else None
            await state.update_data(weekdays=weekdays)
            
            # Показываем клавиатуру для выбора числа месяца
            keyboard = []
            # Числа 1-31 в виде кнопок (по 5 в ряд)
            row = []
            for i in range(1, 32):
                row.append(InlineKeyboardButton(text=str(i), callback_data=f"select_monthday_{i}"))
                if len(row) == 5:
                    keyboard.append(row)
                    row = []
            if row:
                keyboard.append(row)
            
            keyboard.append([InlineKeyboardButton(
                text="✅ Пропустить (любое число)",
                callback_data="skip_monthday"
            )])
            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_task"
            )])
            
            await callback.message.edit_text(
                f"✅ Дни недели: {', '.join(selected_days) if selected_days else 'каждый день'}\n\n"
                f"Шаг 5/7: Выберите число месяца (1-31):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_monthday)

        # Пропуск дней недели
        @dp.callback_query(F.data == "skip_weekdays", StateFilter(TaskCreationStates.waiting_for_weekdays))
        @admin_only
        async def skip_weekdays_callback(callback: CallbackQuery, state: FSMContext) -> None:
            await state.update_data(weekdays=None, selected_days=[])
            await confirm_weekdays_callback(callback, state)

        # Обработчик выбора числа месяца
        @dp.callback_query(F.data.startswith("select_monthday_"), StateFilter(TaskCreationStates.waiting_for_monthday))
        @admin_only
        async def select_monthday_callback(callback: CallbackQuery, state: FSMContext) -> None:
            monthday = int(callback.data.replace("select_monthday_", ""))
            await state.update_data(monthday=monthday)
            
            # Показываем клавиатуру для выбора типа медиа
            keyboard = [
                [InlineKeyboardButton(text="📷 Фото", callback_data="select_media_photo")],
                [InlineKeyboardButton(text="🎥 Видео", callback_data="select_media_video")],
                [InlineKeyboardButton(text="📄 Документ", callback_data="select_media_document")],
                [InlineKeyboardButton(text="✅ Без медиа", callback_data="skip_media")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")]
            ]
            
            await callback.message.edit_text(
                f"✅ Число месяца: {monthday}\n\n"
                f"Шаг 6/7: Выберите тип медиа (или пропустите):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_media_type)

        # Пропуск числа месяца
        @dp.callback_query(F.data == "skip_monthday", StateFilter(TaskCreationStates.waiting_for_monthday))
        @admin_only
        async def skip_monthday_callback(callback: CallbackQuery, state: FSMContext) -> None:
            await state.update_data(monthday=None)
            
            # Показываем клавиатуру для выбора типа медиа
            keyboard = [
                [InlineKeyboardButton(text="📷 Фото", callback_data="select_media_photo")],
                [InlineKeyboardButton(text="🎥 Видео", callback_data="select_media_video")],
                [InlineKeyboardButton(text="📄 Документ", callback_data="select_media_document")],
                [InlineKeyboardButton(text="✅ Без медиа", callback_data="skip_media")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")]
            ]
            
            await callback.message.edit_text(
                f"✅ Число месяца: любое\n\n"
                f"Шаг 6/7: Выберите тип медиа (или пропустите):",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard)
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_media_type)

        # Обработчик выбора типа медиа
        @dp.callback_query(F.data.startswith("select_media_"), StateFilter(TaskCreationStates.waiting_for_media_type))
        @admin_only
        async def select_media_type_callback(callback: CallbackQuery, state: FSMContext) -> None:
            media_type = callback.data.replace("select_media_", "")
            await state.update_data(media_type=media_type)
            
            await callback.message.edit_text(
                f"✅ Тип медиа: {media_type}\n\n"
                f"Шаг 7/7: Введите URL медиафайла или file_id Telegram:",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")
                ]])
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.waiting_for_media_url)

        # Пропуск медиа
        @dp.callback_query(F.data == "skip_media", StateFilter(TaskCreationStates.waiting_for_media_type))
        @admin_only
        async def skip_media_callback(callback: CallbackQuery, state: FSMContext) -> None:
            await state.update_data(media_type=None, media_url=None)
            # Переходим к подтверждению
            data = await state.get_data()
            chat_id = data.get("chat_id")
            time_str = data.get("time_str")
            message_text = data.get("message_text")
            weekdays = data.get("weekdays")
            monthday = data.get("monthday")
            
            chat_title = chat_storage.get_chat_title(chat_id)
            weekday_str = ", ".join(weekdays) if weekdays else "каждый день"
            monthday_str = str(monthday) if monthday else "любое"
            
            safe_chat_title = escape(chat_title)
            safe_message = escape(message_text)
            confirm_text = (
                f"📋 <b>Подтверждение задачи</b>\n\n"
                f"💬 Чат: {safe_chat_title}\n"
                f"⏰ Время: {time_str}\n"
                f"📝 Сообщение: {safe_message}\n"
                f"📅 Дни недели: {weekday_str}\n"
                f"🔢 Число месяца: {monthday_str}\n"
                f"📎 Медиа: нет\n\n"
                f"Создать задачу?"
            )
            
            keyboard = [
                [InlineKeyboardButton(text="✅ Создать", callback_data="create_task")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")]
            ]
            
            await callback.message.edit_text(
                confirm_text,
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode="HTML"
            )
            await callback.answer()
            await state.set_state(TaskCreationStates.confirming)

        # Обработчик ввода URL медиа
        @dp.message(StateFilter(TaskCreationStates.waiting_for_media_url))
        @admin_only
        async def process_media_url_input(message: Message, state: FSMContext) -> None:
            # Проверяем, что это текстовое сообщение
            if not message.text:
                await safe_reply(
                    message,
                    "❌ Пожалуйста, отправьте текстовое сообщение с URL медиафайла.\n\n"
                    "Или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            media_url = message.text.strip()
            if not media_url:
                await safe_reply(
                    message,
                    "❌ URL не может быть пустым.\n\n"
                    "Попробуйте еще раз или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            await state.update_data(media_url=media_url)
            await confirm_task(message, state)

        # Функция подтверждения задачи
        async def confirm_task(message_or_callback, state: FSMContext) -> None:
            data = await state.get_data()
            
            chat_id = data.get("chat_id")
            time_str = data.get("time_str")
            message_text = data.get("message_text")
            weekdays = data.get("weekdays")
            monthday = data.get("monthday")
            media_type = data.get("media_type")
            media_url = data.get("media_url")
            
            # Формируем текст подтверждения
            chat_title = chat_storage.get_chat_title(chat_id)
            weekday_str = ", ".join(weekdays) if weekdays else "каждый день"
            monthday_str = str(monthday) if monthday else "любое"
            media_str = f"{media_type}: {media_url}" if media_type else "нет"
            
            # Экранируем все пользовательские данные
            safe_chat_title = escape(chat_title)
            safe_message = escape(message_text)
            safe_media_str = escape(media_str)
            
            confirm_text = (
                f"📋 <b>Подтверждение задачи</b>\n\n"
                f"💬 Чат: {safe_chat_title}\n"
                f"⏰ Время: {time_str}\n"
                f"📝 Сообщение: {safe_message}\n"
                f"📅 Дни недели: {weekday_str}\n"
                f"🔢 Число месяца: {monthday_str}\n"
                f"📎 Медиа: {safe_media_str}\n\n"
                f"Создать задачу?"
            )
            
            keyboard = [
                [InlineKeyboardButton(text="✅ Создать", callback_data="create_task")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_task")]
            ]
            
            if isinstance(message_or_callback, Message):
                await message_or_callback.reply(
                    confirm_text,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                    parse_mode="HTML"
                )
            else:
                await message_or_callback.message.edit_text(
                    confirm_text,
                    reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                    parse_mode="HTML"
                )
                await message_or_callback.answer()
            
            await state.set_state(TaskCreationStates.confirming)

        # Обработчик подтверждения создания задачи
        @dp.callback_query(F.data == "create_task", StateFilter(TaskCreationStates.confirming))
        @admin_only
        async def create_task_callback(callback: CallbackQuery, state: FSMContext) -> None:
            data = await state.get_data()
            
            chat_id = data.get("chat_id")
            time_str = data.get("time_str")
            message_text = data.get("message_text")
            weekdays = data.get("weekdays")
            monthday = data.get("monthday")
            media_type = data.get("media_type")
            media_url = data.get("media_url")
            
            try:
                task_id = generate_task_id(chat_id, time_str, weekdays, monthday, message_text)
                task = Task(
                    task_id=task_id,
                    chat_id=chat_id,
                    time_str=time_str,
                    weekdays=weekdays,
                    monthday=monthday,
                    message=message_text,
                    media_type=media_type,
                    media_url=media_url,
                    parse_mode=config.parse_mode,
                    enabled=True,
                )
                
                storage.add_task(task)
                await scheduler.refresh_jobs()
                
                safe_task_id = escape(task_id)
                safe_chat_title = escape(chat_storage.get_chat_title(chat_id))
                safe_message = escape(message_text)
                await callback.message.edit_text(
                    f"✅ <b>Задача успешно создана!</b>\n\n"
                    f"📋 ID: <code>{safe_task_id}</code>\n"
                    f"💬 Чат: {safe_chat_title}\n"
                    f"⏰ Время: {time_str}\n"
                    f"📝 Сообщение: {safe_message}",
                    parse_mode="HTML"
                )
                await callback.answer("Задача создана!")
                await state.clear()
            except Exception as e:
                logger.exception("Error creating task")
                await callback.message.edit_text(f"❌ Ошибка при создании задачи: {e}")
                await callback.answer("Ошибка!")
                await state.clear()

        # Обработчик отмены
        @dp.callback_query(F.data == "cancel_task")
        @admin_only
        async def cancel_task_callback(callback: CallbackQuery, state: FSMContext) -> None:
            await callback.message.edit_text("❌ Создание задачи отменено.")
            await callback.answer()
            await state.clear()

        # Старый обработчик команды (оставляем для обратной совместимости, но упрощаем)
        @dp.message(Command("add_task_old"))
        async def add_task_old_handler(message: Message) -> None:
            user_id = message.from_user.id if message.from_user else None
            if not user_id or not admin_manager.is_admin(user_id):
                await safe_reply(message, "❌ У вас нет прав администратора для выполнения этой команды.")
                return
            try:
                parts = message.text.split(maxsplit=1)
                if len(parts) < 2:
                    await safe_reply(
                        message,
                        "❌ Неверный формат. Используйте:\n"
                        "/add_task <время> <сообщение> [chat_id] [дни недели] [число месяца] [тип медиа] [ссылка медиа]\n\n"
                        "Примеры:\n"
                        "• /add_task 09:00 Доброе утро!\n"
                        "• /add_task 12:00 Обед пн,ср,пт\n"
                        "• /add_task 15:00 Напоминание 15\n"
                        "• /add_task 18:00 Фото пн,ср,пт photo https://example.com/image.jpg\n"
                        "• /add_task 09:00 Привет -1001234567890 (для конкретной группы)"
                    )
                    return

                args = parts[1].strip()
                words = args.split()
                if len(words) < 2:
                    await safe_reply(message, "❌ Укажите время и сообщение")
                    return

                time_str = words[0]
                parse_time(time_str)  # Валидация времени сразу
                
                # По умолчанию используется текущий чат
                target_chat_id = str(message.chat.id)
                
                # Парсинг параметров
                weekdays = None
                monthday = None
                media_type = None
                media_url = None
                used_indices = {0}  # Индекс времени уже использован
                
                # Ищем chat_id (число, начинающееся с минуса или большое число > 8 цифр)
                # Проверяем перед парсингом других параметров, чтобы не спутать с числом месяца
                for i, word in enumerate(words):
                    if i in used_indices:
                        continue
                    # Chat_id обычно начинается с минуса (группы) или это большое число (личные чаты)
                    # Проверяем: начинается с "-" ИЛИ это число длиннее 8 цифр (не может быть числом месяца)
                    if (word.startswith("-") and word[1:].isdigit()) or (word.isdigit() and len(word) > 8):
                        try:
                            test_id = int(word)
                            target_chat_id = word
                            used_indices.add(i)
                            logger.info("Detected chat_id parameter: %s", target_chat_id)
                            break
                        except ValueError:
                            pass
                
                # Ищем дни недели (может содержать запятые)
                for i, word in enumerate(words):
                    if i in used_indices:
                        continue
                    word_lower = word.lower().replace(",", "").replace(";", "")
                    if any(day in word_lower for day in ["пн", "вт", "ср", "чт", "пт", "сб", "вс", "mon", "tue", "wed", "thu", "fri", "sat", "sun"]):
                        try:
                            weekdays = parse_weekdays(word)
                            used_indices.add(i)
                            break
                        except:
                            pass
                
                # Ищем число месяца (1-31)
                for i, word in enumerate(words):
                    if i in used_indices:
                        continue
                    try:
                        num = int(word)
                        if 1 <= num <= 31:
                            monthday = num
                            used_indices.add(i)
                            break
                    except:
                        pass
                
                # Ищем тип медиа и URL
                for i, word in enumerate(words):
                    if i in used_indices:
                        continue
                    if word.lower() in ["photo", "video", "document"]:
                        media_type = word.lower()
                        used_indices.add(i)
                        # Ищем URL после типа медиа
                        if i + 1 < len(words) and i + 1 not in used_indices:
                            media_url = words[i + 1]
                            used_indices.add(i + 1)
                        break

                # Формируем текст сообщения из оставшихся слов
                message_words = [w for i, w in enumerate(words[1:], start=1) if i not in used_indices]
                message_text = " ".join(message_words)
                
                if not message_text:
                    await message.reply("❌ Укажите текст сообщения")
                    return

                if media_type and not media_url:
                    await message.reply("❌ Укажите ссылку на медиа после типа (photo/video/document)")
                    return

                if media_type:
                    parse_media(media_type, media_url)

                task_id = generate_task_id(target_chat_id, time_str, weekdays, monthday, message_text)
                task = Task(
                    task_id=task_id,
                    chat_id=target_chat_id,
                    time_str=time_str,
                    weekdays=weekdays,
                    monthday=monthday,
                    message=message_text,
                    media_type=media_type,
                    media_url=media_url,
                    parse_mode=config.parse_mode,
                    enabled=True,
                )

                storage.add_task(task)
                await scheduler.refresh_jobs()
                
                weekday_str = ", ".join(weekdays) if weekdays else "любые"
                monthday_str = str(monthday) if monthday else "любое"
                media_str = f"{media_type}: {media_url}" if media_type else "нет"
                chat_info = f" (ID: {target_chat_id})" if target_chat_id != str(message.chat.id) else " (текущий чат)"
                
                safe_task_id = escape(task_id)
                safe_message = escape(message_text)
                safe_media_str = escape(media_str)
                await message.reply(
                    f"✅ Задача добавлена!\n\n"
                    f"📋 ID задачи: <code>{safe_task_id}</code>\n"
                    f"💬 Чат: {target_chat_id}{chat_info}\n"
                    f"⏰ Время: {time_str}\n"
                    f"📝 Сообщение: {safe_message}\n"
                    f"📅 Дни недели: {weekday_str}\n"
                    f"🔢 Число месяца: {monthday_str}\n"
                    f"📎 Медиа: {safe_media_str}",
                    parse_mode="HTML"
                )
            except ValueError as e:
                await safe_reply(message, f"❌ Ошибка: {e}", reply_markup=get_main_menu_keyboard())
            except Exception as e:
                logger.exception("Error adding task")
                await safe_reply(message, f"❌ Произошла ошибка: {e}", reply_markup=get_main_menu_keyboard())

        @dp.message(Command("list_tasks"))
        async def list_tasks_handler(message: Message) -> None:
            user_id = message.from_user.id if message.from_user else None
            if not user_id or not admin_manager.is_admin(user_id):
                await safe_reply(message, "❌ У вас нет прав администратора для выполнения этой команды.", reply_markup=get_main_menu_keyboard())
                return
            tasks = storage.get_all_tasks()
            if not tasks:
                await safe_reply(
                    message,
                    "📋 Задач пока нет. Используйте /add_task для добавления.",
                    reply_markup=get_main_menu_keyboard()
                )
                return

            # Админы видят все задачи, остальные - только для своего чата
            is_admin = admin_manager.is_admin(user_id)
            
            if is_admin:
                chat_tasks = tasks
                text = "📋 Список всех задач (админ-режим):\n\n"
            else:
                chat_tasks = [t for t in tasks if t.chat_id == str(message.chat.id)]
                if not chat_tasks:
                    await message.reply(
                        "📋 Для этого чата задач нет.",
                        reply_markup=get_main_menu_keyboard()
                    )
                    return
                text = "📋 Список задач:\n\n"
            
            for i, task in enumerate(chat_tasks, 1):
                status = "✅" if task.enabled else "❌"
                weekday_str = ", ".join(task.weekdays) if task.weekdays else "любые"
                monthday_str = str(task.monthday) if task.monthday else "любое"
                media_str = f"{task.media_type}" if task.media_type else "нет"
                chat_info = f" (чат: {task.chat_id})" if is_admin else ""
                
                # Экранируем все специальные символы
                safe_task_id = escape(task.task_id)
                safe_message = escape(task.message[:30])
                safe_media_str = escape(media_str)
                
                text += f"{status} <b>{i}.</b> ID: <code>{safe_task_id}</code>{chat_info}\n"
                text += f"   Время: {task.time_str}\n"
                text += f"   Сообщение: {safe_message}...\n"
                text += f"   Дни: {weekday_str}, Число: {monthday_str}\n"
                text += f"   Медиа: {safe_media_str}\n\n"

            await safe_reply(message, text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

        # Вспомогательная функция для показа списка задач для удаления
        async def show_delete_task_list(message: Message, state: FSMContext) -> None:
            """Показать список задач для удаления и сохранить их в состоянии."""
            user_id = message.from_user.id if message.from_user else None
            if not user_id or not admin_manager.is_admin(user_id):
                await safe_reply(message, "❌ У вас нет прав администратора.", reply_markup=get_main_menu_keyboard())
                return
            
            tasks = storage.get_all_tasks()
            if not tasks:
                await message.reply(
                    "📋 Задач пока нет. Используйте /add_task для добавления.",
                    reply_markup=get_main_menu_keyboard()
                )
                return
            
            # Фильтруем задачи в зависимости от прав
            is_admin = admin_manager.is_admin(user_id)
            if is_admin:
                chat_tasks = tasks
                text = "🗑️ <b>Удаление задачи</b>\n\n📋 <b>Список задач:</b>\n\n"
            else:
                chat_tasks = [t for t in tasks if t.chat_id == str(message.chat.id)]
                if not chat_tasks:
                    await message.reply(
                        "📋 Для этого чата задач нет.",
                        reply_markup=get_main_menu_keyboard()
                    )
                    return
                text = "🗑️ <b>Удаление задачи</b>\n\n📋 <b>Список задач:</b>\n\n"
            
            # Сохраняем список задач в состоянии для последующего поиска по номеру
            task_list = []
            for i, task in enumerate(chat_tasks, 1):
                task_list.append(task)
                status = "✅" if task.enabled else "❌"
                weekday_str = ", ".join(task.weekdays) if task.weekdays else "любые"
                monthday_str = str(task.monthday) if task.monthday else "любое"
                media_str = f"{task.media_type}" if task.media_type else "нет"
                chat_info = f" (чат: {task.chat_id})" if is_admin else ""
                
                safe_task_id = escape(task.task_id)
                safe_message = escape(task.message[:30])
                safe_media_str = escape(media_str)
                
                text += f"{status} <b>{i}.</b> ID: <code>{safe_task_id}</code>{chat_info}\n"
                text += f"   Время: {task.time_str}\n"
                text += f"   Сообщение: {safe_message}...\n"
                text += f"   Дни: {weekday_str}, Число: {monthday_str}\n"
                text += f"   Медиа: {safe_media_str}\n\n"
            
            text += "\n💡 <b>Введите номер задачи для удаления</b> (например: 1)\n"
            text += "Или отправьте /cancel для отмены."
            
            # Сохраняем список задач в состоянии
            await state.update_data(task_list=[task.task_id for task in task_list])
            await safe_reply(
                message,
                text,
                parse_mode="HTML",
                reply_markup=ReplyKeyboardRemove()
            )
            await state.set_state(DeleteTaskStates.waiting_for_task_number)

        @dp.message(Command("delete_task"))
        @admin_only
        async def delete_task_handler(message: Message, state: FSMContext) -> None:
            """Обработчик команды /delete_task - показывает список и запрашивает номер."""
            await show_delete_task_list(message, state)

        # Вспомогательная функция для получения реального task_id из callback_data
        async def get_real_task_id(callback_prefix: str, state: FSMContext) -> str:
            """Получить реальный task_id из callback_prefix (может быть хеш или сам task_id)."""
            # Если это короткий task_id (до 40 символов), возвращаем его
            if len(callback_prefix) <= 40:
                # Проверяем, существует ли задача с таким ID
                if storage.get_task(callback_prefix):
                    return callback_prefix
                # Если не найдена, возможно это хеш из 16 символов
                # Пробуем найти по хешу
                import hashlib
                for task in storage.get_all_tasks():
                    task_hash = hashlib.md5(task.task_id.encode()).hexdigest()[:16]
                    if task_hash == callback_prefix:
                        return task.task_id
                return callback_prefix
            
            # Если это длинный task_id, возвращаем как есть
            return callback_prefix

        EDIT_TASK_PAGE_SIZE = 20

        def build_edit_task_page(tasks: List[Task], offset: int) -> Tuple[str, InlineKeyboardMarkup]:
            total_tasks = len(tasks)
            end_index = min(offset + EDIT_TASK_PAGE_SIZE, total_tasks)
            page_tasks = tasks[offset:end_index]

            keyboard = []
            import hashlib
            for i, task in enumerate(page_tasks, offset + 1):
                chat_title = chat_storage.get_chat_title(task.chat_id)
                message_preview = task.message[:30]
                preview_suffix = "..." if len(task.message) > 30 else ""
                task_preview = f"{i}. {task.time_str} - {message_preview}{preview_suffix} ({chat_title})"
                # Используем хеш если task_id слишком длинный
                if len(task.task_id) > 40:
                    task_id_hash = hashlib.md5(task.task_id.encode()).hexdigest()[:16]
                    callback_data = f"edit_task_{task_id_hash}"
                else:
                    callback_data = f"edit_task_{task.task_id}"
                keyboard.append([InlineKeyboardButton(
                    text=task_preview,
                    callback_data=callback_data
                )])

            navigation = []
            if offset > 0:
                navigation.append(InlineKeyboardButton(
                    text="⬅️ Назад",
                    callback_data="edit_task_prev"
                ))
            remaining = total_tasks - end_index
            if remaining > 0:
                navigation.append(InlineKeyboardButton(
                    text=f"Показать еще ({remaining})",
                    callback_data="edit_task_more"
                ))
            if navigation:
                keyboard.append(navigation)

            keyboard.append([InlineKeyboardButton(
                text="❌ Отмена",
                callback_data="cancel_edit"
            )])

            page_text = (
                "📝 <b>Редактирование задач</b>\n\n"
                f"Показаны задачи {offset + 1}-{end_index} из {total_tasks}.\n"
                "Выберите задачу для редактирования:"
            )
            return page_text, InlineKeyboardMarkup(inline_keyboard=keyboard)

        @dp.message(Command("edit_task"))
        @admin_only
        async def edit_task_handler(message: Message, state: FSMContext) -> None:
            tasks = storage.get_all_tasks()
            if not tasks:
                await message.reply("📋 Задач пока нет. Используйте /add_task для добавления.")
                return

            await state.update_data(edit_task_offset=0)
            page_text, markup = build_edit_task_page(tasks, 0)

            await message.reply(
                page_text,
                reply_markup=markup,
                parse_mode="HTML"
            )

        # Обработчик выбора задачи для редактирования
        @dp.callback_query(F.data.startswith("edit_task_"))
        @admin_only
        async def edit_task_select_callback(callback: CallbackQuery, state: FSMContext, **kwargs) -> None:
            callback_prefix = callback.data.replace("edit_task_", "")
            
            if callback_prefix in {"more", "prev"}:
                tasks = storage.get_all_tasks()
                if not tasks:
                    await callback.answer("📋 Задач пока нет.", show_alert=True)
                    return

                data = await state.get_data()
                offset = data.get("edit_task_offset", 0)
                if callback_prefix == "more":
                    offset += EDIT_TASK_PAGE_SIZE
                else:
                    offset -= EDIT_TASK_PAGE_SIZE

                max_offset = ((len(tasks) - 1) // EDIT_TASK_PAGE_SIZE) * EDIT_TASK_PAGE_SIZE
                offset = max(0, min(offset, max_offset))
                await state.update_data(edit_task_offset=offset)

                page_text, markup = build_edit_task_page(tasks, offset)
                await callback.message.edit_text(
                    page_text,
                    reply_markup=markup,
                    parse_mode="HTML"
                )
                await callback.answer()
                return
            
            # Получаем реальный task_id
            task_id = await get_real_task_id(callback_prefix, state)
            task = storage.get_task(task_id)
            if not task:
                await callback.answer("❌ Задача не найдена", show_alert=True)
                return
            
            # Если использовался хеш, сохраняем соответствие в состоянии
            if len(callback_prefix) == 16 and len(task_id) > 40:
                await state.update_data(task_id_hash=callback_prefix, real_task_id=task_id)
            
            chat_title = chat_storage.get_chat_title(task.chat_id)
            weekday_str = ", ".join(task.weekdays) if task.weekdays else "каждый день"
            monthday_str = str(task.monthday) if task.monthday else "любое"
            media_str = f"{task.media_type}: {task.media_url}" if task.media_type else "нет"
            
            # Экранируем HTML символы в тексте
            safe_task_id = escape(task_id)
            safe_chat_title = escape(chat_title)
            safe_message = escape(task.message[:200])  # Ограничиваем длину для безопасности
            safe_media_str = escape(media_str)
            
            # Проверяем длину callback_data (Telegram ограничивает до 64 байт)
            # Используем хеш task_id если он слишком длинный
            import hashlib
            if len(task_id) > 40:
                task_id_hash = hashlib.md5(task_id.encode()).hexdigest()[:16]
                callback_prefix = task_id_hash
                # Сохраняем соответствие хеша и task_id в состоянии
                await state.update_data(task_id_hash=callback_prefix, real_task_id=task_id)
            else:
                callback_prefix = task_id
            
            keyboard = [
                [InlineKeyboardButton(text="✏️ Изменить время", callback_data=f"edit_field_{callback_prefix}_time")],
                [InlineKeyboardButton(text="✏️ Изменить сообщение", callback_data=f"edit_field_{callback_prefix}_message")],
                [InlineKeyboardButton(text="✏️ Изменить дни недели", callback_data=f"edit_field_{callback_prefix}_weekdays")],
                [InlineKeyboardButton(text="✏️ Изменить число месяца", callback_data=f"edit_field_{callback_prefix}_monthday")],
                [InlineKeyboardButton(text="✏️ Изменить медиа", callback_data=f"edit_field_{callback_prefix}_media")],
                [InlineKeyboardButton(text="🔄 Включить/Выключить", callback_data=f"toggle_task_{callback_prefix}")],
                [InlineKeyboardButton(text="🗑️ Удалить задачу", callback_data=f"delete_task_confirm_{callback_prefix}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]
            ]
            
            await callback.message.edit_text(
                f"📝 <b>Редактирование задачи</b>\n\n"
                f"📋 ID: <code>{safe_task_id}</code>\n"
                f"💬 Чат: {safe_chat_title}\n"
                f"⏰ Время: {task.time_str}\n"
                f"📝 Сообщение: {safe_message}{'...' if len(task.message) > 200 else ''}\n"
                f"📅 Дни недели: {weekday_str}\n"
                f"🔢 Число месяца: {monthday_str}\n"
                f"📎 Медиа: {safe_media_str}\n"
                f"✅ Статус: {'активна' if task.enabled else 'неактивна'}\n\n"
                f"Что хотите изменить?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode="HTML"
            )
            await callback.answer()

        # Обработчик отмены редактирования
        @dp.callback_query(F.data == "cancel_edit")
        @admin_only
        async def cancel_edit_callback(callback: CallbackQuery) -> None:
            await callback.message.edit_text("❌ Редактирование отменено.")
            await callback.answer()

        # Обработчик переключения статуса задачи
        @dp.callback_query(F.data.startswith("toggle_task_"))
        @admin_only
        async def toggle_task_callback(callback: CallbackQuery, state: FSMContext, **kwargs) -> None:
            callback_prefix = callback.data.replace("toggle_task_", "")
            task_id = await get_real_task_id(callback_prefix, state)
            task = storage.get_task(task_id)
            if not task:
                await callback.answer("❌ Задача не найдена", show_alert=True)
                return
            
            task.enabled = not task.enabled
            storage.add_task(task)  # Сохраняем изменения
            await scheduler.refresh_jobs()
            
            status = "активна" if task.enabled else "неактивна"
            await callback.answer(f"✅ Задача теперь {status}")
            
            # Обновляем сообщение
            chat_title = chat_storage.get_chat_title(task.chat_id)
            weekday_str = ", ".join(task.weekdays) if task.weekdays else "каждый день"
            monthday_str = str(task.monthday) if task.monthday else "любое"
            media_str = f"{task.media_type}: {task.media_url}" if task.media_type else "нет"
            
            safe_task_id = escape(task_id)
            safe_chat_title = escape(chat_title)
            safe_message = escape(task.message[:200])
            safe_media_str = escape(media_str)
            
            # Используем тот же callback_prefix для кнопок
            keyboard = [
                [InlineKeyboardButton(text="✏️ Изменить время", callback_data=f"edit_field_{callback_prefix}_time")],
                [InlineKeyboardButton(text="✏️ Изменить сообщение", callback_data=f"edit_field_{callback_prefix}_message")],
                [InlineKeyboardButton(text="✏️ Изменить дни недели", callback_data=f"edit_field_{callback_prefix}_weekdays")],
                [InlineKeyboardButton(text="✏️ Изменить число месяца", callback_data=f"edit_field_{callback_prefix}_monthday")],
                [InlineKeyboardButton(text="✏️ Изменить медиа", callback_data=f"edit_field_{callback_prefix}_media")],
                [InlineKeyboardButton(text="🔄 Включить/Выключить", callback_data=f"toggle_task_{callback_prefix}")],
                [InlineKeyboardButton(text="🗑️ Удалить задачу", callback_data=f"delete_task_confirm_{callback_prefix}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]
            ]
            
            await callback.message.edit_text(
                f"📝 <b>Редактирование задачи</b>\n\n"
                f"📋 ID: <code>{safe_task_id}</code>\n"
                f"💬 Чат: {safe_chat_title}\n"
                f"⏰ Время: {task.time_str}\n"
                f"📝 Сообщение: {safe_message}{'...' if len(task.message) > 200 else ''}\n"
                f"📅 Дни недели: {weekday_str}\n"
                f"🔢 Число месяца: {monthday_str}\n"
                f"📎 Медиа: {safe_media_str}\n"
                f"✅ Статус: {'активна' if task.enabled else 'неактивна'}\n\n"
                f"Что хотите изменить?",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode="HTML"
            )

        # Обработчик подтверждения удаления задачи
        @dp.callback_query(F.data.startswith("delete_task_confirm_"))
        @admin_only
        async def delete_task_confirm_callback(callback: CallbackQuery, state: FSMContext, **kwargs) -> None:
            callback_prefix = callback.data.replace("delete_task_confirm_", "")
            task_id = await get_real_task_id(callback_prefix, state)
            task = storage.get_task(task_id)
            if not task:
                await callback.answer("❌ Задача не найдена", show_alert=True)
                return
            
            safe_message = escape(task.message[:50])
            
            keyboard = [
                [InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"delete_task_{callback_prefix}")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data=f"edit_task_{callback_prefix}")]
            ]
            
            await callback.message.edit_text(
                f"⚠️ <b>Подтверждение удаления</b>\n\n"
                f"Вы уверены, что хотите удалить задачу?\n\n"
                f"⏰ Время: {task.time_str}\n"
                f"📝 Сообщение: {safe_message}...",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=keyboard),
                parse_mode="HTML"
            )
            await callback.answer()

        # Обработчик удаления задачи через callback
        @dp.callback_query(F.data.startswith("delete_task_") & ~F.data.startswith("delete_task_confirm_"))
        @admin_only
        async def delete_task_callback_handler(callback: CallbackQuery, state: FSMContext, **kwargs) -> None:
            callback_prefix = callback.data.replace("delete_task_", "")
            task_id = await get_real_task_id(callback_prefix, state)
            if storage.remove_task(task_id):
                await scheduler.refresh_jobs()
                await callback.message.edit_text("✅ Задача успешно удалена.")
                await callback.answer("Задача удалена")
            else:
                await callback.answer("❌ Задача не найдена", show_alert=True)

        @dp.message(Command("add_admin"))
        async def add_admin_handler(message: Message) -> None:
            user_id = message.from_user.id if message.from_user else None
            if not user_id or not admin_manager.is_admin(user_id):
                await safe_reply(message, "❌ У вас нет прав администратора для выполнения этой команды.")
                return
            try:
                # Проверяем, есть ли упоминание пользователя или ID в сообщении
                if message.reply_to_message and message.reply_to_message.from_user:
                    user_id = message.reply_to_message.from_user.id
                else:
                    parts = message.text.split(maxsplit=1)
                    if len(parts) < 2:
                        await message.reply(
                            "❌ Укажите ID пользователя или ответьте на сообщение пользователя.\n"
                            "Пример: /add_admin 123456789",
                            reply_markup=get_main_menu_keyboard()
                        )
                        return
                    try:
                        user_id = int(parts[1].strip())
                    except ValueError:
                        await safe_reply(message, "❌ ID должен быть числом.", reply_markup=get_main_menu_keyboard())
                        return

                if admin_manager.add_admin(user_id):
                    await safe_reply(message, f"✅ Пользователь {user_id} добавлен в список админов.", reply_markup=get_main_menu_keyboard())
                else:
                    await safe_reply(message, f"ℹ️ Пользователь {user_id} уже является админом.", reply_markup=get_main_menu_keyboard())
            except Exception as e:
                logger.exception("Error adding admin")
                await safe_reply(message, f"❌ Произошла ошибка: {e}", reply_markup=get_main_menu_keyboard())

        @dp.message(Command("remove_admin"))
        @admin_only
        async def remove_admin_handler(message: Message) -> None:
            try:
                parts = message.text.split(maxsplit=1)
                if len(parts) < 2:
                    await message.reply(
                        "❌ Укажите ID пользователя.\n"
                        "Пример: /remove_admin 123456789",
                        reply_markup=get_main_menu_keyboard()
                    )
                    return
                
                user_id = int(parts[1].strip())
                current_user_id = message.from_user.id if message.from_user else None
                
                # Нельзя удалить самого себя
                if user_id == current_user_id:
                    await safe_reply(message, "❌ Вы не можете удалить себя из списка админов.", reply_markup=get_main_menu_keyboard())
                    return

                if admin_manager.remove_admin(user_id):
                    await safe_reply(message, f"✅ Пользователь {user_id} удален из списка админов.", reply_markup=get_main_menu_keyboard())
                else:
                    await safe_reply(message, f"ℹ️ Пользователь {user_id} не найден в списке админов.", reply_markup=get_main_menu_keyboard())
            except ValueError:
                await safe_reply(message, "❌ ID должен быть числом.", reply_markup=get_main_menu_keyboard())
            except Exception as e:
                logger.exception("Error removing admin")
                await safe_reply(message, f"❌ Произошла ошибка: {e}", reply_markup=get_main_menu_keyboard())

        @dp.message(Command("list_admins"))
        @admin_only
        async def list_admins_handler(message: Message) -> None:
            admins = admin_manager.get_all_admins()
            if not admins:
                await safe_reply(message, "📋 Список админов пуст.", reply_markup=get_main_menu_keyboard())
                return
            
            text = "📋 Список админов:\n\n"
            for i, admin_id in enumerate(admins, 1):
                safe_admin_id = escape(str(admin_id))
                text += f"{i}. <code>{safe_admin_id}</code>\n"
            
            await safe_reply(message, text, parse_mode="HTML", reply_markup=get_main_menu_keyboard())

        # Обработчики кнопок главного меню (должны быть после всех командных обработчиков)
        # Убрали StateFilter(None), чтобы кнопки работали даже во время создания задачи
        @dp.message(F.text == "📋 Список задач")
        @admin_only
        async def menu_list_tasks_handler(message: Message, state: FSMContext, **kwargs) -> None:
            # Отменяем текущую операцию, если она активна
            current_state = await state.get_state()
            if current_state:
                await state.clear()
                # Не показываем сообщение об отмене, чтобы не мешать пользователю
            # Перенаправляем на существующий обработчик
            await list_tasks_handler(message)
        
        @dp.message(F.text == "➕ Добавить задачу")
        @admin_only
        async def menu_add_task_handler(message: Message, state: FSMContext, **kwargs) -> None:
            # Отменяем текущую операцию, если она активна
            current_state = await state.get_state()
            if current_state:
                await state.clear()
                # Не показываем сообщение об отмене, чтобы не мешать пользователю
            # Перенаправляем на существующий обработчик
            await add_task_handler(message, state)
        
        @dp.message(F.text == "✏️ Редактировать задачу")
        @admin_only
        async def menu_edit_task_handler(message: Message, state: FSMContext, **kwargs) -> None:
            # Отменяем текущую операцию, если она активна
            current_state = await state.get_state()
            if current_state:
                await state.clear()
                # Не показываем сообщение об отмене, чтобы не мешать пользователю
            # Перенаправляем на существующий обработчик
            await edit_task_handler(message)
        
        @dp.message(F.text == "🗑️ Удалить задачу")
        @admin_only
        async def menu_delete_task_handler(message: Message, state: FSMContext, **kwargs) -> None:
            """Обработчик кнопки меню для удаления задачи."""
            # Отменяем текущую операцию, если она активна
            current_state = await state.get_state()
            if current_state:
                await state.clear()
                # Не показываем сообщение об отмене, чтобы не мешать пользователю
            await show_delete_task_list(message, state)
        
        # Обработчик ввода номера задачи для удаления
        @dp.message(StateFilter(DeleteTaskStates.waiting_for_task_number))
        @admin_only
        async def process_delete_task_number(message: Message, state: FSMContext, **kwargs) -> None:
            # Проверяем, что это текстовое сообщение
            if not message.text:
                await message.reply(
                    "❌ Пожалуйста, отправьте текстовое сообщение с номером задачи.\n\n"
                    "Или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            input_text = message.text.strip()
            
            # Проверяем команду отмены
            if input_text.lower() in ['/cancel', 'отмена', 'cancel']:
                await message.reply(
                    "❌ Удаление задачи отменено.",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                return
            
            # Получаем список задач из состояния
            data = await state.get_data()
            task_list = data.get("task_list", [])
            
            if not task_list:
                await message.reply(
                    "❌ Список задач устарел. Пожалуйста, начните удаление заново.",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                return
            
            # Пытаемся преобразовать ввод в номер
            try:
                task_number = int(input_text)
            except ValueError:
                safe_input = escape(input_text)
                await message.reply(
                    f"❌ <code>{safe_input}</code> не является номером задачи.\n\n"
                    "Пожалуйста, введите число (номер задачи из списка).\n"
                    "Или отправьте /cancel для отмены.",
                    parse_mode="HTML",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            # Проверяем, что номер в допустимом диапазоне
            if task_number < 1 or task_number > len(task_list):
                await message.reply(
                    f"❌ Номер задачи должен быть от 1 до {len(task_list)}.\n\n"
                    "Попробуйте еще раз или отправьте /cancel для отмены.",
                    reply_markup=ReplyKeyboardRemove()
                )
                return
            
            # Получаем task_id по номеру (номер начинается с 1, индекс с 0)
            task_id = task_list[task_number - 1]
            task = storage.get_task(task_id)
            
            if not task:
                safe_task_id = escape(task_id)
                await message.reply(
                    f"❌ Задача с ID <code>{safe_task_id}</code> не найдена.\n\n"
                    "Возможно, она была удалена. Начните удаление заново.",
                    parse_mode="HTML",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                return

            user_id = message.from_user.id if message.from_user else None
            is_admin = user_id and admin_manager.is_admin(user_id)
            
            # Админы могут удалять любые задачи, остальные - только для своего чата
            if not is_admin and task.chat_id != str(message.chat.id):
                await message.reply(
                    "❌ Вы можете удалять только задачи для своего чата.",
                    reply_markup=get_main_menu_keyboard()
                )
                await state.clear()
                return

            # Удаляем задачу
            storage.remove_task(task_id)
            await scheduler.refresh_jobs()
            
            safe_task_id = escape(task_id)
            safe_message = escape(task.message[:50])
            await message.reply(
                f"✅ Задача <b>№{task_number}</b> успешно удалена.\n\n"
                f"📋 ID: <code>{safe_task_id}</code>\n"
                f"📝 Сообщение: {safe_message}...",
                parse_mode="HTML",
                reply_markup=get_main_menu_keyboard()
            )
            await state.clear()
        
        @dp.message(F.text == "💬 ID чата")
        @admin_only
        async def menu_chat_id_handler(message: Message, state: FSMContext, **kwargs) -> None:
            # Отменяем текущую операцию, если она активна
            current_state = await state.get_state()
            if current_state:
                await state.clear()
                # Не показываем сообщение об отмене, чтобы не мешать пользователю
            # Перенаправляем на существующий обработчик
            await chat_id_handler(message)
        
        @dp.message(F.text == "❓ Помощь")
        @admin_only
        async def menu_help_handler(message: Message, state: FSMContext, **kwargs) -> None:
            # Отменяем текущую операцию, если она активна
            current_state = await state.get_state()
            if current_state:
                await state.clear()
                # Не показываем сообщение об отмене, чтобы не мешать пользователю
            # Перенаправляем на существующий обработчик
            await help_handler(message)
        
        # Обработчик для неизвестных текстовых сообщений (fallback)
        # Этот обработчик должен быть последним, чтобы не перехватывать команды и кнопки меню
        @dp.message(F.text, StateFilter(None))
        async def unknown_message_handler(message: Message, state: FSMContext) -> None:
            """Обработчик для неизвестных текстовых сообщений."""
            # Проверяем, не является ли это командой (начинается с /)
            if message.text and message.text.startswith('/'):
                return  # Команды обрабатываются другими обработчиками
            
            # Проверяем, не является ли это кнопкой меню
            menu_buttons = [
                "📋 Список задач", "➕ Добавить задачу", "✏️ Редактировать задачу",
                "🗑️ Удалить задачу", "💬 ID чата", "❓ Помощь"
            ]
            if message.text in menu_buttons:
                return  # Кнопки меню обрабатываются другими обработчиками
            
            user_id = message.from_user.id if message.from_user else None
            if user_id and admin_manager.is_admin(user_id):
                # Если это админ, показываем главное меню
                await safe_reply(
                    message,
                    "❓ Неизвестная команда. Используйте кнопки меню или команды:\n"
                    "/start - Главное меню\n"
                    "/help - Справка\n"
                    "/cancel - Отмена операции",
                    reply_markup=get_main_menu_keyboard()
                )

        try:
            await dp.start_polling(bot)
        finally:
            scheduler.shutdown()
            logger.info("Shutdown complete.")


if __name__ == "__main__":
    asyncio.run(main())
