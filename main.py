import asyncio
import logging
import os
import re
import subprocess
import tempfile
import time
import urllib.parse
from pathlib import Path

import yt_dlp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery, FSInputFile, InlineKeyboardButton,
    InlineKeyboardMarkup, InputMediaAudio, Message,
)
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

RESULTS_PER_PAGE = 5
TOTAL_PAGES = 5
TOTAL_RESULTS = RESULTS_PER_PAGE * TOTAL_PAGES  # 25
BATCH_SIZE = 5         # треков в одном media group
MAX_TRACK_SECS = 20 * 60  # треки длиннее 20 мин пропускаем в режиме альбома
DOWNLOAD_CONCURRENCY = 2  # параллельных загрузок внутри батча

user_searches: dict[int, dict] = {}


class YtStates(StatesGroup):
    waiting_single = State()
    waiting_album = State()


def is_url(text: str) -> bool:
    return bool(re.match(r'https?://', text.strip()))


def sanitize_filename(name: str) -> str:
    return re.sub(r'[<>:"/\\|?*\n\r]', '_', name)[:100]


def n_pages(results: list) -> int:
    return min(TOTAL_PAGES, -(-len(results) // RESULTS_PER_PAGE))


_ANSI_RE = re.compile(r'\x1b\[[0-9;]*m')


def _friendly_error(exc: Exception) -> str:
    msg = _ANSI_RE.sub('', str(exc))
    if 'Private video' in msg:
        return '🔒 приватное видео — пропущено'
    if '403' in msg or 'Forbidden' in msg:
        return '⛔ нет доступа (403)'
    if 'Sign in' in msg or 'authentication' in msg.lower():
        return '🔐 требуется авторизация — пропущено'
    return msg[:300]


async def silent_delete(msg: Message) -> None:
    try:
        await msg.delete()
    except Exception:
        pass


# ── sync workers ──────────────────────────────────────────────────────────────

def _search_tracks_sync(query: str) -> list:
    log.info(f"[SEARCH]   \"{query}\"")
    opts = {'quiet': True, 'no_warnings': True, 'extract_flat': True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        data = ydl.extract_info(f"ytsearch{TOTAL_RESULTS}:{query}", download=False)
    results = (data.get('entries') or [])[:TOTAL_RESULTS]
    log.info(f"[SEARCH]   ✓ {len(results)} треков")
    return results


def _search_playlists_sync(query: str) -> list:
    log.info(f"[SEARCH-PL] \"{query}\"")
    # sp=EgIQAw%3D%3D — YouTube filter: playlists only
    encoded = urllib.parse.quote_plus(query)
    url = f"https://www.youtube.com/results?search_query={encoded}&sp=EgIQAw%3D%3D"
    opts = {'quiet': True, 'no_warnings': True, 'extract_flat': True}
    with yt_dlp.YoutubeDL(opts) as ydl:
        data = ydl.extract_info(url, download=False)
    results = (data.get('entries') or [])[:TOTAL_RESULTS]
    log.info(f"[SEARCH-PL] ✓ {len(results)} плейлистов")
    return results


_COOKIES_OPTS: dict = {'cookiesfrombrowser': ('firefox',)}


def _ydl_hook(d: dict) -> None:
    if d['status'] == 'finished':
        size_mb = (d.get('total_bytes') or d.get('downloaded_bytes') or 0) / 1048576
        log.info(f"[yt-dlp]   ✓ {Path(d['filename']).name}  ({size_mb:.1f} MB)")


_BASE_DL_OPTS: dict = {
    'format': 'bestaudio/best',
    'writethumbnail': True,
    'quiet': True,
    'no_warnings': True,
    'postprocessors': [{'key': 'FFmpegThumbnailsConvertor', 'format': 'jpg'}],
    'progress_hooks': [_ydl_hook],
}


_RETRIABLE = ('403', 'Forbidden', 'Requested format is not available', 'Sign in', 'HTTP Error 429')

# (extra_opts, seconds_to_wait_before_attempt)
# Попытка 2 — тот же конфиг через 3 с: лечит transient "format not available"
# (YouTube иногда возвращает пустой список форматов, но на повторный запрос — нормально)
_RETRY_PLAN = [
    ({},            0),   # 1: без куков, сразу
    (_COOKIES_OPTS, 3),   # 2: с куками, пауза 3 с
    ({},            5),   # 3: без куков, пауза 5 с
    (_COOKIES_OPTS, 5),   # 4: с куками, пауза 5 с
]


def _is_retriable(exc: Exception) -> bool:
    msg = str(exc)
    return any(token in msg for token in _RETRIABLE)


def _download_track_sync(url: str, out_template: str) -> dict:
    opts = {**_BASE_DL_OPTS, 'outtmpl': out_template}
    last_exc: Exception | None = None

    for attempt, (extra, wait) in enumerate(_RETRY_PLAN, 1):
        if wait:
            log.info(f"[RETRY]    #{attempt}  (пауза {wait}с)  {url}")
            time.sleep(wait)
        else:
            log.info(f"[DOWNLOAD] #{attempt}  {url}")
        try:
            with yt_dlp.YoutubeDL({**opts, **extra}) as ydl:
                return ydl.extract_info(url, download=True)
        except Exception as exc:
            if not _is_retriable(exc):
                raise
            log.warning(f"[RETRY]    #{attempt} не удался: {_ANSI_RE.sub('', str(exc))[:120]}")
            last_exc = exc

    raise last_exc  # type: ignore[misc]


def _get_playlist_info_sync(url: str) -> tuple[str, list]:
    opts = {'quiet': True, 'no_warnings': True, 'extract_flat': True, **_COOKIES_OPTS}
    with yt_dlp.YoutubeDL(opts) as ydl:
        data = ydl.extract_info(url, download=False)
    return (data.get('title') or 'Плейлист', data.get('entries') or [])


# ffmpeg делает всё: конвертация + обложка + метаданные
def _convert_to_mp3(
    src: Path, mp3: Path, cover: Path | None,
    title: str, artist: str, album: str,
) -> subprocess.CompletedProcess:
    cmd = ['ffmpeg', '-y', '-i', str(src)]

    if cover and cover.exists():
        cmd += [
            '-i', str(cover),
            '-map', '0:a',
            '-map', '1:v',
            '-c:v', 'copy',
            '-disposition:v:0', 'attached_pic',
        ]

    cmd += [
        '-acodec', 'libmp3lame',
        '-ab', '320k',
        '-ar', '44100',
        '-id3v2_version', '3',
        '-metadata', f'title={title}',
        '-metadata', f'artist={artist}',
    ]
    if album:
        cmd += ['-metadata', f'album={album}']

    cmd.append(str(mp3))
    return subprocess.run(cmd, capture_output=True)


# ── keyboards ─────────────────────────────────────────────────────────────────

def build_track_keyboard(user_id: int, page: int) -> InlineKeyboardMarkup:
    results = user_searches[user_id]['results']
    offset = page * RESULTS_PER_PAGE
    items = results[offset:offset + RESULTS_PER_PAGE]
    pages = n_pages(results)

    rows = []
    for i, entry in enumerate(items):
        title = entry.get('title') or 'Unknown'
        dur = entry.get('duration')
        dur_str = f" [{int(dur // 60)}:{int(dur % 60):02d}]" if dur else ""
        rows.append([InlineKeyboardButton(
            text=f"🎵 {title[:38]}{dur_str}",
            callback_data=f"dl_s:{offset + i}",
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"pg_s:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1} / {pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"pg_s:{page + 1}"))
    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_playlist_keyboard(user_id: int, page: int) -> InlineKeyboardMarkup:
    results = user_searches[user_id]['results']
    offset = page * RESULTS_PER_PAGE
    items = results[offset:offset + RESULTS_PER_PAGE]
    pages = n_pages(results)

    rows = []
    for i, entry in enumerate(items):
        title = entry.get('title') or 'Unknown'
        count = entry.get('n_entries') or entry.get('playlist_count') or ''
        count_str = f" [{count} тр.]" if count else ""
        rows.append([InlineKeyboardButton(
            text=f"📀 {title[:38]}{count_str}",
            callback_data=f"dl_a:{offset + i}",
        )])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"pg_a:{page - 1}"))
    nav.append(InlineKeyboardButton(text=f"{page + 1} / {pages}", callback_data="noop"))
    if page < pages - 1:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"pg_a:{page + 1}"))
    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ── core audio ────────────────────────────────────────────────────────────────

_IMG_EXTS = {'.jpg', '.jpeg', '.png', '.webp'}
_SKIP_EXTS = {'.json', '.part', '.ytdl'} | _IMG_EXTS

# Тип возврата _prepare_track: (mp3_path, title, artist, duration_sec, safe_name)
_TrackInfo = tuple[Path, str, str, int, str]


async def _prepare_track(url: str, track_dir: Path, album: str) -> _TrackInfo:
    """Скачивает и конвертирует один трек в track_dir. Не отправляет."""
    loop = asyncio.get_running_loop()

    track_dir.mkdir(parents=True, exist_ok=True)
    out_tmpl = str(track_dir / '%(title)s.%(ext)s')
    info = await loop.run_in_executor(None, _download_track_sync, url, out_tmpl)

    title = info.get('title') or 'audio'
    duration = int(info.get('duration') or 0)
    artist = info.get('artist') or info.get('uploader') or ''
    album_tag = album or info.get('album') or ''

    all_files = list(track_dir.iterdir())
    audio_files = [f for f in all_files if f.is_file() and f.suffix.lower() not in _SKIP_EXTS]
    cover_files = [f for f in all_files if f.is_file() and f.suffix.lower() in _IMG_EXTS]

    if not audio_files:
        raise RuntimeError("Скачанный файл не найден")

    src = audio_files[0]
    cover = cover_files[0] if cover_files else None
    safe = sanitize_filename(title)
    mp3 = track_dir / '_out.mp3'

    dur_str = f"{duration // 60}:{duration % 60:02d}" if duration else "?"
    log.info(f"[ffmpeg]   {title}  →  mp3  ({dur_str})")
    proc = await loop.run_in_executor(None, _convert_to_mp3, src, mp3, cover, title, artist, album_tag)
    if proc.returncode != 0 or not mp3.exists():
        stderr = proc.stderr.decode(errors='replace')
        raise RuntimeError(f"ffmpeg error:\n{stderr[-400:]}")

    log.info(f"[READY]    {title}  ({dur_str})")
    return mp3, title, artist, duration, safe


async def send_audio(url: str, target: Message, album: str = '') -> None:
    loop = asyncio.get_running_loop()

    with tempfile.TemporaryDirectory() as tmpdir:
        out_tmpl = os.path.join(tmpdir, '%(title)s.%(ext)s')
        info = await loop.run_in_executor(None, _download_track_sync, url, out_tmpl)

        title = info.get('title') or 'audio'
        duration = int(info.get('duration') or 0)
        artist = info.get('artist') or info.get('uploader') or ''
        album_tag = album or info.get('album') or ''

        all_files = list(Path(tmpdir).iterdir())
        audio_files = [f for f in all_files if f.is_file() and f.suffix.lower() not in _SKIP_EXTS]
        cover_files = [f for f in all_files if f.is_file() and f.suffix.lower() in _IMG_EXTS]

        if not audio_files:
            raise RuntimeError("Скачанный файл не найден")

        src = audio_files[0]
        cover = cover_files[0] if cover_files else None
        safe = sanitize_filename(title)
        # Фиксированное имя выхода — исключает WinError 32, когда src уже .mp3
        mp3 = Path(tmpdir) / '_out.mp3'

        proc = await loop.run_in_executor(
            None, _convert_to_mp3, src, mp3, cover, title, artist, album_tag
        )
        if proc.returncode != 0 or not mp3.exists():
            stderr = proc.stderr.decode(errors='replace')
            raise RuntimeError(f"ffmpeg error:\n{stderr[-400:]}")

        await target.answer_audio(
            audio=FSInputFile(mp3, filename=f"{safe}.mp3"),
            title=title,
            performer=artist,
            duration=duration,
        )
        # tmpdir и все файлы внутри удаляются автоматически здесь


async def _send_batch(target: Message, batch: list[_TrackInfo]) -> None:
    """Отправляет готовые треки: 1 трек — answer_audio, 2-10 — media group."""
    if len(batch) == 1:
        mp3, title, artist, duration, safe = batch[0]
        await target.answer_audio(
            audio=FSInputFile(mp3, filename=f"{safe}.mp3"),
            title=title, performer=artist, duration=duration,
        )
    else:
        await target.answer_media_group(media=[
            InputMediaAudio(
                media=FSInputFile(mp3, filename=f"{safe}.mp3"),
                title=title, performer=artist, duration=duration,
            )
            for mp3, title, artist, duration, safe in batch
        ])


async def send_playlist(playlist_url: str, target: Message) -> None:
    loop = asyncio.get_running_loop()

    progress = await target.answer("⏳ Получаю информацию о плейлисте…")
    try:
        album_title, entries = await loop.run_in_executor(
            None, _get_playlist_info_sync, playlist_url
        )

        if not entries:
            await progress.delete()
            await send_audio(playlist_url, target)
            return

        orig_total = len(entries)

        # Фильтрация: собираем валидные, сразу сообщаем о пропущенных
        valid: list[tuple[int, dict]] = []
        skipped_lines: list[str] = []
        for i, entry in enumerate(entries, 1):
            if not entry.get('id'):
                continue
            dur = entry.get('duration') or 0
            title = entry.get('title') or f'Трек {i}'
            if dur > MAX_TRACK_SECS:
                skipped_lines.append(f"• {title} ({int(dur // 60)} мин.)")
            else:
                valid.append((i, entry))

        n = len(valid)
        if n == 0:
            await progress.edit_text("❌ Нет доступных треков для скачивания")
            return

        if skipped_lines:
            await target.answer("⏭ Пропущено (> 20 мин.):\n" + "\n".join(skipped_lines))

        total_batches = -(-n // BATCH_SIZE)

        await progress.edit_text(
            f"📀 <b>{album_title}</b>\nСкачиваю {n} треков…",
            parse_mode="HTML",
        )

        sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

        async def _dl_one(local_idx: int, orig_i: int, entry: dict, batch_tmpdir: str) -> _TrackInfo | None:
            entry_title = entry.get('title') or f'Трек {orig_i}'
            track_url = f"https://www.youtube.com/watch?v={entry['id']}"
            track_dir = Path(batch_tmpdir) / f't{local_idx}'
            async with sem:
                try:
                    return await _prepare_track(track_url, track_dir, album_title)
                except Exception as exc:
                    await target.answer(
                        f"❌ [{orig_i}/{orig_total}] {entry_title}: {_friendly_error(exc)}"
                    )
                    return None

        first_batch = True
        for batch_num in range(1, total_batches + 1):
            batch_slice = valid[(batch_num - 1) * BATCH_SIZE : batch_num * BATCH_SIZE]

            first_i, last_i = batch_slice[0][0], batch_slice[-1][0]

            # Один tmpdir на весь батч — файлы живут до отправки media group
            with tempfile.TemporaryDirectory() as batch_tmpdir:
                await progress.edit_text(
                    f"📀 <b>{album_title}</b>\n"
                    f"⏳ Треки {first_i}–{last_i} из {orig_total}…",
                    parse_mode="HTML",
                )

                results = await asyncio.gather(*[
                    _dl_one(local_idx, orig_i, entry, batch_tmpdir)
                    for local_idx, (orig_i, entry) in enumerate(batch_slice, 1)
                ])

                ready = [r for r in results if r is not None]
                if ready:
                    if first_batch:
                        await target.answer(f"📀 <b>{album_title}</b>", parse_mode="HTML")
                        first_batch = False
                    await _send_batch(target, ready)
                # tmpdir удаляется здесь — уже после отправки

        await progress.delete()

    except Exception:
        await progress.edit_text("❌ Не удалось получить данные о плейлисте")
        raise


# ── input processors ──────────────────────────────────────────────────────────

async def _process_multi_url(message: Message, urls: list[str]) -> None:
    n = len(urls)
    msg = await message.answer(f"⏳ Скачиваю {n} треков…")
    sem = asyncio.Semaphore(DOWNLOAD_CONCURRENCY)

    async def dl_one(idx: int, url: str, track_dir: Path) -> _TrackInfo | None:
        async with sem:
            try:
                return await _prepare_track(url, track_dir, '')
            except Exception as exc:
                await message.answer(f"❌ [{idx}/{n}]: {_friendly_error(exc)}")
                return None

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            results = await asyncio.gather(*[
                dl_one(i, url, Path(tmpdir) / f't{i}')
                for i, url in enumerate(urls, 1)
            ])
            ready = [r for r in results if r is not None]
            for batch_start in range(0, len(ready), BATCH_SIZE):
                await _send_batch(message, ready[batch_start:batch_start + BATCH_SIZE])
        await msg.delete()
    except Exception as exc:
        await msg.edit_text(f"❌ Ошибка: {exc}")


async def process_single(message: Message, text: str) -> None:
    await silent_delete(message)
    tokens = text.split()
    if len(tokens) > 1 and all(is_url(t) for t in tokens):
        await _process_multi_url(message, tokens)
        return
    if is_url(text):
        msg = await message.answer("⏳ Скачиваю аудио…")
        try:
            await send_audio(text, message)
            await msg.delete()
        except Exception as exc:
            await msg.edit_text(f"❌ Ошибка: {exc}")
    else:
        msg = await message.answer("🔍 Ищу треки на YouTube…")
        try:
            results = await asyncio.get_running_loop().run_in_executor(
                None, _search_tracks_sync, text
            )
            if not results:
                await msg.edit_text("❌ Ничего не найдено")
                return
            user_searches[message.from_user.id] = {'query': text, 'results': results}
            await msg.edit_text(
                f"🎵 Результаты для: <b>{text}</b>\nСтраница 1 / {n_pages(results)}",
                reply_markup=build_track_keyboard(message.from_user.id, 0),
                parse_mode="HTML",
            )
        except Exception as exc:
            await msg.edit_text(f"❌ Ошибка поиска: {exc}")


async def process_album(message: Message, text: str) -> None:
    await silent_delete(message)
    if is_url(text):
        try:
            await send_playlist(text, message)
        except Exception as exc:
            await message.answer(f"❌ Ошибка: {exc}")
    else:
        msg = await message.answer("🔍 Ищу плейлисты на YouTube…")
        try:
            results = await asyncio.get_running_loop().run_in_executor(
                None, _search_playlists_sync, text
            )
            if not results:
                await msg.edit_text("❌ Плейлисты не найдены")
                return
            user_searches[message.from_user.id] = {'query': text, 'results': results}
            await msg.edit_text(
                f"📀 Плейлисты для: <b>{text}</b>\nСтраница 1 / {n_pages(results)}",
                reply_markup=build_playlist_keyboard(message.from_user.id, 0),
                parse_mode="HTML",
            )
        except Exception as exc:
            await msg.edit_text(f"❌ Ошибка поиска: {exc}")


# ── command handlers ──────────────────────────────────────────────────────────

@router.message(Command("s"))
async def cmd_single(message: Message, command: CommandObject, state: FSMContext):
    args = (command.args or "").strip()
    if not args:
        await state.set_state(YtStates.waiting_single)
        prompt = await message.answer("🎵 Отправь название трека или ссылку на YouTube:")
        await state.update_data(prompt_msg_id=prompt.message_id)
        return
    await process_single(message, args)


@router.message(Command("a"))
async def cmd_alb(message: Message, command: CommandObject, state: FSMContext):
    args = (command.args or "").strip()
    if not args:
        await state.set_state(YtStates.waiting_album)
        prompt = await message.answer("📀 Отправь название альбома / плейлиста или ссылку:")
        await state.update_data(prompt_msg_id=prompt.message_id)
        return
    await process_album(message, args)


@router.message(YtStates.waiting_single)
async def handle_single_query(message: Message, state: FSMContext):
    data = await state.get_data()
    await state.clear()
    prompt_msg_id = data.get('prompt_msg_id')
    if prompt_msg_id:
        try:
            await bot.delete_message(message.chat.id, prompt_msg_id)
        except Exception:
            pass
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Пустое сообщение. Попробуй /s снова.")
        return
    await process_single(message, text)


@router.message(YtStates.waiting_album)
async def handle_album_query(message: Message, state: FSMContext):
    data = await state.get_data()
    await state.clear()
    prompt_msg_id = data.get('prompt_msg_id')
    if prompt_msg_id:
        try:
            await bot.delete_message(message.chat.id, prompt_msg_id)
        except Exception:
            pass
    text = (message.text or "").strip()
    if not text:
        await message.answer("❌ Пустое сообщение. Попробуй /a снова.")
        return
    await process_album(message, text)


# ── callback handlers ─────────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("pg_s:"))
async def cb_page_single(callback: CallbackQuery):
    page = int(callback.data.split(":")[1])
    uid = callback.from_user.id
    if uid not in user_searches:
        await callback.answer("Результаты устарели", show_alert=True)
        return
    data = user_searches[uid]
    await callback.message.edit_text(
        f"🎵 Результаты для: <b>{data['query']}</b>\nСтраница {page + 1} / {n_pages(data['results'])}",
        reply_markup=build_track_keyboard(uid, page),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("pg_a:"))
async def cb_page_album(callback: CallbackQuery):
    page = int(callback.data.split(":")[1])
    uid = callback.from_user.id
    if uid not in user_searches:
        await callback.answer("Результаты устарели", show_alert=True)
        return
    data = user_searches[uid]
    await callback.message.edit_text(
        f"📀 Плейлисты для: <b>{data['query']}</b>\nСтраница {page + 1} / {n_pages(data['results'])}",
        reply_markup=build_playlist_keyboard(uid, page),
        parse_mode="HTML",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("dl_s:"))
async def cb_download_single(callback: CallbackQuery):
    idx = int(callback.data.split(":")[1])
    uid = callback.from_user.id
    search = user_searches.get(uid)
    if not search or idx >= len(search['results']):
        await callback.answer("Результаты устарели", show_alert=True)
        return

    entry = search['results'][idx]
    vid_id = entry.get('id')
    if not vid_id:
        await callback.answer("Не удалось получить ID видео", show_alert=True)
        return

    title = entry.get('title') or 'трек'
    await callback.answer("⬇️ Скачиваю…")
    msg = await callback.message.answer(f"⏳ Скачиваю <b>{title}</b>…", parse_mode="HTML")
    try:
        await send_audio(f"https://www.youtube.com/watch?v={vid_id}", callback.message)
        await silent_delete(msg)
        await silent_delete(callback.message)
    except Exception as exc:
        await msg.edit_text(f"❌ Ошибка: {exc}")


@router.callback_query(F.data.startswith("dl_a:"))
async def cb_download_album(callback: CallbackQuery):
    idx = int(callback.data.split(":")[1])
    uid = callback.from_user.id
    search = user_searches.get(uid)
    if not search or idx >= len(search['results']):
        await callback.answer("Результаты устарели", show_alert=True)
        return

    entry = search['results'][idx]
    playlist_id = entry.get('id')
    playlist_url = entry.get('url') or (
        f"https://www.youtube.com/playlist?list={playlist_id}" if playlist_id else None
    )
    if not playlist_url:
        await callback.answer("Не удалось получить URL плейлиста", show_alert=True)
        return

    await callback.answer("📀 Начинаю скачивать…")
    try:
        await send_playlist(playlist_url, callback.message)
        await silent_delete(callback.message)
    except Exception as exc:
        await callback.message.answer(f"❌ Ошибка: {exc}")


@router.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery):
    await callback.answer()


async def main():
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    asyncio.run(main())
