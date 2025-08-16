import os
import json
import yt_dlp
import redis
from loguru import logger


# Redis
r = redis.Redis(host="localhost", port=6379, db=0)


download_path = "downloads/%(playlist_title)s/%(title)s.%(ext)s"
archive_file = "archive.txt"  # в корне проекта


def append_to_archive(video_id: str):
    """Записываем ID в archive.txt"""
    with open(archive_file, "a", encoding="utf-8") as f:
        f.write(f"youtube {video_id}\n")

def already_downloaded(video_id: str) -> bool:
    return r.sismember("downloaded_videos", video_id)

def mark_downloaded(video_id: str):
    r.sadd("downloaded_videos", video_id)
    append_to_archive(video_id)

def cache_info(key: str, info: dict, ttl: int = 3600):
    """Сохраняем JSON в Redis"""
    r.setex(key, ttl, json.dumps(info))

def get_cached_info(key: str):
    """Берём JSON из Redis"""
    data = r.get(key)
    if data:
        return json.loads(data)
    return None

def process_entry(ydl, entry, playlist_title=None):
    """Проверка и загрузка одного видео"""
    if not entry:
        return

    video_id = entry.get("id")
    if not video_id:
        return

    # проверка по Redis
    if already_downloaded(video_id):
        logger.info(f"⏩ Пропускаем {video_id}, уже скачано")
        return

    # если в entry нет playlist_title — проставляем его вручную
    if playlist_title and not entry.get("playlist_title"):
        entry["playlist_title"] = playlist_title

    # получаем финальное имя файла
    filename = ydl.prepare_filename(entry)
    if os.path.exists(filename):
        logger.info(f"📂 Файл {filename} уже существует, добавляем в Redis")
        mark_downloaded(video_id)
        return

    # качаем
    ydl.download([url])
    mark_downloaded(video_id)


def download(url: str, download_path: str = "downloads"):
    ydl_opts = {
        "outtmpl": os.path.join(download_path, "%(playlist_title)s/%(title)s.%(ext)s"),
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "ignoreerrors": True,
        "download_archive": "archive.txt",
        "retries": 10,
        "fragment_retries": 10,
        "continuedl": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        cache_key = f"ytinfo:{url}"

        # проверка кэша
        info = get_cached_info(cache_key)
        if info:
            logger.info("⚡ Берём данные из Redis-кэша")
        else:
            logger.info("🌍 Тянем данные с YouTube")
            info = ydl.extract_info(url, download=False)
            cache_info(cache_key, info, ttl=3600)

        # плейлист или одиночное видео
        if "entries" in info and info["entries"]:
            playlist_title = info.get("title")
            logger.info(f"📂 Найден плейлист: {playlist_title} ({len(info['entries'])} видео)")
            for entry in info["entries"]:
                process_entry(ydl, entry, playlist_title=playlist_title)
        else:
            logger.info(f"▶ Найдено видео: {info.get('title')}")
            process_entry(ydl, info)


if __name__ == "__main__":
    # Ссылка (может быть видео или плейлист)
    url = "https://youtube.com/playlist?list=PL7AEFfKAwqe7TA68MWsIWvQjwdlccSBGQ&si=61B4d-VQA3Er3wPr"
    # url = "https://youtube.com/playlist?list=PL13yZK73a_cxtjBI7PEJRkeZ2ddP54KfZ&si=fnsHI5RyY9RjYz5V"
    download(url)
