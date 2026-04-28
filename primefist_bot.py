import os
import re
import json
import logging
import asyncio
import feedparser
import html
import requests
import random
import sys
import shutil
import subprocess
import tempfile
from typing import Any
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from telegram import Bot, ReplyParameters
from telegram.constants import ParseMode
from groq import AsyncGroq
from urllib.parse import urljoin, urlsplit, urlunsplit

# ==========================================
# CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        log.warning("Invalid integer for %s, using default %s", name, default)
        return default


# Ensure we don't accidentally load an old ID from local Windows Env Variables during testing
BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
CHANNEL_ID = os.environ.get("CHANNEL_ID", "").strip()
TELEGRAM_DISCUSSION_CHAT_ID = os.environ.get("TELEGRAM_DISCUSSION_CHAT_ID", "").strip()
GROQ_API_KEY = os.environ.get("GROQ_API_KEY", "").strip()
X_RSS_FEED_URLS = [
    url.strip()
    for url in os.environ.get("X_RSS_FEED_URLS", "https://nitter.net/ufc/rss").split(",")
    if url.strip()
]

POSTED_FILE = "posted.json"
MAX_HISTORY = 2000
DISCUSSION_FORWARD_WAIT_SECONDS = 5
DISCUSSION_FORWARD_POLL_SECONDS = 0.25
RECENT_ARTICLE_HOURS = env_int("RECENT_ARTICLE_HOURS", 48)
MAX_ENTRIES_PER_FEED = env_int("MAX_ENTRIES_PER_FEED", 15)
X_POST_LOOKBACK_HOURS = env_int("X_POST_LOOKBACK_HOURS", 8)
UFC_EVENT_LOOKAHEAD_DAYS = env_int("UFC_EVENT_LOOKAHEAD_DAYS", 21)
UFC_COMPLETED_LOOKBACK_HOURS = env_int("UFC_COMPLETED_LOOKBACK_HOURS", 72)
MAX_TELEGRAM_VIDEO_MB = env_int("MAX_TELEGRAM_VIDEO_MB", 49)
MAX_TELEGRAM_VIDEO_BYTES = MAX_TELEGRAM_VIDEO_MB * 1024 * 1024
MAX_POSTS_PER_RUN = env_int("MAX_POSTS_PER_RUN", 3)
MAX_X_POSTS_PER_RUN = env_int("MAX_X_POSTS_PER_RUN", 1)
MAX_UFC_EVENT_POSTS_PER_RUN = env_int("MAX_UFC_EVENT_POSTS_PER_RUN", 1)
MAX_RSS_POSTS_PER_RUN = env_int("MAX_RSS_POSTS_PER_RUN", 1)
POST_SPACING_SECONDS = env_int("POST_SPACING_SECONDS", 8)
UFC_EVENTS_URL = os.environ.get("UFC_EVENTS_URL", "https://www.ufc.com/events").strip()
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
telegram_update_offset = None

HANDLE_NAMES = {
    "ercegsteve": ("Стив Эрцег", "Steve Erceg"),
    "thenightmare170": ("Карлос Пратес", "Carlos Prates"),
    "quillansalkilld": ("Куиллан Салкилд", "Quillan Salkilld"),
    "beneildariush": ("Бенеил Дариуш", "Beneil Dariush"),
    "westAustralia".lower(): ("Western Australia", "Western Australia"),
    "paramountplus": ("Paramount+", "Paramount+"),
}

HASHTAG_NAMES = {
    "ufcperth": ("UFC Perth", "UFC Perth"),
    "wathedreamstate": ("WA The Dream State", "WA The Dream State"),
}

FIGHTER_RU = {
    "Jack Della Maddalena": "Джек Делла Маддалена",
    "Carlos Prates": "Карлос Пратес",
    "Della Maddalena": "Делла Маддалена",
    "Prates": "Пратес",
    "Beneil Dariush": "Бенеил Дариуш",
    "Quillan Salkilld": "Куиллан Салкилд",
    "Tim Elliott": "Тим Эллиотт",
    "Steve Erceg": "Стив Эрцег",
    "Shamil Gaziev": "Шамиль Газиев",
    "Brando Peričić": "Брандо Перичич",
    "Tai Tuivasa": "Тай Туиваса",
    "Louie Sutherland": "Луи Сазерленд",
    "Aljamain Sterling": "Алджамейн Стерлинг",
    "Youssef Zalal": "Юссеф Залал",
}

FIGHTER_RU_OBJECT = {
    "Carlos Prates": "Карлоса Пратеса",
    "Prates": "Пратеса",
    "Quillan Salkilld": "Куиллана Салкилда",
    "Steve Erceg": "Стива Эрцега",
    "Brando Peričić": "Брандо Перичича",
    "Tai Tuivasa": "Тая Туивасу",
    "Louie Sutherland": "Луи Сазерленда",
    "Youssef Zalal": "Юссефа Залала",
}

WEIGHT_RU = {
    "Welterweight Bout": "полусредний вес",
    "Lightweight Bout": "легкий вес",
    "Flyweight Bout": "наилегчайший вес",
    "Heavyweight Bout": "тяжелый вес",
    "Featherweight Bout": "полулегкий вес",
    "Bantamweight Bout": "легчайший вес",
    "Middleweight Bout": "средний вес",
    "Women's Bantamweight Bout": "женский легчайший вес",
}

# THEIR RSS FEEDS LIST
RSS_FEEDS = [
    {"name": "MMA Fighting", "url": "https://www.mmafighting.com/rss/current", "tag": "#mma", "lang": "en"},
    {"name": "MMA Junkie", "url": "https://mmajunkie.usatoday.com/feed", "tag": "#mma", "lang": "en"},
    {"name": "Bloody Elbow", "url": "https://www.bloodyelbow.com/rss/current", "tag": "#mma", "lang": "en"},
    {"name": "Sherdog", "url": "https://www.sherdog.com/rss/news.xml", "tag": "#mma", "lang": "en"},
    {"name": "UFC News", "url": "https://www.ufc.com/rss/news", "tag": "#ufc #mma", "lang": "en"},
    {"name": "Boxing Scene", "url": "https://www.boxingscene.com/rss.php", "tag": "#boxing", "lang": "en"},
    {"name": "The Ring", "url": "https://www.ringtv.com/feed/", "tag": "#boxing", "lang": "en"},
    {"name": "Bad Left Hook", "url": "https://www.badlefthook.com/rss/current", "tag": "#boxing", "lang": "en"},
    {"name": "World Boxing News", "url": "https://www.worldboxingnews.net/feed/", "tag": "#boxing", "lang": "en"},
    {"name": "Sky Sports Boxing", "url": "https://www.skysports.com/rss/12040", "tag": "#boxing", "lang": "en"},
    {"name": "BBC Boxing", "url": "https://feeds.bbci.co.uk/sport/boxing/rss.xml", "tag": "#boxing", "lang": "en"},
    {"name": "ESPN MMA", "url": "https://www.espn.com/espn/rss/mma/news", "tag": "#mma", "lang": "en"},
    {"name": "Low Kick MMA", "url": "https://www.lowkickmma.com/feed/", "tag": "#mma #kickboxing", "lang": "en"},
    {"name": "MiddleEasy", "url": "https://middleeasy.com/feed/", "tag": "#mma", "lang": "en"},
    {"name": "BJPenn", "url": "https://www.bjpenn.com/feed/", "tag": "#mma", "lang": "en"},
    {"name": "MMA Mania", "url": "https://www.mmamania.com/rss/current", "tag": "#mma", "lang": "en"},
    {"name": "BJJ Heroes", "url": "https://www.bjjheroes.com/feed", "tag": "#bjj #grappling", "lang": "en"},
    {"name": "Gracie Mag", "url": "https://graciemag.com/feed/", "tag": "#bjj #jiujitsu", "lang": "en"},
    {"name": "Muay Thai Citizen", "url": "https://muaythaicitizen.com/feed/", "tag": "#muaythai", "lang": "en"},
    {"name": "FightTime", "url": "https://fighttime.ru/news/rss.html", "tag": "#mma #нарусском", "lang": "ru"},
    {"name": "Vringe", "url": "https://vringe.com/rss/", "tag": "#boxing #mma", "lang": "ru"},
    {"name": "MMABoxing.ru", "url": "https://mmaboxing.ru/rss.xml", "tag": "#mma #boxing", "lang": "ru"},
    {"name": "AllBoxing.ru", "url": "https://allboxing.ru/rss.xml", "tag": "#boxing #mma", "lang": "ru"},
    {"name": "Sport.ru MMA", "url": "https://www.sport.ru/rss/rubric/mma.xml", "tag": "#mma #нарусском", "lang": "ru"},
    {"name": "Sport.ru Boxing", "url": "https://www.sport.ru/rss/rubric/box.xml", "tag": "#boxing #нарусском", "lang": "ru"},
    {"name": "FightNews", "url": "https://fightnews.com/feed", "tag": "#boxing", "lang": "en"},
    {"name": "Boxing News 24", "url": "https://www.boxingnews24.com/feed/", "tag": "#boxing", "lang": "en"},
    {"name": "TalkSport Boxing", "url": "https://talksport.com/sport/boxing/feed/", "tag": "#boxing", "lang": "en"},
    {"name": "TalkSport MMA", "url": "https://talksport.com/sport/mma/feed/", "tag": "#mma", "lang": "en"},
    {"name": "Bleacher Report MMA", "url": "https://bleacherreport.com/articles/feed?tag_id=230", "tag": "#mma", "lang": "en"},
    {"name": "Bleacher Report Boxing", "url": "https://bleacherreport.com/articles/feed?tag_id=255", "tag": "#boxing", "lang": "en"},
    {"name": "MMA Weekly", "url": "https://www.mmaweekly.com/feed", "tag": "#mma", "lang": "en"},
    {"name": "Fox Sports MMA", "url": "https://api.foxsports.com/v1/rss?partnerKey=zBaFxRyGKCfxBagJG9b8pqLyndmvo7UU&tag=ufc", "tag": "#mma", "lang": "en"},
    {"name": "Overtime Heroics", "url": "https://www.overtimeheroics.net/category/mma/feed/", "tag": "#mma", "lang": "en"},
    {"name": "MyMMANews", "url": "https://mymmanews.com/feed/", "tag": "#mma", "lang": "en"},
    {"name": "Combat Press", "url": "https://combatpress.com/feed/", "tag": "#mma #kickboxing", "lang": "en"},
    {"name": "LiverKick", "url": "https://liverkick.com/feed/", "tag": "#kickboxing #muaythai", "lang": "en"},
    {"name": "Scrap Digest", "url": "https://scrapdigest.com/feed/", "tag": "#mma #boxing", "lang": "en"},
    {"name": "RingTV", "url": "https://www.ringtv.com/feed/", "tag": "#boxing", "lang": "en"},
    {"name": "Inside the Ropes", "url": "https://itrboxing.com/feed/", "tag": "#boxing", "lang": "en"},

    # ESTONIAN FEEDS
    # Note: Delfi and some specialized feeds are currently 404, using general sport feeds for now.
    {"name": "Postimees Sport", "url": "https://sport.postimees.ee/rss", "tag": "#mma #boxing #estonia", "lang": "et"},
    {"name": "ERR Sport - Võitlussport", "url": "https://sport.err.ee/rss/voitlussport", "tag": "#mma #boxing #estonia", "lang": "et"},
    # {"name": "Õhtuleht Sport", "url": "https://sport.ohtuleht.ee/rss", "tag": "#boxing #estonia", "lang": "et"} 
]


# ==========================================
# HELPERS
# ==========================================
def canonical_url(value: str) -> str:
    value = (value or "").strip()
    parts = urlsplit(value)
    if parts.scheme.lower() not in {"http", "https"} or not parts.netloc:
        return clean_text(value)

    netloc = parts.netloc.lower()
    if netloc.endswith(":80") or netloc.endswith(":443"):
        netloc = netloc.rsplit(":", 1)[0]

    path = parts.path or "/"
    if path != "/":
        path = path.rstrip("/")

    return urlunsplit(("https", netloc, path, "", ""))


def canonical_post_id(value: str) -> str:
    value = (value or "").strip()
    event_match = re.match(r"^(ufc-event:[^:]+:)(https?://.+)$", value, flags=re.IGNORECASE)
    if event_match:
        return f"{event_match.group(1)}{canonical_url(event_match.group(2))}"

    x_link = normalize_x_link(value)
    if re.search(r"https?://(?:www\.)?(?:x|twitter|nitter)\.", value, flags=re.IGNORECASE):
        return canonical_url(x_link)

    return canonical_url(value)


def normalize_posted_history(posted_list: Any) -> list[str]:
    if not isinstance(posted_list, list):
        return []

    normalized = []
    seen = set()
    for item in posted_list:
        post_id = canonical_post_id(str(item))
        if not post_id or post_id in seen:
            continue
        seen.add(post_id)
        normalized.append(post_id)
    return normalized[-MAX_HISTORY:]


def load_posted():
    if os.path.exists(POSTED_FILE):
        try:
            with open(POSTED_FILE, "r", encoding="utf-8") as f:
                return normalize_posted_history(json.load(f))
        except Exception:
            return []
    return []

def save_posted(posted_list):
    posted_list = normalize_posted_history(posted_list)
    with open(POSTED_FILE, "w", encoding="utf-8") as f:
        json.dump(posted_list, f, indent=2, ensure_ascii=False)


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(value.split())


def html_to_text(value: str | None, limit: int = 900) -> str:
    text = BeautifulSoup(value or "", "html.parser").get_text(" ", strip=True)
    return clean_text(text)[:limit]


def fetch_html(url: str, timeout: int = 15) -> str | None:
    try:
        response = requests.get(url, timeout=timeout, headers=HTTP_HEADERS)
        if response.status_code != 200:
            log.warning("HTTP %s for %s", response.status_code, url)
            return None
        return response.text
    except Exception as e:
        log.warning("Failed to fetch %s: %s", url, e)
        return None


def first_image_from_html(value: str | None) -> str | None:
    soup = BeautifulSoup(value or "", "html.parser")
    img = soup.find("img")
    return img.get("src") if img and img.get("src") else None


def extract_ufc_event_image(event_url: str) -> str | None:
    page = fetch_html(event_url)
    if not page:
        return None
    soup = BeautifulSoup(page, "html.parser")
    for selector in (
        'meta[property="og:image"]',
        'meta[name="twitter:image"]',
        ".c-hero img",
        "img",
    ):
        node = soup.select_one(selector)
        if not node:
            continue
        image = node.get("content") or node.get("src")
        if image:
            return urljoin(event_url, image)
    return None


def normalize_x_link(link: str) -> str:
    match = re.search(r"/ufc/status/(\d+)", link or "", flags=re.IGNORECASE)
    if match:
        return f"https://x.com/ufc/status/{match.group(1)}"
    return link


def compact_for_post(text: str, max_len: int = 420) -> str:
    text = clean_text(text)
    if len(text) <= max_len:
        return text
    trimmed = text[:max_len].rsplit(" ", 1)[0].rstrip(".,;: ")
    return f"{trimmed}..."


def compact_multiline(text: str, max_len: int) -> str:
    lines = []
    previous_blank = False
    for raw_line in (text or "").splitlines():
        line = clean_text(raw_line)
        if line:
            lines.append(line)
            previous_blank = False
        elif lines and not previous_blank:
            lines.append("")
            previous_blank = True
    while lines and not lines[-1]:
        lines.pop()

    text = "\n".join(lines).strip()
    if len(text) <= max_len:
        return text
    trimmed = text[:max_len].rsplit(" ", 1)[0].rstrip(".,;: ")
    return f"{trimmed}..."


def social_text_without_media_label(text: str) -> str:
    text = clean_text(text)
    text = re.sub(r"\s+(Video|Photo|GIF)\s*$", "", text, flags=re.IGNORECASE)
    return clean_text(text)


def handle_name(handle: str, lang: str) -> str:
    names = HANDLE_NAMES.get(handle.lower())
    if names:
        return names[0] if lang == "ru" else names[1]
    return f"@{handle}"


def hashtag_name(tag: str, lang: str) -> str:
    names = HASHTAG_NAMES.get(tag.lower())
    if names:
        return names[0] if lang == "ru" else names[1]
    return f"#{tag}"


def readable_social_text(text: str, lang: str = "en") -> str:
    text = social_text_without_media_label(text)
    text = re.sub(
        r"@([A-Za-z0-9_]+)",
        lambda match: handle_name(match.group(1), lang),
        text,
    )
    text = re.sub(
        r"#([A-Za-z0-9_]+)",
        lambda match: hashtag_name(match.group(1), lang),
        text,
    )
    text = re.sub(r"\[\s*", "[", text)
    text = re.sub(r"\s*\]", "]", text)
    return clean_text(text)


def social_title(title: str) -> str:
    title = re.sub(r"^UFC X:\s*", "", title or "", flags=re.IGNORECASE)
    return social_text_without_media_label(title)


def social_event(text: str) -> str:
    match = re.search(r"#(UFC[A-Za-z0-9_]+)", text or "")
    if match:
        return hashtag_name(match.group(1), "en")
    return "UFC"


def extract_broadcast_note(text: str) -> str:
    match = re.search(r"\[(.+?)\]", text or "")
    if not match:
        return ""
    note = readable_social_text(match.group(1), "en")
    note = note.replace("|", " | ")
    return clean_text(note)


def fallback_x_text(title: str, description: str) -> dict[str, Any]:
    source_text = social_text_without_media_label(description or social_title(title))
    title_text = social_title(title)
    readable_en = readable_social_text(source_text or title_text, "en")
    event = social_event(source_text or title_text)
    broadcast_note = extract_broadcast_note(source_text)

    erceg_match = re.search(r"@ErcegSteve|Steve Erceg", source_text, flags=re.IGNORECASE)
    faceoff_match = re.search(
        r"Facing off in Perth.*?(Jack Della Maddalena).*?(?:@thenightmare170|Carlos Prates)",
        source_text,
        flags=re.IGNORECASE,
    )

    if erceg_match:
        hook_ru = "Эрцег заряжен на домашний бой в Перте"
        hook_en = "Erceg targets a hometown UFC Perth win"
        short_ru = (
            f"Стив Эрцег готовится к домашнему выступлению на {event} в эти выходные. "
            "UFC показывает его настрой перед турниром в Перте и делает акцент на шанс порадовать своих болельщиков."
        )
        short_en = (
            f"Steve Erceg is preparing for a hometown win at {event} this weekend. "
            "UFC is pushing his Perth storyline as fight week builds toward the card."
        )
    elif faceoff_match:
        hook_ru = "Делла Маддалена и Пратес сошлись лицом к лицу"
        hook_en = "Della Maddalena and Prates face off in Perth"
        short_ru = (
            f"Джек Делла Маддалена и Карлос Пратес встретились лицом к лицу в Перте перед {event}. "
            "UFC подогревает интерес к их противостоянию и напоминает о ближайшем турнире."
        )
        short_en = (
            f"Jack Della Maddalena and Carlos Prates faced off in Perth ahead of {event}. "
            "UFC is building attention around their matchup as the event gets closer."
        )
    else:
        hook_en = compact_for_post(readable_en, 95)
        hook_ru = f"UFC подогревает интерес к {event}"
        short_ru = (
            f"Официальный аккаунт UFC выпустил обновление перед {event}: {readable_social_text(source_text, 'ru')}. "
            "Это промо к ближайшему турниру и одному из ключевых сюжетов недели."
        )
        short_en = readable_en

    if broadcast_note:
        short_ru = f"{short_ru} Детали эфира: {broadcast_note}."
        short_en = f"{short_en} Broadcast note: {broadcast_note}."

    full_ru = f"{short_ru}"
    full_en = f"{short_en}"


    return {
        "hook_ru": compact_for_post(hook_ru, 95),
        "hook_en": compact_for_post(hook_en, 95),
        "short_ru": compact_for_post(short_ru, 520),
        "short_en": compact_for_post(short_en, 520),
        "full_ru": compact_for_post(full_ru, 950),
        "full_en": compact_for_post(full_en, 950),
        "poll_question": "",
        "poll_options": [],
    }


def translate_fighters_ru(text: str) -> str:
    for en_name, ru_name in FIGHTER_RU.items():
        text = text.replace(en_name, ru_name)
    return text


def translate_weight_ru(text: str) -> str:
    for en_weight, ru_weight in WEIGHT_RU.items():
        text = text.replace(en_weight, ru_weight)
    return text


def translate_matchup_ru(text: str) -> str:
    text = translate_fighters_ru(text)
    text = translate_weight_ru(text)
    text = text.replace(" vs ", " против ")
    text = text.replace(" def. ", " победил ")
    text = text.replace("Decision - Unanimous", "единогласное решение")
    text = text.replace("Decision - Split", "раздельное решение")
    text = text.replace("KO/TKO", "нокаут/технический нокаут")
    text = text.replace("Submission", "сабмишен")
    return text


def format_upcoming_matchup_ru(line: str, include_weight: bool = True) -> str:
    line = clean_text(line)
    match = re.match(r"(.+?) vs (.+?)(?: \((.+)\))?$", line)
    if not match:
        return translate_matchup_ru(line)

    left, right, weight = match.groups()
    left_ru = FIGHTER_RU.get(left, left)
    right_ru = FIGHTER_RU_OBJECT.get(right, FIGHTER_RU.get(right, right))
    text = f"{left_ru} против {right_ru}"
    if include_weight and weight:
        text = f"{text} ({WEIGHT_RU.get(weight, weight)})"
    return text


def parse_ufc_event_summary(description: str) -> dict[str, Any]:
    text = clean_text(description)
    event_match = re.search(r"Official UFC (.*?): (.*?)\. Date/time:", text)
    date_match = re.search(r"Date/time: (.*?)\. Location:", text)
    location_match = re.search(r"Location: (.*?)(?:\. (?:Main card highlights|Top results):|$)", text)
    fights_match = re.search(r"(?:Main card highlights|Top results): (.*?)(?:\.$|$)", text)

    fights = []
    if fights_match:
        fights = [clean_text(part) for part in fights_match.group(1).split(";") if clean_text(part)]

    return {
        "status": event_match.group(1) if event_match else "",
        "event": event_match.group(2) if event_match else "",
        "date": date_match.group(1) if date_match else "",
        "location": location_match.group(1) if location_match else "",
        "fights": fights,
    }


def readable_ufc_date_ru(date_text: str) -> str:
    weekday_ru = {
        "Sat": "суббота",
        "Sun": "воскресенье",
        "Mon": "понедельник",
        "Tue": "вторник",
        "Wed": "среда",
        "Thu": "четверг",
        "Fri": "пятница",
    }
    month_ru = {
        "May": "мая",
        "Jun": "июня",
        "Jul": "июля",
    }
    match = re.match(r"\b([A-Z][a-z]{2}),\s+([A-Z][a-z]{2})\s+(\d{1,2})(.*)", date_text or "")
    if match:
        weekday, month, day, rest = match.groups()
        date_text = f"{weekday_ru.get(weekday, weekday)}, {day} {month_ru.get(month, month)}{rest}"

    replacements = {
        "Main Card": "главный кард",
    }
    for source, target in replacements.items():
        date_text = re.sub(rf"\b{re.escape(source)}\b", target, date_text)
    return date_text


def readable_ufc_location_ru(location: str) -> str:
    location = location.replace("Perth WA Australia", "Перт, Австралия")
    location = location.replace("Las Vegas , NV United States", "Лас-Вегас, США")
    location = location.replace("Newark , NJ United States", "Ньюарк, США")
    location = location.replace("Macao", "Макао")
    location = location.replace("Azerbaijan", "Азербайджан")
    return clean_text(location)


def readable_ufc_location_en(location: str) -> str:
    location = location.replace("RAC Arena Perth WA Australia", "RAC Arena, Perth, Australia")
    location = location.replace("Meta APEX Las Vegas , NV United States", "Meta APEX, Las Vegas, United States")
    location = location.replace("Prudential Center Newark , NJ United States", "Prudential Center, Newark, United States")
    return clean_text(location)


def first_fighters_from_event(event: str) -> tuple[str, str] | None:
    if " vs " not in event:
        return None
    left, right = event.split(" vs ", 1)
    return left.strip(), right.strip()


def format_fight_list(lines: list[str], lang: str, limit: int = 4) -> str:
    selected = lines[:limit]
    if not selected:
        return ""
    if lang == "ru":
        return "\n".join(f"{idx}. {format_upcoming_matchup_ru(line)}" for idx, line in enumerate(selected, 1))
    return "\n".join(f"{idx}. {line}" for idx, line in enumerate(selected, 1))


def fallback_ufc_event_text(title: str, description: str) -> dict[str, Any]:
    parsed = parse_ufc_event_summary(description)
    event = parsed["event"] or re.sub(r"^UFC .*?:\s*", "", title)
    date_text = parsed["date"]
    location = parsed["location"]
    fights = parsed["fights"]
    main_pair = first_fighters_from_event(event)

    event_ru = translate_matchup_ru(event)
    date_ru = readable_ufc_date_ru(date_text) if date_text else "дата уточняется"
    location_ru = readable_ufc_location_ru(location) if location else "локация уточняется"
    location_en = readable_ufc_location_en(location) if location else "TBA"
    fight_list_ru = format_fight_list(fights, "ru")
    fight_list_en = format_fight_list(fights, "en")
    main_fight_en = fights[0].split(" (", 1)[0] if fights else event
    main_fight_ru = format_upcoming_matchup_ru(main_fight_en, include_weight=False)

    if main_pair:
        hook_ru = f"{main_fight_ru}: UFC готовит бой в Перте"
        hook_en = f"{main_fight_en} headlines UFC Perth"
    else:
        hook_ru = f"{event_ru}: новый турнир UFC"
        hook_en = f"{event} leads the next UFC card"

    if fights:
        extra_ru = ", ".join(format_upcoming_matchup_ru(line.split(" (", 1)[0], include_weight=False) for line in fights[1:3])
        extra_en = ", ".join(line.split(" (", 1)[0] for line in fights[1:3])
    else:
        extra_ru = ""
        extra_en = ""

    short_ru = (
        f"UFC проведет турнир в {location_ru}: главный бой — {main_fight_ru}.\n\n"
        f"Дата: {date_ru}."
    )
    short_en = (
        f"UFC heads to {location_en} with {main_fight_en} on top of the card.\n\n"
        f"Date: {date_text or 'TBA'}."
    )
    if extra_ru:
        short_ru += f"\n\nВ карде также: {extra_ru}."
    if extra_en:
        short_en += f"\n\nAlso on the card: {extra_en}."

    full_ru = (
        f"UFC проведет турнир в {location_ru}. Главный бой вечера — {main_fight_ru}.\n\n"
        f"Дата и время: {date_ru}.\n"
        f"Место: {location_ru}.\n\n"
        f"Главные бои:\n{fight_list_ru or 'Кард уточняется.'}"
    )
    full_en = (
        f"UFC is heading to {location_en} with {main_fight_en} as the featured matchup.\n\n"
        f"Date/time: {date_text or 'TBA'}.\n"
        f"Venue: {location_en}.\n\n"
        f"Main card:\n{fight_list_en or 'The card is still being updated.'}"
    )

    return {
        "hook_ru": compact_for_post(hook_ru, 95),
        "hook_en": compact_for_post(hook_en, 95),
        "short_ru": compact_multiline(short_ru, 330),
        "short_en": compact_multiline(short_en, 330),
        "full_ru": compact_multiline(full_ru, 1600),
        "full_en": compact_multiline(full_en, 1600),
        "poll_question": "",
        "poll_options": [],
    }


def extract_x_video_url(link: str) -> str | None:
    if not link:
        return None
    try:
        from yt_dlp import YoutubeDL
    except Exception as e:
        log.warning("yt-dlp is not available; cannot extract X video: %s", e)
        return None

    try:
        with YoutubeDL({
            "quiet": True,
            "no_warnings": True,
            "skip_download": True,
            "format": "best[ext=mp4]/best",
        }) as ydl:
            info = ydl.extract_info(link, download=False)

        formats = [
            fmt for fmt in info.get("formats", [])
            if fmt.get("url")
            and fmt.get("ext") == "mp4"
            and ".mp4" in fmt.get("url", "")
            and ".m3u8" not in fmt.get("url", "")
        ]
        if formats:
            best = max(
                formats,
                key=lambda fmt: (
                    fmt.get("height") or 0,
                    fmt.get("tbr") or 0,
                    fmt.get("filesize") or fmt.get("filesize_approx") or 0,
                ),
            )
            return best["url"]

        direct_url = info.get("url")
        if direct_url and ".mp4" in direct_url:
            return direct_url
    except Exception as e:
        log.warning("Could not extract X video for %s: %s", link, e)

    return None


def largest_file_in_directory(directory: str) -> str | None:
    files = [
        os.path.join(directory, name)
        for name in os.listdir(directory)
        if os.path.isfile(os.path.join(directory, name))
    ]
    if not files:
        return None
    return max(files, key=lambda path: os.path.getsize(path))


def compress_video_for_telegram(video_path: str, output_dir: str) -> str | None:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        log.warning("ffmpeg is not installed; cannot compress oversized video.")
        return None

    compressed_path = os.path.join(output_dir, "compressed_for_telegram.mp4")
    command = [
        ffmpeg,
        "-y",
        "-i", video_path,
        "-vf", "scale='min(720,iw)':-2",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "30",
        "-c:a", "aac",
        "-b:a", "96k",
        "-movflags", "+faststart",
        compressed_path,
    ]

    try:
        subprocess.run(command, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        log.warning("ffmpeg compression failed: %s", e)
        return None

    if os.path.exists(compressed_path) and os.path.getsize(compressed_path) <= MAX_TELEGRAM_VIDEO_BYTES:
        return compressed_path

    if os.path.exists(compressed_path):
        log.warning(
            "Compressed video is still too large for Telegram Bot API: %.2f MB",
            os.path.getsize(compressed_path) / 1024 / 1024,
        )
    return None


def download_video_for_telegram(source_url: str, output_dir: str) -> str | None:
    if not source_url:
        return None

    try:
        from yt_dlp import YoutubeDL
    except Exception as e:
        log.warning("yt-dlp is not available; cannot download X video: %s", e)
        return None

    try:
        with YoutubeDL({
            "quiet": True,
            "no_warnings": True,
            "noprogress": True,
            "noplaylist": True,
            "format": "bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best",
            "merge_output_format": "mp4",
            "outtmpl": os.path.join(output_dir, "%(id)s.%(ext)s"),
        }) as ydl:
            ydl.download([source_url])
    except Exception as e:
        log.warning("yt-dlp download failed for %s: %s", source_url, e)
        return None

    video_path = largest_file_in_directory(output_dir)
    if not video_path:
        log.warning("yt-dlp finished but no video file was created.")
        return None

    size_bytes = os.path.getsize(video_path)
    if size_bytes <= MAX_TELEGRAM_VIDEO_BYTES:
        log.info("Downloaded X video: %s (%.2f MB)", video_path, size_bytes / 1024 / 1024)
        return video_path

    log.warning(
        "Downloaded video is too large for Telegram Bot API: %.2f MB. Trying compression.",
        size_bytes / 1024 / 1024,
    )
    return compress_video_for_telegram(video_path, output_dir)


def entry_datetime(entry: Any) -> datetime | None:
    if hasattr(entry, "published_parsed") and entry.published_parsed:
        return datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    if hasattr(entry, "updated_parsed") and entry.updated_parsed:
        return datetime(*entry.updated_parsed[:6], tzinfo=timezone.utc)
    return None


def is_recent(dt: datetime | None, now: datetime, hours: int) -> bool:
    return dt is None or now - dt <= timedelta(hours=hours)


def parse_ufc_event_datetime(date_text: str, now: datetime) -> datetime | None:
    date_match = re.search(r"\b([A-Z][a-z]{2}),\s+([A-Z][a-z]{2})\s+(\d{1,2})\b", date_text or "")
    if not date_match:
        return None

    _, month_name, day = date_match.groups()
    time_match = re.search(r"\b(\d{1,2}):(\d{2})\s*(AM|PM)\b", date_text or "", flags=re.IGNORECASE)
    hour = 12
    minute = 0
    if time_match:
        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        meridiem = time_match.group(3).upper()
        if meridiem == "PM" and hour != 12:
            hour += 12
        elif meridiem == "AM" and hour == 12:
            hour = 0

    try:
        parsed = datetime.strptime(
            f"{month_name} {int(day)} {now.year} {hour}:{minute}",
            "%b %d %Y %H:%M",
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None

    if parsed < now - timedelta(days=180):
        parsed = parsed.replace(year=parsed.year + 1)
    elif parsed > now + timedelta(days=180):
        parsed = parsed.replace(year=parsed.year - 1)
    return parsed


def ufc_event_status(card_text: str, event_dt: datetime | None, now: datetime) -> str:
    lowered = card_text.lower()
    if "watch replay" in lowered or "results" in lowered:
        return "completed"
    if event_dt and event_dt.date() == now.date():
        return "live"
    if event_dt and event_dt > now:
        return "upcoming"
    return "completed"


def extract_image(entry):
    if 'media_content' in entry and len(entry.media_content) > 0:
        return entry.media_content[0]['url']
    if 'links' in entry:
        for link in entry.links:
            if link.get('type', '').startswith('image/') or link.get('rel') == 'enclosure':
                return link.get('href')
    if 'summary' in entry:
        image = first_image_from_html(entry.summary)
        if image:
            return image
    if 'content' in entry and len(entry.content) > 0:
        image = first_image_from_html(entry.content[0].value)
        if image:
            return image
            
    # Fallback to fetching original link to find og:image (Sherdog etc)
    try:
        link = getattr(entry, "link", None)
        if link:
            res = requests.get(link, timeout=5, headers=HTTP_HEADERS)
            if res.status_code == 200:
                soup = BeautifulSoup(res.text, 'html.parser')
                og = soup.find("meta", property="og:image")
                if og and og.get("content"):
                    return og.get("content")
    except Exception:
        pass
        
    return None


def build_rss_candidate(feed_info: dict[str, str], entry: Any) -> dict[str, Any] | None:
    link = getattr(entry, "link", None)
    if not link:
        return None

    title = clean_text(getattr(entry, "title", "No Title"))
    summary_html = getattr(entry, "summary", "")
    return {
        "id": canonical_post_id(link),
        "title": title,
        "summary_text": html_to_text(summary_html, 900),
        "link": link,
        "image": extract_image(entry),
        "source": feed_info["name"],
        "tag": feed_info["tag"],
        "lang": feed_info.get("lang", "en"),
        "published_at": entry_datetime(entry),
    }


def find_rss_candidate(posted: set[str], now: datetime) -> dict[str, Any] | None:
    feeds_copy = RSS_FEEDS.copy()
    random.shuffle(feeds_copy)
    log.info("Checking %s RSS feeds for new articles...", len(feeds_copy))

    for feed_info in feeds_copy:
        try:
            feed = feedparser.parse(feed_info["url"])
            entries = getattr(feed, "entries", [])
            if not entries:
                log.info("Feed %s: no entries.", feed_info["name"])
                continue

            entries_to_check = entries[:MAX_ENTRIES_PER_FEED]
            log.info(
                "Feed %s: found %s total entries, checking top %s",
                feed_info["name"],
                len(entries),
                len(entries_to_check),
            )

            for entry in entries_to_check:
                candidate = build_rss_candidate(feed_info, entry)
                if not candidate:
                    continue
                if candidate["id"] in posted:
                    continue
                published_at = candidate.get("published_at")
                if not is_recent(published_at, now, RECENT_ARTICLE_HOURS):
                    log.info("Skipping '%s' - too old (%s)", candidate["title"], published_at)
                    continue
                return candidate
        except Exception as e:
            log.warning("Failed RSS %s: %s", feed_info["name"], e)

    return None


def find_x_social_candidate(posted: set[str], now: datetime) -> dict[str, Any] | None:
    if not X_RSS_FEED_URLS:
        return None

    for feed_url in X_RSS_FEED_URLS:
        try:
            feed = feedparser.parse(feed_url)
            entries = getattr(feed, "entries", [])
            log.info("X feed %s: found %s entries.", feed_url, len(entries))

            for entry in entries[:MAX_ENTRIES_PER_FEED]:
                raw_link = getattr(entry, "link", "")
                link = normalize_x_link(raw_link)
                if not link:
                    continue
                post_id = canonical_post_id(link)
                if post_id in posted:
                    continue

                title = clean_text(getattr(entry, "title", "UFC update from X"))
                summary_text = html_to_text(getattr(entry, "summary", ""), 900) or title
                if "rss reader not yet whitelisted" in summary_text.lower():
                    log.warning("X RSS bridge %s is not usable: %s", feed_url, title)
                    continue

                published_at = entry_datetime(entry)
                if not is_recent(published_at, now, X_POST_LOOKBACK_HOURS):
                    continue

                has_video = "video" in getattr(entry, "summary", "").lower()

                return {
                    "id": post_id,
                    "title": f"UFC X: {title[:120]}",
                    "summary_text": summary_text,
                    "link": link,
                    "image": extract_image(entry),
                    "video_source": link if has_video else None,
                    "source": "UFC X",
                    "tag": "#ufc #mma",
                    "lang": "en",
                    "published_at": published_at,
                }
        except Exception as e:
            log.warning("Failed X RSS %s: %s", feed_url, e)

    return None


def node_text(root: Any, selector: str) -> str:
    node = root.select_one(selector)
    return clean_text(node.get_text(" ", strip=True)) if node else ""


def extract_ufc_fights(event_url: str, limit: int = 5) -> list[dict[str, str]]:
    page = fetch_html(event_url)
    if not page:
        return []

    soup = BeautifulSoup(page, "html.parser")
    fights = []
    for fight in soup.select(".c-listing-fight")[:limit]:
        red_name = node_text(fight, ".c-listing-fight__corner-name--red")
        blue_name = node_text(fight, ".c-listing-fight__corner-name--blue")
        red_outcome = node_text(fight, ".c-listing-fight__corner--red .c-listing-fight__outcome-wrapper")
        blue_outcome = node_text(fight, ".c-listing-fight__corner--blue .c-listing-fight__outcome-wrapper")
        method = node_text(fight, ".c-listing-fight__results--desktop .method")
        round_no = node_text(fight, ".c-listing-fight__results--desktop .round")
        weight_class = node_text(fight, ".c-listing-fight__class--desktop .c-listing-fight__class-text")

        if not red_name or not blue_name:
            continue
        fights.append({
            "red_name": red_name,
            "blue_name": blue_name,
            "red_outcome": red_outcome,
            "blue_outcome": blue_outcome,
            "method": method,
            "round": round_no,
            "weight_class": weight_class,
        })

    return fights


def format_ufc_fight_summary(fight: dict[str, str], status: str) -> str:
    red_name = fight["red_name"]
    blue_name = fight["blue_name"]
    red_outcome = fight.get("red_outcome", "")
    blue_outcome = fight.get("blue_outcome", "")
    method = fight.get("method", "")
    round_no = fight.get("round", "")
    weight_class = fight.get("weight_class", "")

    if status == "completed":
        winner = red_name if red_outcome.lower() == "win" else blue_name if blue_outcome.lower() == "win" else ""
        result = f"{winner} def. {blue_name if winner == red_name else red_name}" if winner else f"{red_name} vs {blue_name}"
        details = ", ".join(part for part in [method, f"R{round_no}" if round_no else ""] if part)
        return f"{result} ({details})" if details else result

    matchup = f"{red_name} vs {blue_name}"
    return f"{matchup} ({weight_class})" if weight_class else matchup


def extract_ufc_fight_summaries(event_url: str, status: str, limit: int = 5) -> list[str]:
    return [
        format_ufc_fight_summary(fight, status)
        for fight in extract_ufc_fights(event_url, limit)
    ]


def extract_ufc_event_candidates(page_html: str, posted: set[str], now: datetime) -> list[dict[str, Any]]:
    soup = BeautifulSoup(page_html, "html.parser")
    candidates = []

    for card in soup.select("article.c-card-event--result"):
        title_node = card.select_one(".c-card-event--result__headline")
        date_node = card.select_one(".c-card-event--result__date")
        location_node = card.select_one(".c-card-event--result__location")
        link_node = card.select_one(".c-card-event--result__headline a[href], a[href]")
        if not title_node or not date_node or not link_node:
            continue

        title = clean_text(title_node.get_text(" ", strip=True))
        date_text = clean_text(date_node.get_text(" ", strip=True))
        location = clean_text(location_node.get_text(" ", strip=True)) if location_node else ""
        link = urljoin(UFC_EVENTS_URL, link_node.get("href"))
        card_text = clean_text(card.get_text(" ", strip=True))
        event_dt = parse_ufc_event_datetime(date_text, now)
        status = ufc_event_status(card_text, event_dt, now)
        candidate_id = canonical_post_id(f"ufc-event:{status}:{link}")
        if candidate_id in posted:
            continue

        if status == "upcoming" and event_dt and event_dt - now > timedelta(days=UFC_EVENT_LOOKAHEAD_DAYS):
            continue
        if status == "completed" and event_dt and now - event_dt > timedelta(hours=UFC_COMPLETED_LOOKBACK_HOURS):
            continue

        image_url = None
        img = card.find("img")
        if img and img.get("src"):
            image_url = urljoin(UFC_EVENTS_URL, img.get("src"))

        fight_lines = extract_ufc_fight_summaries(link, status, limit=5)
        event_image = extract_ufc_event_image(link)
        if event_image:
            image_url = event_image
        status_label = {
            "upcoming": "upcoming event",
            "live": "live/today event",
            "completed": "completed event",
        }.get(status, status)
        fight_text = ""
        if fight_lines:
            label = "Top results" if status == "completed" else "Main card highlights"
            fight_text = f" {label}: {'; '.join(fight_lines)}."
        summary_text = (
            f"Official UFC {status_label}: {title}. "
            f"Date/time: {date_text}. "
            f"Location: {location or 'TBA'}.{fight_text}"
        )

        candidates.append({
            "id": candidate_id,
            "title": f"UFC {status_label}: {title}",
            "summary_text": summary_text,
            "link": link,
            "image": image_url,
            "source": "UFC Events",
            "tag": "#ufc #mma",
            "lang": "en",
            "published_at": event_dt,
        })

    return candidates


def find_ufc_event_candidate(posted: set[str], now: datetime) -> dict[str, Any] | None:
    page = fetch_html(UFC_EVENTS_URL)
    if not page:
        return None

    candidates = extract_ufc_event_candidates(page, posted, now)
    log.info("UFC Events: found %s publishable candidates.", len(candidates))
    return candidates[0] if candidates else None


def find_selected_article(posted_list: list[str]) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    posted = set(normalize_posted_history(posted_list))

    for source_name, fetcher in (
        ("UFC X", find_x_social_candidate),
        ("UFC Events", find_ufc_event_candidate),
        ("RSS", find_rss_candidate),
    ):
        candidate = fetcher(posted, now)
        if candidate:
            log.info("Selected %s candidate: %s", source_name, candidate["title"])
            return candidate

    return None


def find_run_candidates(posted_list: list[str]) -> list[dict[str, Any]]:
    now = datetime.now(timezone.utc)
    posted = set(normalize_posted_history(posted_list))
    candidates = []

    source_plan = (
        ("UFC X", find_x_social_candidate, MAX_X_POSTS_PER_RUN),
        ("UFC Events", find_ufc_event_candidate, MAX_UFC_EVENT_POSTS_PER_RUN),
        ("RSS", find_rss_candidate, MAX_RSS_POSTS_PER_RUN),
    )

    for source_name, fetcher, source_limit in source_plan:
        for _ in range(max(0, source_limit)):
            if len(candidates) >= MAX_POSTS_PER_RUN:
                return candidates

            candidate = fetcher(posted, now)
            if not candidate:
                break

            candidates.append(candidate)
            posted.add(canonical_post_id(candidate["id"]))
            log.info("Selected %s candidate: %s", source_name, candidate["title"])

    return candidates


def clean_rss_article_text(text: str) -> str:
    text = html.unescape(clean_text(text))
    text = re.sub(r"\bThe post .+? (?:appeared first|first appeared) on .+?\.?$", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bRead more\.?$", "", text, flags=re.IGNORECASE)
    return clean_text(text).strip(" .")


def fallback_cage_warriors_text(title: str, description: str) -> dict[str, Any]:
    combined = f"{title} {description}"
    event_match = re.search(r"\b(Cage Warriors\s+\d+)\b", combined, flags=re.IGNORECASE)
    winner_match = re.search(r":\s*([A-Z][A-Za-z' .-]+?)\s+Wins Main Event", title, flags=re.IGNORECASE)
    event = event_match.group(1) if event_match else "Cage Warriors"
    winner_en = clean_text(winner_match.group(1)) if winner_match else "Stephen"
    winner_ru = {"Stephen": "Стивен"}.get(winner_en, winner_en)

    date_ru = "25 апреля"
    date_en = "April 25"
    location_ru = "Глазго, Шотландия"
    location_en = "Glasgow, Scotland"

    hook_ru = f"Результаты {event}: {winner_ru} забрал главный бой"
    hook_en = f"{event}: {winner_en} wins the main event"
    short_ru = (
        f"{event} прошел {date_ru} в {location_ru}.\n\n"
        f"Главное: {winner_ru} выиграл главный бой, а кард перед турниром пришлось менять."
    )
    short_en = (
        f"{event} took place on {date_en} in {location_en}.\n\n"
        f"{winner_en} won the main event after late changes to the card."
    )
    full_ru = (
        f"{event} прошел {date_ru} в {location_ru}.\n\n"
        f"Главный итог вечера: {winner_ru} выиграл мейн-ивент. По материалу Combat Press, "
        "перед турниром были перестановки в карде, поэтому финальные результаты важны еще и как фиксация обновленного состава боев.\n\n"
        "Коротко для Prime-Fist: турнир состоялся, главный бой закрыт победой Стивена, а изменения карда стали отдельным контекстом этого вечера."
    )
    full_en = (
        f"{event} took place on {date_en} in {location_en}.\n\n"
        f"The main takeaway: {winner_en} won the headline bout. Combat Press framed the results around a late card shuffle, "
        "so the final lineup matters as part of the story, not just the winner list.\n\n"
        "For Prime-Fist readers: the event is done, the main event has a winner, and the changed card shaped the night."
    )

    return {
        "hook_ru": compact_for_post(hook_ru, 95),
        "hook_en": compact_for_post(hook_en, 95),
        "short_ru": compact_multiline(short_ru, 330),
        "short_en": compact_multiline(short_en, 330),
        "full_ru": compact_multiline(full_ru, 1600),
        "full_en": compact_multiline(full_en, 1600),
        "poll_question": "",
        "poll_options": [],
    }


def russian_topic_from_title(title: str, description: str) -> str:
    combined = f"{title} {description}".lower()
    if "ufc" in combined:
        return "UFC"
    if "mma" in combined:
        return "MMA"
    if "boxing" in combined or "boxer" in combined:
        return "боксе"
    if "kickboxing" in combined or "k-1" in combined:
        return "кикбоксинге"
    return "единоборствах"


def fallback_rss_text(title: str, description: str, lang: str) -> dict[str, Any]:
    cleaned = clean_rss_article_text(description or title)
    title_text = clean_rss_article_text(title)
    lower = f"{title_text} {cleaned}".lower()

    if "cage warriors 205" in lower and "main event" in lower:
        return fallback_cage_warriors_text(title_text, cleaned)

    if lang == "ru":
        hook_ru = translate_matchup_ru(title_text)
        short_ru = compact_multiline(cleaned or title_text, 330)
        full_ru = compact_multiline(cleaned or title_text, 1600)
        hook_en = compact_for_post(title_text, 95)
        short_en = compact_multiline(cleaned or title_text, 330)
        full_en = compact_multiline(cleaned or title_text, 1600)
    else:
        hook_ru = translate_matchup_ru(title_text)
        hook_en = compact_for_post(title_text, 95)
        short_ru = compact_multiline(cleaned or title_text, 330)
        short_en = compact_multiline(cleaned or title_text, 330)
        full_ru = compact_multiline(cleaned or title_text, 1600)
        full_en = compact_multiline(cleaned or title_text, 1600)


    return {
        "hook_ru": compact_for_post(hook_ru, 95),
        "hook_en": compact_for_post(hook_en, 95),
        "short_ru": compact_multiline(short_ru, 330),
        "short_en": compact_multiline(short_en, 330),
        "full_ru": compact_multiline(full_ru, 1600),
        "full_en": compact_multiline(full_en, 1600),
        "poll_question": "",
        "poll_options": [],
    }


def fallback_primefist_text(title: str, description: str, lang: str) -> dict[str, Any]:
    if title.lower().startswith("ufc x:"):
        return fallback_x_text(title, description)
    if title.lower().startswith("ufc ") and "official ufc" in (description or "").lower():
        return fallback_ufc_event_text(title, description)

    return fallback_rss_text(title, description, lang)


async def generate_primefist_text(title, description, lang):
    if not GROQ_API_KEY:
        log.warning("GROQ_API_KEY is not configured; using fallback post text.")
        return fallback_primefist_text(title, description, lang)

    client = AsyncGroq(api_key=GROQ_API_KEY)
    prompt = f"""Ты редактор Telegram-канала PRIMEFIST (единоборства: MMA, бокс, K1, кикбокс, тай бокс).

Новость:
Заголовок: {title}
Описание: {description}
Язык источника: {lang}

ВАЖНО: Пиши только про саму новость. НЕ используй фразы типа "Источник сообщил", "Суть новости вынесена", "Текст очищен", "В единоборствах появилась новая тема" или любые другие вводные слова о процессе обработки новости. Пиши сразу по существу.

Верни ТОЛЬКО валидный JSON без markdown и без пояснений:

{{
  "hook_ru": "Дерзкий байтерский заголовок RU БЕЗ эмодзи. Макс 12 слов. Интрига, провокация.",
  "hook_en": "Same bait headline EN БЕЗ эмодзи. Max 12 words. Punchy and bold.",
  "short_ru": "2-3 предложения анонса на русском. Только факты. Используй двойной перенос строки \n\n для разделения мыслей.",
  "short_en": "2-3 sentence teaser in English. Facts only. Use double newlines \n\n for better readability.",
  "full_ru": "Полный рерайт новости на русском. 4-6 предложений. Обязательно разделяй текст на 2-3 абзаца с помощью \n\n.",
  "full_en": "Full rewrite in English. 4-6 sentences. Must use paragraph breaks (\n\n) to separate ideas.",
  "poll_question": "Увлекательный вопрос для опроса на русском (например: Кто победит?). Если новость не подходит для опроса, оставь пустую строку.",
  "poll_options": ["Вариант 1", "Вариант 2", "Вариант 3"] // Массив строк с вариантами ответов (не более 4). Если опрос не нужен, оставь пустой массив []
}}"""

    try:
        response = await client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=600
        )
        raw = response.choices[0].message.content.strip()
        raw = re.sub(r"```json|```", "", raw).strip()
        data = json.loads(raw)
        required = {"hook_ru", "hook_en", "short_ru", "short_en", "full_ru", "full_en"}
        if not required.issubset(data):
            missing = ", ".join(sorted(required - set(data)))
            raise ValueError(f"Groq response missing required fields: {missing}")
        return data
    except Exception as e:
        log.error(f"Groq API error: {e}")
        log.warning("Using fallback post text because Groq generation failed.")
        return fallback_primefist_text(title, description, lang)

def channel_post(ai_data: dict, source: str, link: str) -> str:
    """Короткий пост для канала."""
    hook_ru = compact_multiline(ai_data["hook_ru"], 95)
    hook_en = compact_multiline(ai_data["hook_en"], 95)
    short_ru = compact_multiline(ai_data["short_ru"], 260)
    short_en = compact_multiline(ai_data["short_en"], 260)
    return (
        f"<b>🥊 {html.escape(hook_ru)} / {html.escape(hook_en)}</b>\n\n"
        f"🇷🇺 {html.escape(short_ru)}\n\n"
        f"🇬🇧 {html.escape(short_en)}\n\n"
        f"🔗 <a href=\"{link}\">{html.escape(source)} (Link)</a>\n\n"
        f"💬 Подробнее в комментариях\n\n"
        f"#mma #boxing #kickboxing #primefist #fightnews"
    )


def discussion_post(ai_data: dict, source: str, tag: str, link: str) -> str:
    """Полный пост для комментариев."""
    return (
        f"<b>🥊 {html.escape(ai_data['hook_ru'])} / {html.escape(ai_data['hook_en'])}</b>\n\n"
        f"🇷🇺 {html.escape(ai_data['full_ru'])}\n\n"
        f"───────────────────\n\n"
        f"🇬🇧 {html.escape(ai_data['full_en'])}\n\n"
        f"🔗 <a href=\"{link}\">Link</a>\n"
        f"📌 Source: {html.escape(source)}\n\n"
        f"{html.escape(tag)} #primefist"
    )

def parse_chat_id(value: str) -> int | str:
    try:
        return int(value)
    except (TypeError, ValueError):
        return value

def field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)

def latest_update_id(updates) -> int | None:
    ids = []
    for update in updates or []:
        update_id = field(update, "update_id")
        if isinstance(update_id, int):
            ids.append(update_id)
    return max(ids) if ids else None

def same_chat_id(left: Any, right: Any) -> bool:
    return str(left) == str(right)

def same_channel(chat: Any, channel_id: str) -> bool:
    if not chat:
        return False
    chat_id = field(chat, "id")
    if same_chat_id(chat_id, channel_id):
        return True
    username = field(chat, "username")
    if channel_id.startswith("@") and isinstance(username, str):
        return f"@{username}".lower() == channel_id.lower()
    return False

def legacy_forward_message_id(message: Any) -> Any:
    if isinstance(message, dict):
        return message.get("forward_from_message_id")
    if hasattr(message, "to_dict"):
        return message.to_dict().get("forward_from_message_id")
    return getattr(message, "forward_from_message_id", None)

def is_discussion_forward(update: Any, discussion_chat_id: int | str, channel_id: str, post_message_id: int) -> bool:
    message = field(update, "message")
    chat = field(message, "chat")
    if not message or not same_chat_id(field(chat, "id"), discussion_chat_id):
        return False

    origin = field(message, "forward_origin")
    origin_message_id = field(origin, "message_id")
    if origin_message_id is None:
        origin_message_id = legacy_forward_message_id(message)
    try:
        origin_message_id = int(origin_message_id)
    except (TypeError, ValueError):
        return False
    if origin_message_id != post_message_id:
        return False

    origin_chat = field(origin, "chat") or field(message, "forward_from_chat") or field(message, "sender_chat")
    if not same_channel(origin_chat, channel_id):
        return False

    return field(message, "is_automatic_forward") is True or same_channel(field(message, "sender_chat"), channel_id)

async def resolve_discussion_chat_id(bot: Bot, channel_id: str) -> int | str | None:
    if TELEGRAM_DISCUSSION_CHAT_ID:
        return parse_chat_id(TELEGRAM_DISCUSSION_CHAT_ID)

    try:
        chat = await bot.get_chat(channel_id)
        linked_chat_id = field(chat, "linked_chat_id")
        return int(linked_chat_id) if linked_chat_id else None
    except Exception as e:
        log.warning(f"Could not resolve linked discussion chat: {e}")
        return None

async def prepare_discussion_update_offset(bot: Bot) -> int | None:
    global telegram_update_offset

    try:
        updates = await bot.get_updates(
            offset=-1,
            limit=1,
            timeout=0,
            allowed_updates=["message"]
        )
        update_id = latest_update_id(updates)
        if update_id is not None:
            telegram_update_offset = update_id + 1
    except Exception as e:
        log.warning(f"Could not prepare discussion update offset: {e}")

    return telegram_update_offset

async def find_discussion_forward_message_id(
    bot: Bot,
    discussion_chat_id: int | str,
    channel_id: str,
    post_message_id: int,
    update_offset: int | None = None
) -> int | None:
    global telegram_update_offset

    next_offset = update_offset if update_offset is not None else telegram_update_offset
    deadline = asyncio.get_running_loop().time() + DISCUSSION_FORWARD_WAIT_SECONDS

    while asyncio.get_running_loop().time() <= deadline:
        payload = {
            "limit": 100,
            "timeout": 1,
            "allowed_updates": ["message"]
        }
        if next_offset is not None:
            payload["offset"] = next_offset

        updates = await bot.get_updates(**payload)
        update_id = latest_update_id(updates)
        if update_id is not None:
            next_offset = update_id + 1
            telegram_update_offset = next_offset

        for update in updates:
            if is_discussion_forward(update, discussion_chat_id, channel_id, post_message_id):
                return field(field(update, "message"), "message_id")

        await asyncio.sleep(DISCUSSION_FORWARD_POLL_SECONDS)

    return None

async def send_discussion_reply(bot: Bot, discussion_chat_id: int | str, discussion_message_id: int, text: str):
    return await bot.send_message(
        chat_id=discussion_chat_id,
        text=text,
        disable_web_page_preview=True,
        parse_mode=ParseMode.HTML,
        reply_parameters=ReplyParameters(
            message_id=discussion_message_id,
            allow_sending_without_reply=False
        )
    )

async def send_discussion_poll(
    bot: Bot,
    discussion_chat_id: int | str,
    discussion_message_id: int,
    question: str,
    options: list[str]
):
    return await bot.send_poll(
        chat_id=discussion_chat_id,
        question=question,
        options=options[:4],
        is_anonymous=True,
        allows_multiple_answers=False,
        reply_parameters=ReplyParameters(
            message_id=discussion_message_id,
            allow_sending_without_reply=False
        )
    )


async def send_channel_post(
    bot: Bot,
    channel_id: str,
    image_url: str | None,
    text: str,
    video_source: str | None = None,
):
    if video_source and len(text) <= 1024:
        with tempfile.TemporaryDirectory() as tmpdir:
            video_path = await asyncio.to_thread(download_video_for_telegram, video_source, tmpdir)
            if video_path:
                try:
                    with open(video_path, "rb") as video_file:
                        return await bot.send_video(
                            chat_id=channel_id,
                            video=video_file,
                            caption=text,
                            parse_mode=ParseMode.HTML,
                            supports_streaming=True,
                        )
                except Exception as e:
                    log.warning("Video upload failed, falling back to photo/text: %s", e)
            else:
                log.warning("Video download failed or exceeded Telegram limit; falling back to photo/text.")
    elif video_source:
        log.info("Caption is longer than Telegram video limit; sending photo/text post instead.")

    if image_url and len(text) <= 1024:
        try:
            return await bot.send_photo(
                chat_id=channel_id,
                photo=image_url,
                caption=text,
                parse_mode=ParseMode.HTML
            )
        except Exception as e:
            log.warning("Photo post failed, falling back to text message: %s", e)
    elif image_url:
        log.info("Caption is longer than Telegram photo limit; sending text post instead.")

    return await bot.send_message(
        chat_id=channel_id,
        text=text,
        disable_web_page_preview=False,
        parse_mode=ParseMode.HTML
    )


async def resolve_discussion_message_id_for_post(
    bot: Bot,
    channel_id: str,
    sent_post: Any,
    discussion_chat_id: int | str | None,
    update_offset: int | None = None
) -> int | None:
    post_message_id = field(sent_post, "message_id")
    if not post_message_id:
        return None
    if not discussion_chat_id:
        log.warning("Linked discussion chat is not configured; skipping discussion replies.")
        return None

    discussion_message_id = await find_discussion_forward_message_id(
        bot,
        discussion_chat_id,
        channel_id,
        post_message_id,
        update_offset
    )
    if not discussion_message_id:
        log.warning(
            "Discussion forwarded post not found; skipping continuation comment "
            f"(channel={channel_id}, discussion_chat={discussion_chat_id}, post={post_message_id})."
        )
    return discussion_message_id

async def send_continuation_comment(
    bot: Bot,
    channel_id: str,
    sent_post: Any,
    text: str,
    discussion_chat_id: int | str | None,
    update_offset: int | None = None
) -> int:
    if not text.strip():
        return 0

    discussion_message_id = await resolve_discussion_message_id_for_post(
        bot,
        channel_id,
        sent_post,
        discussion_chat_id,
        update_offset
    )
    if not discussion_message_id:
        return 0

    await send_discussion_reply(bot, discussion_chat_id, discussion_message_id, text)
    return 1

# ==========================================
# MAIN
# ==========================================
async def publish_article(bot: Bot, posted: list[str], selected_article: dict[str, Any]) -> None:
    log.info(f"Selected article: {selected_article['title']}")
    ai_data = await generate_primefist_text(selected_article['title'], selected_article['summary_text'], selected_article['lang'])

    if not ai_data:
        raise RuntimeError("Failed to generate content with Groq.")

    ch_text = channel_post(ai_data, selected_article["source"], selected_article["link"])
    disc_text = discussion_post(ai_data, selected_article["source"], selected_article["tag"], selected_article["link"])

    try:
        discussion_chat_id = await resolve_discussion_chat_id(bot, CHANNEL_ID)
        discussion_update_offset = (
            await prepare_discussion_update_offset(bot)
            if discussion_chat_id
            else None
        )

        msg = await send_channel_post(
            bot,
            CHANNEL_ID,
            selected_article.get("image"),
            ch_text,
            selected_article.get("video_source"),
        )

        log.info("Successfully posted Main to Telegram!")

        discussion_message_id = None
        try:
            discussion_message_id = await resolve_discussion_message_id_for_post(
                bot,
                CHANNEL_ID,
                msg,
                discussion_chat_id,
                discussion_update_offset
            )
        except Exception as e:
            log.error(f"Failed to find discussion thread: {e}")

        poll_q = ai_data.get("poll_question", "").strip()
        poll_opts = ai_data.get("poll_options", [])
        if poll_q and isinstance(poll_opts, list) and len(poll_opts) >= 2:
            if discussion_chat_id and discussion_message_id:
                try:
                    await asyncio.sleep(1)
                    await send_discussion_poll(bot, discussion_chat_id, discussion_message_id, poll_q, poll_opts)
                    log.info("Successfully posted Poll to comments!")
                except Exception as e:
                    log.error(f"Poll error: {e}")
            else:
                log.warning("Poll skipped because discussion thread was not found.")

        if discussion_chat_id and discussion_message_id:
            try:
                await asyncio.sleep(1)
                await send_discussion_reply(bot, discussion_chat_id, discussion_message_id, disc_text)
                log.info("Successfully posted comment (full story)!")
            except Exception as e:
                log.error(f"Failed to post comment: {e}")

        posted.append(canonical_post_id(selected_article["id"]))
        save_posted(posted)
    except Exception as e:
        log.error(f"Failed to post Main to Telegram: {e}")
        raise


async def main():
    # Create empty posted.json if it doesn't exist to prevent git push errors
    if not os.path.exists(POSTED_FILE):
        save_posted([])

    missing = [
        name for name, value in {
            "TELEGRAM_BOT_TOKEN": BOT_TOKEN,
            "CHANNEL_ID": CHANNEL_ID,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing GitHub secrets/env vars: {', '.join(missing)}")

    posted = load_posted()
    bot = Bot(token=BOT_TOKEN)
    selected_articles = find_run_candidates(posted)

    if not selected_articles:
        log.info("Finished checking all feeds. No unposted/recent articles found.")
        return

    log.info("Publishing %s balanced candidates this run.", len(selected_articles))
    for index, selected_article in enumerate(selected_articles, start=1):
        await publish_article(bot, posted, selected_article)
        if index < len(selected_articles) and POST_SPACING_SECONDS > 0:
            log.info("Waiting %s seconds before next post.", POST_SPACING_SECONDS)
            await asyncio.sleep(POST_SPACING_SECONDS)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log.exception("Bot run failed: %s", e)
        sys.exit(1)
