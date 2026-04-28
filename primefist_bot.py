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
from typing import Any
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, timezone
from telegram import Bot, ReplyParameters
from telegram.constants import ParseMode
from groq import AsyncGroq
from urllib.parse import urljoin

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
UFC_EVENTS_URL = os.environ.get("UFC_EVENTS_URL", "https://www.ufc.com/events").strip()
HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
telegram_update_offset = None

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
def load_posted():
    if os.path.exists(POSTED_FILE):
        try:
            with open(POSTED_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_posted(posted_list):
    posted_list = posted_list[-MAX_HISTORY:]
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


def normalize_x_link(link: str) -> str:
    match = re.search(r"/ufc/status/(\d+)", link or "", flags=re.IGNORECASE)
    if match:
        return f"https://x.com/ufc/status/{match.group(1)}"
    return link


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
        "id": link,
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
                if link in posted:
                    continue

                title = clean_text(getattr(entry, "title", "UFC update from X"))
                summary_text = html_to_text(getattr(entry, "summary", ""), 900) or title
                if "rss reader not yet whitelisted" in summary_text.lower():
                    log.warning("X RSS bridge %s is not usable: %s", feed_url, title)
                    continue

                published_at = entry_datetime(entry)
                if not is_recent(published_at, now, X_POST_LOOKBACK_HOURS):
                    continue

                return {
                    "id": link,
                    "title": f"UFC X: {title[:120]}",
                    "summary_text": summary_text,
                    "link": link,
                    "image": extract_image(entry),
                    "source": "UFC X",
                    "tag": "#ufc #mma",
                    "lang": "en",
                    "published_at": published_at,
                }
        except Exception as e:
            log.warning("Failed X RSS %s: %s", feed_url, e)

    return None


def extract_ufc_results(event_url: str, limit: int = 5) -> list[str]:
    page = fetch_html(event_url)
    if not page:
        return []

    soup = BeautifulSoup(page, "html.parser")
    results = []
    for fight in soup.select(".c-listing-fight")[:limit]:
        red_name = clean_text(fight.select_one(".c-listing-fight__corner-name--red").get_text(" ", strip=True)) if fight.select_one(".c-listing-fight__corner-name--red") else ""
        blue_name = clean_text(fight.select_one(".c-listing-fight__corner-name--blue").get_text(" ", strip=True)) if fight.select_one(".c-listing-fight__corner-name--blue") else ""
        red_outcome = clean_text(fight.select_one(".c-listing-fight__corner--red .c-listing-fight__outcome-wrapper").get_text(" ", strip=True)) if fight.select_one(".c-listing-fight__corner--red .c-listing-fight__outcome-wrapper") else ""
        blue_outcome = clean_text(fight.select_one(".c-listing-fight__corner--blue .c-listing-fight__outcome-wrapper").get_text(" ", strip=True)) if fight.select_one(".c-listing-fight__corner--blue .c-listing-fight__outcome-wrapper") else ""
        method = clean_text(fight.select_one(".c-listing-fight__results--desktop .method").get_text(" ", strip=True)) if fight.select_one(".c-listing-fight__results--desktop .method") else ""
        round_no = clean_text(fight.select_one(".c-listing-fight__results--desktop .round").get_text(" ", strip=True)) if fight.select_one(".c-listing-fight__results--desktop .round") else ""

        if not red_name or not blue_name:
            continue
        winner = red_name if red_outcome.lower() == "win" else blue_name if blue_outcome.lower() == "win" else ""
        result = f"{winner} def. {blue_name if winner == red_name else red_name}" if winner else f"{red_name} vs {blue_name}"
        details = ", ".join(part for part in [method, f"R{round_no}" if round_no else ""] if part)
        results.append(f"{result} ({details})" if details else result)

    return results


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
        candidate_id = f"ufc-event:{status}:{link}"
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

        result_lines = extract_ufc_results(link) if status == "completed" else []
        status_label = {
            "upcoming": "upcoming event",
            "live": "live/today event",
            "completed": "completed event",
        }.get(status, status)
        result_text = f" Top results: {'; '.join(result_lines)}." if result_lines else ""
        summary_text = (
            f"Official UFC {status_label}: {title}. "
            f"Date/time: {date_text}. "
            f"Location: {location or 'TBA'}.{result_text}"
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
    posted = set(posted_list)

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


async def generate_primefist_text(title, description, lang):
    client = AsyncGroq(api_key=GROQ_API_KEY)
    prompt = f"""Ты редактор Telegram-канала PRIMEFIST (единоборства: MMA, бокс, K1, кикбокс, тай бокс).

Новость:
Заголовок: {title}
Описание: {description}
Язык источника: {lang}

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
        return json.loads(raw)
    except Exception as e:
        log.error(f"Groq API error: {e}")
        return None

def channel_post(ai_data: dict, source: str, link: str) -> str:
    """Короткий пост для канала."""
    return (
        f"<b>🥊 {html.escape(ai_data['hook_ru'])} / {html.escape(ai_data['hook_en'])}</b>\n\n"
        f"🇷🇺 {html.escape(ai_data['short_ru'])}\n\n"
        f"🇬🇧 {html.escape(ai_data['short_en'])}\n\n"
        f"🔗 <a href=\"{link}\">{html.escape(source)} (Link)</a>\n\n"
        f"💬 Подробнее в комментариях\n\n"
        f"#mma #boxing #kickboxing #primefist #fightnews"
    )


def discussion_post(ai_data: dict, source: str, tag: str, link: str) -> str:
    """Полный пост для комментариев."""
    return (
        f"📖 Полная новость / Full story\n\n"
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


async def send_channel_post(bot: Bot, channel_id: str, image_url: str | None, text: str):
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
async def main():
    # Create empty posted.json if it doesn't exist to prevent git push errors
    if not os.path.exists(POSTED_FILE):
        save_posted([])

    missing = [
        name for name, value in {
            "TELEGRAM_BOT_TOKEN": BOT_TOKEN,
            "CHANNEL_ID": CHANNEL_ID,
            "GROQ_API_KEY": GROQ_API_KEY,
        }.items()
        if not value
    ]
    if missing:
        raise RuntimeError(f"Missing GitHub secrets/env vars: {', '.join(missing)}")

    posted = load_posted()
    bot = Bot(token=BOT_TOKEN)
    selected_article = find_selected_article(posted)

    if not selected_article:
        log.info("Finished checking all feeds. No unposted/recent articles found.")
        return

        
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

        msg = await send_channel_post(bot, CHANNEL_ID, selected_article.get("image"), ch_text)
            
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
        
        # Send poll to the same discussion thread first if AI generated it.
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
        
        posted.append(selected_article["id"])
        save_posted(posted)
    except Exception as e:
        log.error(f"Failed to post Main to Telegram: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log.exception("Bot run failed: %s", e)
        sys.exit(1)
