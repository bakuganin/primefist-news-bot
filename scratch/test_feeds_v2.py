import feedparser
import requests
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

FEEDS = [
    {"name": "Delfi Est Sport", "url": "https://www.delfi.ee/rss/sport.xml"},
    {"name": "Delfi Rus Sport", "url": "https://rus.delfi.ee/rss/sport.xml"},
    {"name": "Postimees Sport", "url": "https://sport.postimees.ee/rss"},
    {"name": "ERR Sport", "url": "https://sport.err.ee/rss/voitlussport"},
]

for f in FEEDS:
    print(f"Checking {f['name']} ({f['url']})...")
    try:
        res = requests.get(f['url'], timeout=10, headers={'User-Agent': 'Mozilla/5.0'})
        print(f"  Status: {res.status_code}")
        if res.status_code == 200:
            feed = feedparser.parse(res.text)
            print(f"  Entries found: {len(feed.entries)}")
            if feed.entries:
                print(f"  Latest: {feed.entries[0].title}")
    except Exception as e:
        print(f"  Error: {e}")
    print("-" * 20)
