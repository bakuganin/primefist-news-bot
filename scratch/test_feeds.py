import feedparser
import requests
import sys

# Set encoding for Windows terminal
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

FEEDS = [
    {"name": "Delfi Sport (Rus) - Boxing", "url": "https://rus.delfi.ee/rss/sport/boks.xml"},
    {"name": "Delfi Sport (Est) - Poks", "url": "https://sport.delfi.ee/rss/poks"},
    {"name": "Postimees Sport - Võitlussport", "url": "https://sport.postimees.ee/rss/term/41450"},
    {"name": "ERR Sport - Võitlussport", "url": "https://sport.err.ee/rss/voitlussport"},
    {"name": "Õhtuleht Sport - Poks", "url": "https://sport.ohtuleht.ee/rss/poks"},
    # Potential Fallbacks
    {"name": "Delfi Sport (Rus) - General", "url": "https://rus.delfi.ee/rss/sport/"},
    {"name": "Delfi Sport (Est) - General", "url": "https://sport.delfi.ee/rss/"},
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
        else:
            print(f"  Failed: HTTP {res.status_code}")
    except Exception as e:
        print(f"  Error: {e}")
    print("-" * 20)
