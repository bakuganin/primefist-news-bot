import requests
from bs4 import BeautifulSoup

def test_scrape_ufc_events():
    url = "https://www.ufc.com/events"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code != 200:
            print(f"Failed to fetch: {response.status_code}")
            return
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for upcoming events
        events = soup.select('.c-card-event--upcoming')
        print(f"Found {len(events)} upcoming events.")
        
        for event in events:
            # Extract Title (often fighters)
            title_node = event.select_one('.c-card-event--upcoming__headline')
            title = title_node.get_text(strip=True) if title_node else "Unknown Event"
            
            # Date
            date_node = event.select_one('.c-card-event--upcoming__date')
            date_str = date_node.get_text(strip=True) if date_node else "Unknown Date"
            
            # Link
            link_node = event.select_one('a')
            link = "https://www.ufc.com" + link_node['href'] if link_node else ""
            
            print(f"Event: {title}")
            print(f"Date: {date_str}")
            print(f"Link: {link}")
            print("-" * 20)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    test_scrape_ufc_events()
