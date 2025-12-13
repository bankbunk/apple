import requests
import json
import re
import random
import time
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup

# =============================================================================
# CONFIGURATION
# =============================================================================
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
]

def get_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
    }

# =============================================================================
# HELPER: RECURSIVE GENRE FINDER
# =============================================================================
def find_key_recursive(data, target_key):
    found_values = []
    if isinstance(data, dict):
        for key, value in data.items():
            if key == target_key:
                if isinstance(value, list): found_values.extend(value)
                else: found_values.append(value)
            elif isinstance(value, (dict, list)):
                found_values.extend(find_key_recursive(value, target_key))
    elif isinstance(data, list):
        for item in data:
            found_values.extend(find_key_recursive(item, target_key))
    return found_values

# =============================================================================
# METHOD 1: ODESLI
# =============================================================================
def fetch_via_odesli(spotify_url):
    session = requests.Session()
    try:
        # 1. Resolve ID
        res = session.get("https://api.odesli.co/resolve", params={'url': spotify_url}, headers=get_headers())
        if res.status_code == 429: return None, "Rate Limit (429)"
        if res.status_code != 200: return None, f"API Error {res.status_code}"
        
        data = res.json()
        entity_id = data.get('id')
        entity_type = data.get('type')
        
        # 2. Get Page Data
        slug = 's' if entity_type == 'song' else 'a'
        page = session.get(f"https://song.link/{slug}/{entity_id}", headers=get_headers())
        soup = BeautifulSoup(page.text, 'html.parser')
        
        next_data = soup.find('script', id='__NEXT_DATA__')
        if not next_data: return None, "Odesli Data Not Found"
        
        json_data = json.loads(next_data.string)
        page_data = json_data.get('props', {}).get('pageProps', {}).get('pageData', {})
        
        raw_link = None
        for section in page_data.get('sections', []):
            if 'links' in section:
                for link in section['links']:
                    if link.get('platform') == 'appleMusic':
                        raw_link = link.get('url')
                        break
            if raw_link: break
            
        if not raw_link: return None, "Apple Music link not found"
        
        # 3. Clean URL
        url = raw_link.replace("geo.music.apple.com", "music.apple.com")
        url = re.sub(r'\.com/[a-z]{2}/', '.com/us/', url)
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        new_query = {}
        if 'i' in query_params: new_query['i'] = query_params['i']
        clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(new_query, doseq=True), parsed.fragment))
        return clean_url, None
    except Exception as e: return None, f"Odesli Exception: {str(e)[:50]}"

# =============================================================================
# METHOD 2: TAPELINK.IO
# =============================================================================
def fetch_via_tapelink(spotify_url):
    session = requests.Session()
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Content-Type': 'application/json',
        'Origin': 'https://www.tapelink.io',
        'Referer': 'https://www.tapelink.io/'
    }
    try:
        # Step 1: Generate Link
        res = session.post("https://www.tapelink.io/api/generate-link", json={"url": spotify_url}, headers=headers)
        if res.status_code != 200: return None, f"Tapelink API {res.status_code}"
        
        data = res.json()
        if not data.get('success'): return None, "Tapelink Success=False"
            
        share_link = data.get('shareableLink')
        full_url = f"https://{share_link}" if not share_link.startswith("http") else share_link

        # Step 2: Scrape Data
        page_res = session.get(full_url, headers=headers)
        soup = BeautifulSoup(page_res.text, 'html.parser')
        next_data = soup.find('script', id='__NEXT_DATA__')
        
        if not next_data: return None, "Tapelink Data Missing"
        
        json_data = json.loads(next_data.string)
        platforms = json_data['props']['pageProps']['initialSongData'].get('platforms', {})
        apple_link = platforms.get('apple_music')
        
        if apple_link: return apple_link, None
        return None, "Tapelink Apple missing"
    except Exception as e: return None, f"Tapelink Exception: {str(e)[:50]}"

# =============================================================================
# METHOD 3: SQUIGLY.LINK
# =============================================================================
def fetch_via_squigly(spotify_url):
    session = requests.Session()
    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Referer': 'https://squigly.link/',
        'Origin': 'https://squigly.link',
        'Content-Type': 'application/json'
    }
    try:
        # Step 1: Create Slug
        res = session.post("https://squigly.link/api/create", json={"url": spotify_url}, headers=headers)
        if res.status_code != 200: return None, f"Squigly API {res.status_code}"
        slug = res.json().get('slug')
        if not slug: return None, "No Slug"

        # Step 2: Resolve
        res = session.get(f"https://squigly.link/api/resolve/{slug}", headers=headers)
        apple = res.json().get('services', {}).get('apple', {})
        
        if apple and apple.get('url'): return apple['url'], None
        return None, "Squigly Apple missing"
    except Exception as e: return None, f"Squigly Exception: {str(e)[:50]}"

# =============================================================================
# GENRE SCRAPER
# =============================================================================
def get_genres(apple_url):
    try:
        res = requests.get(apple_url, headers=get_headers(), timeout=10)
        jsonld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(jsonld_pattern, res.text, re.DOTALL)
        
        all_genres = []
        for match in matches:
            try:
                data = json.loads(match.strip())
                all_genres.extend(find_key_recursive(data, "genre"))
            except: continue
        
        final = [g for g in list(set(all_genres)) if g.lower() != "music"]
        return final, None
    except Exception as e: return None, f"Apple Scrape Error: {str(e)[:50]}"

# =============================================================================
# MAIN EXECUTION
# =============================================================================
if __name__ == "__main__":
    TEST_URLS = [
        "https://open.spotify.com/track/6g9i9Uny9yR6ZpBX8NYuzz",
        "https://open.spotify.com/track/3Z7OFraob1P0QscGaSoh0v",
        "https://open.spotify.com/track/0JKjdalJgMHiHGiPAX44iK",
        "https://open.spotify.com/track/0ybTlxpt6jsG1LzGuHyEgA",
        "https://open.spotify.com/track/3X1pyT3vAKkkS3ExTARZNf"
    ]

    METHODS = [
        ("Odesli", fetch_via_odesli),
        ("Tapelink", fetch_via_tapelink),
        ("Squigly", fetch_via_squigly)
    ]

    print(f"🚀 STARTING GITHUB ACTIONS TEST ON {len(TEST_URLS)} TRACKS\n")

    for i, track_url in enumerate(TEST_URLS):
        print(f"🎵 TRACK {i+1}: {track_url}")
        print("-" * 60)
        
        for name, method in METHODS:
            start_time = time.time()
            apple_url, error = method(track_url)
            duration = time.time() - start_time
            
            if error:
                print(f"   [{name}] ❌ Failed ({duration:.2f}s): {error}")
            else:
                print(f"   [{name}] ✅ Link Found ({duration:.2f}s)")
                # Try to fetch genres immediately to test Apple blocking too
                genres, g_err = get_genres(apple_url)
                if g_err:
                    print(f"       -> 🍎 Genre Fetch Failed: {g_err}")
                else:
                    print(f"       -> 🍎 Genres: {genres}")
            
            # Sleep to prevent self-imposed rate limits between methods
            time.sleep(1)
        
        print("\n" + "=" * 60 + "\n")
        # Sleep between tracks
        time.sleep(2)
