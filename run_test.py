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
# METHOD 1: ODESLI (Hybrid: API ID -> Page Scrape)
# =============================================================================
def fetch_via_odesli(spotify_url):
    session = requests.Session()
    
    # 1. Resolve ID via API
    try:
        res = session.get("https://api.odesli.co/resolve", params={'url': spotify_url}, headers=get_headers())
        if res.status_code == 429: return None, "Rate Limit"
        res.raise_for_status()
        data = res.json()
        entity_id = data.get('id')
        entity_type = data.get('type')
    except Exception as e: return None, f"Odesli API Error: {e}"

    # 2. Get Page Data (Scraping)
    slug = 's' if entity_type == 'song' else 'a'
    try:
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

    except Exception as e: return None, f"Scraping Error: {e}"

# =============================================================================
# METHOD 2: TAPELINK.IO
# =============================================================================
def fetch_via_tapelink(spotify_url):
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Content-Type': 'application/json',
        'Origin': 'https://www.tapelink.io',
        'Referer': 'https://www.tapelink.io/',
        'Accept': '*/*'
    }

    try:
        # Step 1: Generate Link
        response = session.post("https://www.tapelink.io/api/generate-link", json={"url": spotify_url}, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('success'): return None, "Tapelink Success=False"
            
        share_link_stub = data.get('shareableLink')
        full_share_url = f"https://{share_link_stub}" if not share_link_stub.startswith("http") else share_link_stub

        # Step 2: Scrape Data
        page_response = session.get(full_share_url, headers=headers)
        page_response.raise_for_status()
        
        soup = BeautifulSoup(page_response.text, 'html.parser')
        next_data_tag = soup.find('script', id='__NEXT_DATA__')
        
        if not next_data_tag: return None, "Tapelink Data Not Found"
        
        json_data = json.loads(next_data_tag.string)
        initial_data = json_data['props']['pageProps']['initialSongData']
        platforms = initial_data.get('platforms', {})
        apple_link = platforms.get('apple_music')
        
        if apple_link: return apple_link, None
        return None, "Tapelink: Apple missing"
    except Exception as e: return None, f"Tapelink Error: {e}"

# =============================================================================
# METHOD 3: SQUIGLY.LINK
# =============================================================================
def fetch_via_squigly(spotify_url):
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Referer': 'https://squigly.link/',
        'Origin': 'https://squigly.link',
        'Content-Type': 'application/json'
    }

    try:
        # Step 1: Create Slug
        # NOTE: This often returns HTTP 201 (Created), which raise_for_status() accepts as success.
        response = session.post("https://squigly.link/api/create", json={"url": spotify_url}, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        slug = data.get('slug')
        if not slug: return None, "Squigly: No slug returned"

        # Step 2: Resolve Slug
        resolve_url = f"https://squigly.link/api/resolve/{slug}"
        response = session.get(resolve_url, headers=headers)
        response.raise_for_status()
        
        result_data = response.json()
        apple_service = result_data.get('services', {}).get('apple')

        if apple_service and apple_service.get('url'):
            return apple_service['url'], None
        return None, "Squigly: Apple link not found"

    except Exception as e: return None, f"Squigly Error: {e}"

# =============================================================================
# GENRE SCRAPER
# =============================================================================
def get_genres(apple_url):
    try:
        response = requests.get(apple_url, headers=get_headers(), timeout=15)
        response.raise_for_status()
        
        jsonld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(jsonld_pattern, response.text, re.DOTALL)
        
        all_genres = []
        for match in matches:
            try:
                data = json.loads(match.strip())
                found = find_key_recursive(data, "genre")
                all_genres.extend(found)
            except: continue
        
        unique_genres = list(set(all_genres))
        final_genres = [g for g in unique_genres if g.lower() != "music"]
        return final_genres, None
    except Exception as e: return None, f"Genre Fetch Error: {e}"

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
        print("-" * 70)
        
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
                    print(f"       -> 🍎 Genre Error: {g_err}")
                else:
                    print(f"       -> 🍎 Genres: {genres}")
            
            # Small sleep to be polite
            time.sleep(1)
        
        print("\n" + "=" * 70 + "\n")
        time.sleep(2)
