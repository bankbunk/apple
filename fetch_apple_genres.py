import os
import requests
import json
import re
import time
import random
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup

# =============================================================================
# CONFIGURATION
# =============================================================================
WORKER_URL = os.environ.get("TURSO_WORKER_URL")

PROCESS_LIMIT = 1000

START_TIME = time.time()
MAX_RUNTIME_SECONDS = 5 * 60 * 60 + 15 * 60

GENRES_TO_KEEP_WHOLE = [
    "singer/songwriter",
    "adult/contemporary"
]

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
def resolve_odesli(spotify_url):
    session = requests.Session()
    
    # 1. Resolve ID via API
    try:
        res = session.get("https://api.odesli.co/resolve", params={'url': spotify_url}, headers=get_headers(), timeout=10)
        if res.status_code == 429: return None # Rate Limit
        if res.status_code != 200: return None
        
        data = res.json()
        entity_id = data.get('id')
        entity_type = data.get('type')
        
        # Shortcut: Check if API gave the link directly
        links = data.get('linksByPlatform', {})
        if 'appleMusic' in links:
            return links['appleMusic'].get('url')
            
    except Exception as e: return None

    # 2. Get Page Data (Scraping Fallback)
    if not entity_id or not entity_type: return None
    
    slug = 's' if entity_type == 'song' else 'a'
    try:
        page = session.get(f"https://song.link/{slug}/{entity_id}", headers=get_headers(), timeout=10)
        soup = BeautifulSoup(page.text, 'html.parser')
        
        next_data = soup.find('script', id='__NEXT_DATA__')
        if not next_data: return None
        
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
            
        return raw_link

    except Exception as e: return None

# =============================================================================
# METHOD 2: TAPELINK.IO
# =============================================================================
def resolve_tapelink(spotify_url):
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
        response = session.post("https://www.tapelink.io/api/generate-link", json={"url": spotify_url}, headers=headers, timeout=10)
        if response.status_code != 200: return None
        data = response.json()
        
        if not data.get('success'): return None
            
        share_link_stub = data.get('shareableLink')
        if not share_link_stub: return None
        
        full_share_url = f"https://{share_link_stub}" if not share_link_stub.startswith("http") else share_link_stub

        # Step 2: Scrape Data
        page_response = session.get(full_share_url, headers=headers, timeout=10)
        if page_response.status_code != 200: return None
        
        soup = BeautifulSoup(page_response.text, 'html.parser')
        next_data_tag = soup.find('script', id='__NEXT_DATA__')
        if not next_data_tag: return None
        
        json_data = json.loads(next_data_tag.string)
        initial_data = json_data['props']['pageProps']['initialSongData']
        platforms = initial_data.get('platforms', {})
        return platforms.get('apple_music')

    except Exception: return None

# =============================================================================
# METHOD 3: SQUIGLY.LINK
# =============================================================================
def resolve_squigly(spotify_url):
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
        'Referer': 'https://squigly.link/',
        'Origin': 'https://squigly.link',
        'Content-Type': 'application/json'
    }

    try:
        # Step 1: Create Slug
        response = session.post("https://squigly.link/api/create", json={"url": spotify_url}, headers=headers, timeout=10)
        if response.status_code not in [200, 201]: return None
        slug = response.json().get('slug')
        if not slug: return None

        # Step 2: Resolve Slug
        resolve_url = f"https://squigly.link/api/resolve/{slug}"
        response = session.get(resolve_url, headers=headers, timeout=10)
        if response.status_code != 200: return None
        
        result_data = response.json()
        return result_data.get('services', {}).get('apple', {}).get('url')

    except Exception: return None

# =============================================================================
# APPLE MUSIC SCRAPER (Extended to find Date + Genres)
# =============================================================================
def scrape_apple_metadata(apple_url):
    if not apple_url: return None
    
    # Clean URL
    apple_url = apple_url.replace("geo.music.apple.com", "music.apple.com")
    apple_url = re.sub(r'\.com/[a-z]{2}/', '.com/us/', apple_url)
    
    # Clean Params
    try:
        parsed = urlparse(apple_url)
        query_params = parse_qs(parsed.query)
        new_query = {}
        if 'i' in query_params: new_query['i'] = query_params['i']
        apple_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(new_query, doseq=True), parsed.fragment))
    except: pass
    
    try:
        response = requests.get(apple_url, headers=get_headers(), timeout=10)
        if response.status_code != 200: return None
        
        jsonld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(jsonld_pattern, response.text, re.DOTALL)
        
        for match in matches:
            try:
                data = json.loads(match.strip())
                
                # --- DATE EXTRACTION ---
                date_published = None
                if 'datePublished' in data: date_published = data['datePublished']
                elif 'audio' in data and 'datePublished' in data['audio']: date_published = data['audio']['datePublished']
                elif 'inAlbum' in data and 'datePublished' in data['inAlbum']: date_published = data['inAlbum']['datePublished']

                # Normalize date format (handle YYYY or YYYY-MM)
                if date_published:
                    if len(date_published) == 4:
                        date_published = f"{date_published}-12-31"
                    elif len(date_published) == 7:
                        date_published = f"{date_published}-28"
                
                # --- GENRE EXTRACTION ---
                raw_genres = find_key_recursive(data, "genre")
                
                processed_genres = []
                for g in raw_genres:
                    if isinstance(g, str):
                        if g.lower() in GENRES_TO_KEEP_WHOLE:
                            processed_genres.append(g)
                        else:
                            parts = g.split('/')
                            for part in parts:
                                p = part.strip()
                                if p: processed_genres.append(p)

                clean_genres = list(set([g for g in processed_genres if g.lower() != "music"]))
                
                if not clean_genres: continue 
                
                return {
                    'url': apple_url,
                    'date': date_published, # Format YYYY-MM-DD
                    'genres': clean_genres
                }
            except: continue
        return None
    except: return None

# =============================================================================
# MAIN LOGIC
# =============================================================================
def process_track(spotify_id, isrc):
    spotify_url = f"https://open.spotify.com/track/{spotify_id}"
    
    results = []
    
    # 1. Fetch from all providers sequentially
    link1 = resolve_odesli(spotify_url)
    if link1: 
        meta = scrape_apple_metadata(link1)
        if meta: results.append(meta)
    
    time.sleep(random.uniform(0.5, 1.0)) # Polite delay
    
    link2 = resolve_tapelink(spotify_url)
    if link2 and (not link1 or link2 != link1):
        meta = scrape_apple_metadata(link2)
        if meta: results.append(meta)

    time.sleep(random.uniform(0.5, 1.0))

    link3 = resolve_squigly(spotify_url)
    if link3 and (not link1 or link3 != link1) and (not link2 or link3 != link2):
        meta = scrape_apple_metadata(link3)
        if meta: results.append(meta)
        
    if not results:
        print(f"   [SKIP] No Apple data found for {spotify_id}", flush=True)
        return None
        
    # 2. Sort by Date (Oldest Wins)
    # If date is missing (None), treat as '9999-99-99' (put at end)
    results.sort(key=lambda x: x['date'] if x['date'] else '9999-99-99')
    
    best_match = results[0]
    print(f"   [FOUND] {spotify_id} -> {best_match['date']} | Genres: {best_match['genres']}", flush=True)
    
    return {
        'isrc': isrc,
        'track_id': spotify_id,
        'apple_music_genres': json.dumps(best_match['genres']),
        'updated_at': int(time.time()) 
    }

BATCH_SIZE = 250

def send_updates_to_turso(updates):
    """Send a batch of updates to Turso"""
    if not updates:
        return True
    
    print(f"--- Sending batch of {len(updates)} updates to Turso ---", flush=True)
    try:
        res = requests.post(f"{WORKER_URL}/genres", json=updates, timeout=30)
        if res.status_code == 200:
            print(f"Batch sent successfully.", flush=True)
            return True
        else:
            print(f"Batch failed: {res.text}", flush=True)
            return False
    except Exception as e:
        print(f"Error sending batch: {e}", flush=True)
        return False

def run_job():
    if not WORKER_URL:
        print("Error: TURSO_WORKER_URL secret is missing.", flush=True)
        return

    # Determine limit
    limit = PROCESS_LIMIT
    if limit == 0:
        limit = 50  # Production safety limit

    print(f"--- 1. Fetching tracks missing Apple Genres (Limit: {limit}) ---", flush=True)
    try:
        res = requests.post(f"{WORKER_URL}/genres/find-missing-apple", json={"limit": limit}, timeout=30)
        res.raise_for_status()
        data = res.json()
        tracks = data.get('tracks', [])
    except Exception as e:
        print(f"Failed to fetch job: {e}", flush=True)
        return

    if not tracks:
        print("No tracks need updating.", flush=True)
        return

    print(f"Processing {len(tracks)} tracks...", flush=True)

    updates = []
    total_sent = 0

    for i, t in enumerate(tracks):
        # Check time limit before each track
        elapsed = time.time() - START_TIME
        if elapsed >= MAX_RUNTIME_SECONDS:
            print(f"--- TIME LIMIT REACHED ({elapsed/3600:.2f}h) - Stopping gracefully ---", flush=True)
            break

        try:
            res = process_track(t['id'], t['isrc'])
            if res:
                updates.append(res)
            else:
                updates.append({
                    'isrc': t['isrc'],
                    'track_id': t['id'],
                    'apple_music_genres': '[]',
                    'updated_at': int(time.time())
                })
        except Exception as e:
            print(f"Error processing {t['id']}: {e}", flush=True)

        time.sleep(1)  # Rate limit protection

        # Send batch every BATCH_SIZE tracks
        if len(updates) >= BATCH_SIZE:
            print(f"--- Reached {BATCH_SIZE} tracks (Total processed: {i + 1}/{len(tracks)}) ---", flush=True)
            if send_updates_to_turso(updates):
                total_sent += len(updates)
                updates = []  # Clear for next batch
            else:
                print("Batch failed, will retry with next batch", flush=True)

    # Send remaining updates
    if updates:
        print(f"--- 2. Sending final batch of {len(updates)} updates to Turso ---", flush=True)
        if send_updates_to_turso(updates):
            total_sent += len(updates)

    print(f"--- DONE: Total {total_sent} tracks sent to Turso ---", flush=True)

if __name__ == "__main__":
    run_job()
