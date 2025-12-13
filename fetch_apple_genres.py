import os
import requests
import json
import re
import time
import random
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from bs4 import BeautifulSoup

# =============================================================================
# CONFIG
# =============================================================================
WORKER_URL = os.environ.get("TURSO_WORKER_URL") # Defined in GitHub Secrets
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
# LINK RESOLVERS (ODESLI, TAPELINK, SQUIGLY)
# =============================================================================
def resolve_odesli(spotify_url):
    try:
        res = requests.get("https://api.odesli.co/resolve", params={'url': spotify_url}, headers=get_headers(), timeout=10)
        if res.status_code != 200: return None
        data = res.json()
        
        # Odesli often gives the API ID, we need to construct the link or scrape their landing page
        # To save time, we look for the direct appleMusic link in their linksByPlatform if available
        links = data.get('linksByPlatform', {})
        if 'appleMusic' in links:
            return links['appleMusic'].get('url')
        return None 
    except: return None

def resolve_tapelink(spotify_url):
    try:
        headers = get_headers()
        headers.update({'Origin': 'https://www.tapelink.io', 'Content-Type': 'application/json'})
        res = requests.post("https://www.tapelink.io/api/generate-link", json={"url": spotify_url}, headers=headers, timeout=10)
        if res.status_code != 200: return None
        data = res.json()
        share_stub = data.get('shareableLink')
        if not share_stub: return None
        
        full_url = f"https://{share_stub}" if not share_stub.startswith("http") else share_stub
        # We need to scrape the tapelink page to get the actual Apple Music link
        page = requests.get(full_url, headers=headers, timeout=10)
        soup = BeautifulSoup(page.text, 'html.parser')
        next_data = soup.find('script', id='__NEXT_DATA__')
        if next_data:
            jd = json.loads(next_data.string)
            return jd['props']['pageProps']['initialSongData']['platforms'].get('apple_music')
        return None
    except: return None

def resolve_squigly(spotify_url):
    try:
        headers = get_headers()
        headers.update({'Origin': 'https://squigly.link', 'Content-Type': 'application/json'})
        # 1. Create
        res = requests.post("https://squigly.link/api/create", json={"url": spotify_url}, headers=headers, timeout=10)
        if res.status_code not in [200, 201]: return None
        slug = res.json().get('slug')
        if not slug: return None
        
        # 2. Resolve
        res2 = requests.get(f"https://squigly.link/api/resolve/{slug}", headers=headers, timeout=10)
        if res2.status_code != 200: return None
        return res2.json().get('services', {}).get('apple', {}).get('url')
    except: return None

# =============================================================================
# APPLE MUSIC SCRAPER
# =============================================================================
def scrape_apple_metadata(apple_url):
    if not apple_url: return None
    
    # Clean URL
    apple_url = apple_url.replace("geo.music.apple.com", "music.apple.com")
    apple_url = re.sub(r'\.com/[a-z]{2}/', '.com/us/', apple_url)
    
    try:
        response = requests.get(apple_url, headers=get_headers(), timeout=10)
        if response.status_code != 200: return None
        
        jsonld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(jsonld_pattern, response.text, re.DOTALL)
        
        for match in matches:
            try:
                data = json.loads(match.strip())
                
                # We need datePublished and genre
                # Depending on page type (Album vs Song), structure varies.
                # Usually nested in 'audio' or 'inAlbum' or at root.
                
                # Flatten structure to find date
                date_published = None
                
                # Try specific paths first
                if 'datePublished' in data: date_published = data['datePublished']
                elif 'audio' in data and 'datePublished' in data['audio']: date_published = data['audio']['datePublished']
                elif 'inAlbum' in data and 'datePublished' in data['inAlbum']: date_published = data['inAlbum']['datePublished']
                
                # Extract genres
                raw_genres = find_key_recursive(data, "genre")
                clean_genres = list(set([g for g in raw_genres if g.lower() != "music"]))
                
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
    
    # 1. Get Links from all providers
    results = []
    
    # We run them sequentially here, but could use threading for speed if needed.
    # For a GitHub action running hourly, sequential is safer/easier to debug.
    
    link1 = resolve_odesli(spotify_url)
    if link1: 
        meta = scrape_apple_metadata(link1)
        if meta: results.append(meta)
        
    # Sleep briefly to be polite
    time.sleep(0.5) 
    
    link2 = resolve_tapelink(spotify_url)
    if link2 and (not link1 or link2 != link1):
        meta = scrape_apple_metadata(link2)
        if meta: results.append(meta)

    time.sleep(0.5)

    link3 = resolve_squigly(spotify_url)
    if link3 and (not link1 or link3 != link1) and (not link2 or link3 != link2):
        meta = scrape_apple_metadata(link3)
        if meta: results.append(meta)
        
    if not results:
        print(f"   [SKIP] No Apple data found for {spotify_id}")
        return None
        
    # 2. Sort by Date (Oldest Wins)
    # If date is missing (None), treat as '9999-99-99' (put at end)
    results.sort(key=lambda x: x['date'] if x['date'] else '9999-99-99')
    
    best_match = results[0]
    print(f"   [FOUND] {spotify_id} -> {best_match['date']} | Genres: {best_match['genres']}")
    
    return {
        'isrc': isrc,
        'track_id': spotify_id,
        'apple_music_genres': json.dumps(best_match['genres']),
        'updated_at': int(time.time()) # Important to update timestamp so we don't query again immediately
    }

def run_job():
    if not WORKER_URL:
        print("Error: TURSO_WORKER_URL secret is missing.")
        return

    print("--- 1. Fetching tracks missing Apple Genres ---")
    try:
        res = requests.post(f"{WORKER_URL}/genres/find-missing-apple", json={"limit": 30}, timeout=30)
        res.raise_for_status()
        data = res.json()
        tracks = data.get('tracks', [])
    except Exception as e:
        print(f"Failed to fetch job: {e}")
        return

    if not tracks:
        print("No tracks need updating.")
        return

    print(f"Processing {len(tracks)} tracks...")
    
    updates = []
    for t in tracks:
        res = process_track(t['id'], t['isrc'])
        if res:
            updates.append(res)
        else:
            # If we fail to find data, we should still update 'updated_at' 
            # or set genres to '[]' so we don't loop forever on unfindable tracks.
            # Here we set empty array to mark as "checked".
            updates.append({
                'isrc': t['isrc'],
                'track_id': t['id'],
                'apple_music_genres': '[]',
                'updated_at': int(time.time())
            })
        time.sleep(1) # Rate limit protection

    if updates:
        print(f"--- 2. Sending {len(updates)} updates to Turso ---")
        try:
            # The worker now accepts payloads without duration_ms if apple_music_genres is present.
            res = requests.post(f"{WORKER_URL}/genres", json=updates, timeout=30)
            if res.status_code == 200:
                print("Success.")
            else:
                print(f"Update failed: {res.text}")
        except Exception as e:
            print(f"Error sending updates: {e}")

if __name__ == "__main__":
    run_job()
