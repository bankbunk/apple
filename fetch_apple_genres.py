import os
import requests
import json
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, quote
from bs4 import BeautifulSoup

class RateLimitException(Exception):
    pass

class SoftRateLimitException(Exception):
    """When API returns 200 but with empty/error data"""
    pass

# =============================================================================
# CONFIGURATION
# =============================================================================
WORKER_URL = os.environ.get("TURSO_WORKER_URL")

# Defaults to 0/1 (no sharding)
WORKER_INDEX = int(os.environ.get("WORKER_INDEX", 0))
TOTAL_WORKERS = int(os.environ.get("TOTAL_WORKERS", 1))

CURRENT_PRIMARY_PROVIDER = "Odesli"
# --------------------------------------------------

PROCESS_LIMIT = 0

START_TIME = time.time()
MAX_RUNTIME_SECONDS = 55 * 60

GENRES_TO_KEEP_WHOLE = [
    "singer/songwriter",
    "adult/contemporary"
]

SQUIGLY_COOLDOWN_UNTIL = 0
ODESLI_COOLDOWN_UNTIL = 0
SONGLINK_COOLDOWN_UNTIL = 0

# Minimum time each track processing must take (smart delay)
MIN_TRACK_DURATION = 1.5  # seconds

# Add delay between tracks to avoid rate limits
REQUEST_DELAY = 0.5  # seconds between tracks

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
    global ODESLI_COOLDOWN_UNTIL
    
    # Check cooldown
    if time.time() < ODESLI_COOLDOWN_UNTIL:
        print(f"      [Odesli] On cooldown, skipping", flush=True)
        return None
    
    session = requests.Session()

    # 1. Resolve ID via API
    try:
        res = session.get("https://api.odesli.co/resolve", params={'url': spotify_url}, headers=get_headers(), timeout=10)
        
        if res.status_code == 429: 
            raise RateLimitException("Odesli")
        
        if res.status_code != 200:
            print(f"      [Odesli] API returned {res.status_code}", flush=True)
            return None

        data = res.json()
        
        # DEBUG: Check what API actually returned
        entity_id = data.get('id')
        entity_type = data.get('type')
        links = data.get('linksByPlatform', {})
        
        # Shortcut: Check if API gave the link directly
        if 'appleMusic' in links:
            apple_url = links['appleMusic'].get('url')
            if apple_url:
                print(f"      [Odesli] Direct link found", flush=True)
                return apple_url
        
        # Check for soft rate limit (API returned but no useful data)
        if not entity_id or not entity_type:
            print(f"      [Odesli] API returned empty entity (soft rate limit?): id={entity_id}, type={entity_type}", flush=True)
            # Check if response looks like a rate limit
            if not links and not entity_id:
                raise SoftRateLimitException("Odesli returned empty response")
            return None

    except RateLimitException: 
        raise
    except SoftRateLimitException:
        raise
    except Exception as e:
        print(f"      [Odesli] API Error: {e}", flush=True)
        return None

    # 2. Get Page Data (Scraping Fallback)
    slug = 's' if entity_type == 'song' else 'a'
    try:
        page = session.get(f"https://song.link/{slug}/{entity_id}", headers=get_headers(), timeout=10)
        
        if page.status_code == 429: 
            raise RateLimitException("Odesli Page")

        if page.status_code != 200:
            print(f"      [Odesli] Page returned {page.status_code}", flush=True)
            return None

        soup = BeautifulSoup(page.text, 'html.parser')

        next_data = soup.find('script', id='__NEXT_DATA__')
        if not next_data:
            print(f"      [Odesli] No NEXT_DATA found on page", flush=True)
            return None

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

        if raw_link:
            print(f"      [Odesli] Page scrape found link", flush=True)
        else:
            print(f"      [Odesli] No Apple link in page data", flush=True)
            
        return raw_link

    except RateLimitException: 
        raise
    except Exception as e:
        print(f"      [Odesli] Page Scrape Error: {e}", flush=True)
        return None

# =============================================================================
# METHOD 2: SONGLINK API (Replaces Tapelink)
# =============================================================================
def resolve_songlink_api(spotify_url):
    global SONGLINK_COOLDOWN_UNTIL
    
    # Check cooldown
    if time.time() < SONGLINK_COOLDOWN_UNTIL:
        print(f"      [SongLink] On cooldown, skipping", flush=True)
        return None
    
    # URL-encode the Spotify URL
    encoded_url = quote(spotify_url)
    api_url = f"https://api.song.link/v1-alpha.1/links?url={encoded_url}"

    try:
        # We use a standard requests.get, but include our rotating headers 
        # to appear more like a browser/legitimate client
        response = requests.get(api_url, headers=get_headers(), timeout=10)
        
        if response.status_code == 429:
            raise RateLimitException("SongLink API")
        
        if response.status_code != 200:
            print(f"      [SongLink] API returned {response.status_code}", flush=True)
            return None
            
        data = response.json()
        
        # Extract Apple Music URL
        apple_music_url = data.get('linksByPlatform', {}).get('appleMusic', {}).get('url')
        
        if apple_music_url:
            print(f"      [SongLink] Direct link found", flush=True)
            return apple_music_url
        else:
            print(f"      [SongLink] No Apple Music equivalent found in response", flush=True)
            return None

    except RateLimitException:
        raise
    except Exception as e:
        print(f"      [SongLink] Error: {e}", flush=True)
        return None

# =============================================================================
# METHOD 3: SQUIGLY.LINK
# =============================================================================
def resolve_squigly(spotify_url):
    global SQUIGLY_COOLDOWN_UNTIL
    
    # Check cooldown
    if time.time() < SQUIGLY_COOLDOWN_UNTIL:
        print(f"      [Squigly] On cooldown, skipping", flush=True)
        return None
    
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
        
        if response.status_code == 429: 
            raise RateLimitException("Squigly")
        
        if response.status_code not in [200, 201]:
            print(f"      [Squigly] Create returned {response.status_code}", flush=True)
            return None
        
        data = response.json()
        if not data:
            print(f"      [Squigly] Create returned empty response", flush=True)
            return None
            
        slug = data.get('slug')
        if not slug:
            print(f"      [Squigly] No slug in response", flush=True)
            return None

        # Step 2: Resolve Slug
        resolve_url = f"https://squigly.link/api/resolve/{slug}"
        response = session.get(resolve_url, headers=headers, timeout=10)
        
        if response.status_code == 429: 
            raise RateLimitException("Squigly Resolve")
        
        if response.status_code != 200:
            print(f"      [Squigly] Resolve returned {response.status_code}", flush=True)
            return None

        result_data = response.json()
        if not result_data:
            print(f"      [Squigly] Resolve returned empty response", flush=True)
            return None
        
        services = result_data.get('services')
        if not services:
            print(f"      [Squigly] No services in response", flush=True)
            return None
            
        apple_data = services.get('apple')
        if not apple_data:
            print(f"      [Squigly] No Apple in services", flush=True)
            return None
            
        apple_url = apple_data.get('url')
        if apple_url:
            print(f"      [Squigly] Found Apple link", flush=True)
        else:
            print(f"      [Squigly] Apple data has no URL", flush=True)
            
        return apple_url

    except RateLimitException: 
        raise
    except Exception as e:
        print(f"      [Squigly] Error: {e}", flush=True)
        return None
    
# =============================================================================
# APPLE MUSIC SCRAPER (Extended to find Date + Genres)
# =============================================================================
def scrape_apple_metadata(apple_url):
    if not apple_url: 
        return None

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
        if response.status_code != 200:
            print(f"      [Apple] HTTP {response.status_code} for {apple_url}", flush=True)
            return None

        jsonld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
        matches = re.findall(jsonld_pattern, response.text, re.DOTALL)

        if not matches:
            print(f"      [Apple] No JSON-LD found on {apple_url}", flush=True)
            return None

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
                    'date': date_published,
                    'genres': clean_genres
                }
            except Exception as e:
                print(f"      [Apple] JSON Parse Error: {e}", flush=True)
                continue
        return None
    except Exception as e:
        print(f"      [Apple] Request Failed: {e}", flush=True)
        return None

# =============================================================================
# MAIN LOGIC
# =============================================================================
def process_track(spotify_id, isrc):
    global SQUIGLY_COOLDOWN_UNTIL, ODESLI_COOLDOWN_UNTIL, SONGLINK_COOLDOWN_UNTIL
    global CURRENT_PRIMARY_PROVIDER # Allows the switch to persist for the NEXT track

    spotify_url = f"https://open.spotify.com/track/{spotify_id}"
    print(f"   [Processing] {spotify_id} (Primary: {CURRENT_PRIMARY_PROVIDER})", flush=True)

    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        start_ts = time.time()
        
        # 1. CRITICAL HEALTH CHECK: Are BOTH primaries broken?
        odesli_down = time.time() < ODESLI_COOLDOWN_UNTIL
        songlink_down = time.time() < SONGLINK_COOLDOWN_UNTIL

        if odesli_down and songlink_down:
            print(f"   [CRITICAL] Both Odesli and SongLink are rate limited. Sleeping 5 minutes...", flush=True)
            time.sleep(300)
            # After sleep, reset cooldowns (or just let the loop retry naturally)
            # We treat the sleep as the 'penalty'
            pass 

        # 2. SELECT PROVIDER
        # If our current primary is down, switch to the other one immediately
        if CURRENT_PRIMARY_PROVIDER == "Odesli" and odesli_down:
            print(f"      [Switch] Odesli is down, switching to SongLink", flush=True)
            CURRENT_PRIMARY_PROVIDER = "SongLink"
        elif CURRENT_PRIMARY_PROVIDER == "SongLink" and songlink_down:
            print(f"      [Switch] SongLink is down, switching to Odesli", flush=True)
            CURRENT_PRIMARY_PROVIDER = "Odesli"

        # 3. DEFINE EXECUTION
        if CURRENT_PRIMARY_PROVIDER == "Odesli":
            resolver_func = resolve_odesli
            provider_name = "Odesli"
        else:
            resolver_func = resolve_songlink_api
            provider_name = "SongLink"

        # 4. EXECUTE PRIMARY
        apple_url = None
        try:
            apple_url = resolver_func(spotify_url)
            
        except (RateLimitException, SoftRateLimitException):
            print(f"      [429] {provider_name} failed. Marking cooldown & switching.", flush=True)
            
            # Set Cooldown
            if provider_name == "Odesli":
                ODESLI_COOLDOWN_UNTIL = time.time() + 120
                CURRENT_PRIMARY_PROVIDER = "SongLink" # Switch for next retry
            else:
                SONGLINK_COOLDOWN_UNTIL = time.time() + 120
                CURRENT_PRIMARY_PROVIDER = "Odesli" # Switch for next retry
            
            retry_count += 1
            continue # Loop again immediately to try the OTHER provider

        except Exception as e:
            print(f"      [{provider_name}] Error: {e}", flush=True)
            # Generic error, maybe try squigly?

        # 5. PROCESS RESULT OR FALLBACK
        final_meta = None

        # If Primary worked
        if apple_url:
            final_meta = scrape_apple_metadata(apple_url)

        # If Primary failed to find link (Not a 429, just 404/Empty), try Squigly
        if not final_meta and time.time() > SQUIGLY_COOLDOWN_UNTIL:
            try:
                print(f"      [Fallback] Trying Squigly...", flush=True)
                squigly_link = resolve_squigly(spotify_url)
                if squigly_link:
                    final_meta = scrape_apple_metadata(squigly_link)
            except RateLimitException:
                print(f"      [429] Squigly rate limited.", flush=True)
                SQUIGLY_COOLDOWN_UNTIL = time.time() + 120

        elapsed = time.time() - start_ts

        # 6. RETURN SUCCESS
        if final_meta:
            # Clean genres logic is inside scrape_apple_metadata, so we trust it
            print(f"   [FOUND] {spotify_id} -> {final_meta['date']} | Genres: {final_meta['genres']} ({elapsed:.2f}s)", flush=True)
            return {
                'isrc': isrc,
                'track_id': spotify_id,
                'apple_music_genres': json.dumps(final_meta['genres']),
                'updated_at': int(time.time() / 86400)
            }
        
        # If we reached here, no data found this attempt.
        # Since we didn't hit a `continue` (Rate Limit), we assume legitimate "Not Found".
        break

    print(f"   [SKIP] No Apple data found for {spotify_id}", flush=True)
    return None

BATCH_SIZE = 100

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

    continuous_mode = (PROCESS_LIMIT == 0)

    print(f"--- Starting Job (Worker {WORKER_INDEX}/{TOTAL_WORKERS} | Continuous: {continuous_mode}) ---", flush=True)

    while (time.time() - START_TIME) < MAX_RUNTIME_SECONDS:
        current_limit = 50 if continuous_mode else PROCESS_LIMIT

        print(f"--- 1. Fetching tracks (Limit: {current_limit}) ---", flush=True)

        try:
            # --- EDITED: Pass sharding params to API ---
            payload = {
                "limit": current_limit,
                "worker_index": WORKER_INDEX,
                "total_workers": TOTAL_WORKERS
            }
            res = requests.post(f"{WORKER_URL}/genres/find-missing-apple", json=payload, timeout=30)
            res.raise_for_status()
            data = res.json()
            tracks = data.get('tracks', [])
        except Exception as e:
            print(f"Failed to fetch job: {e}", flush=True)
            if continuous_mode:
                time.sleep(60)
                continue
            else:
                return

        if not tracks:
            if continuous_mode:
                print("No tracks found. Sleeping 5 minutes before checking again...", flush=True)
                time.sleep(5 * 60)
                continue
            else:
                print("No tracks need updating.", flush=True)
                return

        print(f"Processing {len(tracks)} tracks...", flush=True)

        updates = []
        total_sent = 0

        for i, t in enumerate(tracks):
            # 1. Start timer for this track
            track_start_time = time.time()

            if (time.time() - START_TIME) >= MAX_RUNTIME_SECONDS:
                print(f"--- TIME LIMIT REACHED - Stopping gracefully ---", flush=True)
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
                        'updated_at': int(time.time() / 86400)
                    })
            except Exception as e:
                print(f"Error processing {t['id']}: {e}", flush=True)

            if len(updates) >= BATCH_SIZE:
                print(f"--- Reached {BATCH_SIZE} tracks (Total processed: {i + 1}/{len(tracks)}) ---", flush=True)
                if send_updates_to_turso(updates):
                    total_sent += len(updates)
                    updates = []
                else:
                    print("Batch failed, will retry with next batch", flush=True)

            # 2. Smart Delay: Ensure total time taken is at least MIN_TRACK_DURATION
            elapsed_track = time.time() - track_start_time
            if elapsed_track < MIN_TRACK_DURATION:
                time.sleep(MIN_TRACK_DURATION - elapsed_track)

        if updates:
            print(f"--- 2. Sending final batch of {len(updates)} updates to Turso ---", flush=True)
            if send_updates_to_turso(updates):
                total_sent += len(updates)

        print(f"--- Cycle Done: Sent {total_sent} tracks ---", flush=True)

        if not continuous_mode:
            break
        
if __name__ == "__main__":
    run_job()
