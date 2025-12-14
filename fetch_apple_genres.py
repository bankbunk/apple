import os
import requests
import json
import re
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
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

PROCESS_LIMIT = 0

START_TIME = time.time()
MAX_RUNTIME_SECONDS = 50 * 60

GENRES_TO_KEEP_WHOLE = [
    "singer/songwriter",
    "adult/contemporary"
]

SQUIGLY_COOLDOWN_UNTIL = 0
ODESLI_COOLDOWN_UNTIL = 0
TAPELINK_COOLDOWN_UNTIL = 0

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
# METHOD 2: TAPELINK.IO
# =============================================================================
def resolve_tapelink(spotify_url):
    global TAPELINK_COOLDOWN_UNTIL
    
    # Check cooldown
    if time.time() < TAPELINK_COOLDOWN_UNTIL:
        print(f"      [Tapelink] On cooldown, skipping", flush=True)
        return None
    
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
        
        if response.status_code == 429: 
            raise RateLimitException("Tapelink")
        
        if response.status_code != 200:
            print(f"      [Tapelink] API returned {response.status_code}", flush=True)
            return None
        
        data = response.json()

        if not data.get('success'):
            error_msg = data.get('error', data.get('message', 'unknown'))
            print(f"      [Tapelink] API success=false: {error_msg}", flush=True)
            # Check if this looks like rate limiting
            if 'rate' in str(error_msg).lower() or 'limit' in str(error_msg).lower():
                raise SoftRateLimitException("Tapelink rate limited")
            return None

        share_link_stub = data.get('shareableLink')
        if not share_link_stub:
            print(f"      [Tapelink] No shareableLink in response", flush=True)
            return None

        full_share_url = f"https://{share_link_stub}" if not share_link_stub.startswith("http") else share_link_stub

        # Step 2: Scrape Data
        page_response = session.get(full_share_url, headers=headers, timeout=10)
        
        if page_response.status_code == 429: 
            raise RateLimitException("Tapelink Page")
        
        if page_response.status_code != 200:
            print(f"      [Tapelink] Page returned {page_response.status_code}", flush=True)
            return None

        soup = BeautifulSoup(page_response.text, 'html.parser')
        next_data_tag = soup.find('script', id='__NEXT_DATA__')
        
        if not next_data_tag:
            print(f"      [Tapelink] No NEXT_DATA on page", flush=True)
            return None

        json_data = json.loads(next_data_tag.string)
        initial_data = json_data['props']['pageProps']['initialSongData']
        platforms = initial_data.get('platforms', {})
        apple_link = platforms.get('apple_music')
        
        if apple_link:
            print(f"      [Tapelink] Found Apple link", flush=True)
        else:
            print(f"      [Tapelink] No apple_music in platforms: {list(platforms.keys())}", flush=True)
        
        return apple_link

    except RateLimitException: 
        raise
    except SoftRateLimitException:
        raise
    except KeyError as e:
        print(f"      [Tapelink] Missing key: {e}", flush=True)
        return None
    except Exception as e:
        print(f"      [Tapelink] Error: {e}", flush=True)
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
        
        slug = response.json().get('slug')
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
        apple_url = result_data.get('services', {}).get('apple', {}).get('url')
        
        if apple_url:
            print(f"      [Squigly] Found Apple link", flush=True)
        else:
            print(f"      [Squigly] No Apple in services", flush=True)
            
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
    global SQUIGLY_COOLDOWN_UNTIL, ODESLI_COOLDOWN_UNTIL, TAPELINK_COOLDOWN_UNTIL
    spotify_url = f"https://open.spotify.com/track/{spotify_id}"
    
    print(f"   [Processing] {spotify_id}", flush=True)

    def check_provider(resolver_func, provider_name):
        try:
            link = resolver_func(spotify_url)
            if link:
                return scrape_apple_metadata(link)
        except (RateLimitException, SoftRateLimitException):
            raise
        except Exception as e:
            print(f"      [{provider_name}] Unexpected: {e}", flush=True)
        return None

    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        start_ts = time.time()
        results = []
        should_retry = False

        # PHASE 1: Primary Providers in Parallel (Odesli & Tapelink)
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_to_provider = {}
            
            if time.time() > ODESLI_COOLDOWN_UNTIL:
                future_to_provider[executor.submit(check_provider, resolve_odesli, "Odesli")] = "Odesli"
            else:
                print(f"      [Odesli] On cooldown, skipping", flush=True)
                
            if time.time() > TAPELINK_COOLDOWN_UNTIL:
                future_to_provider[executor.submit(check_provider, resolve_tapelink, "Tapelink")] = "Tapelink"
            else:
                print(f"      [Tapelink] On cooldown, skipping", flush=True)

            for future in as_completed(future_to_provider):
                provider_name = future_to_provider[future]
                try:
                    data = future.result()
                    if data:
                        results.append(data)
                except RateLimitException:
                    print(f"      [429] {provider_name} rate limited. Cooldown 2 min.", flush=True)
                    if provider_name == "Odesli":
                        ODESLI_COOLDOWN_UNTIL = time.time() + 120
                    else:
                        TAPELINK_COOLDOWN_UNTIL = time.time() + 120
                    should_retry = True
                except SoftRateLimitException:
                    print(f"      [SOFT 429] {provider_name} soft rate limited. Cooldown 2 min.", flush=True)
                    if provider_name == "Odesli":
                        ODESLI_COOLDOWN_UNTIL = time.time() + 120
                    else:
                        TAPELINK_COOLDOWN_UNTIL = time.time() + 120
                    should_retry = True
                except Exception as e:
                    print(f"      [{provider_name}] Error: {e}", flush=True)

        # PHASE 2: Fallback (Squigly) if no results yet
        if not results and time.time() > SQUIGLY_COOLDOWN_UNTIL:
            try:
                print(f"      Trying Squigly (fallback)...", flush=True)
                link = resolve_squigly(spotify_url)
                if link:
                    meta = scrape_apple_metadata(link)
                    if meta:
                        results.append(meta)
            except RateLimitException:
                print(f"      [429] Squigly rate limited. Cooldown 2 min.", flush=True)
                SQUIGLY_COOLDOWN_UNTIL = time.time() + 120
            except Exception as e:
                print(f"      [Squigly] Unexpected: {e}", flush=True)

        elapsed = time.time() - start_ts

        # If we got results, return them
        if results:
            results.sort(key=lambda x: x['date'] if x['date'] else '9999-99-99')
            best_match = results[0]
            print(f"   [FOUND] {spotify_id} -> {best_match['date']} | Genres: {best_match['genres']} ({elapsed:.2f}s)", flush=True)
            return {
                'isrc': isrc,
                'track_id': spotify_id,
                'apple_music_genres': json.dumps(best_match['genres']),
                'updated_at': int(time.time())
            }

        # If providers rate limited, wait and retry
        if should_retry:
            retry_count += 1
            if retry_count < max_retries:
                wait_time = min(30 * retry_count, 120)
                print(f"   [RETRY] Waiting {wait_time}s before retry {retry_count}/{max_retries}...", flush=True)
                time.sleep(wait_time)
                continue
        
        break

    print(f"   [SKIP] No Apple data found for {spotify_id} ({elapsed:.2f}s)", flush=True)
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

    print(f"--- Starting Job (Continuous: {continuous_mode}, Max Runtime: {MAX_RUNTIME_SECONDS}s) ---", flush=True)

    while (time.time() - START_TIME) < MAX_RUNTIME_SECONDS:
        current_limit = 50 if continuous_mode else PROCESS_LIMIT

        print(f"--- 1. Fetching tracks (Limit: {current_limit}) ---", flush=True)

        try:
            res = requests.post(f"{WORKER_URL}/genres/find-missing-apple", json={"limit": current_limit}, timeout=30)
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
                        'updated_at': int(time.time())
                    })
            except Exception as e:
                print(f"Error processing {t['id']}: {e}", flush=True)

            # ADD DELAY between tracks to avoid rate limits
            time.sleep(REQUEST_DELAY)

            if len(updates) >= BATCH_SIZE:
                print(f"--- Reached {BATCH_SIZE} tracks (Total processed: {i + 1}/{len(tracks)}) ---", flush=True)
                if send_updates_to_turso(updates):
                    total_sent += len(updates)
                    updates = []
                else:
                    print("Batch failed, will retry with next batch", flush=True)

        if updates:
            print(f"--- 2. Sending final batch of {len(updates)} updates to Turso ---", flush=True)
            if send_updates_to_turso(updates):
                total_sent += len(updates)

        print(f"--- Cycle Done: Sent {total_sent} tracks ---", flush=True)

        if not continuous_mode:
            break

if __name__ == "__main__":
    run_job()
