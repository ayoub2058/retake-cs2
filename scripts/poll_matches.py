import os
import sys
import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Iterable, List, Optional

# Fix Windows console encoding for Unicode player names
if sys.platform == "win32":
    for _s in (sys.stdout, sys.stderr):
        if hasattr(_s, "reconfigure"):
            _s.reconfigure(encoding="utf-8", errors="replace")

try:
    from dotenv import load_dotenv

    load_dotenv(".env.local")
    load_dotenv()
except ImportError:
    pass

import psycopg2
from psycopg2.extras import RealDictCursor
import requests

API_URL = "https://api.steampowered.com/ICSGOPlayers_730/GetNextMatchSharingCode/v1"
MAX_MATCHES_PER_USER = int(os.getenv("MAX_MATCHES_PER_USER", "50"))
POLL_INTERVAL_SECONDS = int(os.getenv("POLL_INTERVAL_SECONDS", "0"))
LATEST_ONLY = os.getenv("LATEST_ONLY", "true").strip().lower() in (
    "1",
    "true",
    "yes",
)


def get_db_url() -> str:
    return (
        os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
        or ""
    )


def has_column(cursor: RealDictCursor, table: str, column: str) -> bool:
    cursor.execute(
        """
        select 1
        from information_schema.columns
        where table_schema = 'public'
          and table_name = %s
          and column_name = %s
        """,
        (table, column),
    )
    return cursor.fetchone() is not None


def fetch_users(cursor: RealDictCursor, use_auth_code_valid: bool) -> Iterable[Dict[str, Any]]:
    if use_auth_code_valid:
        cursor.execute(
            """
            select steam_id, auth_code, last_known_match_code
            from public.users
            where auth_code is not null
              and auth_code <> ''
              and (auth_code_valid is null or auth_code_valid = true)
            """
        )
    else:
        cursor.execute(
            """
            select steam_id, auth_code, last_known_match_code
            from public.users
            where auth_code is not null
              and auth_code <> ''
            """
        )
    return cursor.fetchall()


def flag_auth_invalid(
    cursor: RealDictCursor, steam_id: int, use_auth_code_valid: bool
) -> None:
    if use_auth_code_valid:
        cursor.execute(
            """
            update public.users
            set auth_code_valid = false
            where steam_id = %s
            """,
            (steam_id,),
        )
    else:
        cursor.execute(
            """
            update public.users
            set auth_code = null
            where steam_id = %s
            """,
            (steam_id,),
        )


def insert_match(
    cursor: RealDictCursor, share_code: str, steam_id: int
) -> None:
    cursor.execute(
        """
        insert into public.matches_to_download (share_code, status, user_id)
        values (%s, %s, %s)
        """,
        (share_code, "pending", steam_id),
    )


def update_last_known(cursor: RealDictCursor, share_code: str, steam_id: int) -> None:
    cursor.execute(
        """
        update public.users
        set last_known_match_code = %s
        where steam_id = %s
        """,
        (share_code, steam_id),
    )


def build_params(
    api_key: str, steam_id: int, auth_code: str, known_code: Optional[str]
) -> Dict[str, str]:
    params = {
        "key": api_key,
        "steamid": str(steam_id),
        "steamidkey": auth_code,
    }
    if known_code:
        params["knowncode"] = known_code
    return params


def parse_share_code(payload: Dict[str, Any]) -> Optional[str]:
    result = payload.get("result")
    if not isinstance(result, dict):
        return None
    next_code = result.get("nextcode") or result.get("next_code")
    if isinstance(next_code, str) and next_code:
        return next_code
    return None


def redact_value(value: Optional[str], keep: int = 4) -> str:
    if not value:
        return "<empty>"
    if len(value) <= keep:
        return "*" * len(value)
    return f"{value[:keep]}...{value[-keep:]}"


POLL_CONCURRENCY = int(os.getenv("POLL_CONCURRENCY", "5"))


def log(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Poller] {msg}", flush=True)


def poll_user(
    api_key: str,
    db_url: str,
    user: Dict[str, Any],
    use_auth_code_valid: bool,
) -> int:
    """Poll a single user for new matches. Returns number of matches found."""
    steam_id = int(user["steam_id"])
    auth_code = str(user["auth_code"]).strip()
    known_code = user.get("last_known_match_code")
    if isinstance(known_code, str):
        known_code = known_code.strip()

    if not known_code:
        log(
            f"Missing known match code for {steam_id}. "
            "Set last_known_match_code in the users table first."
        )
        return 0

    fetched = 0
    latest_only_code: Optional[str] = None
    session = requests.Session()

    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            while fetched < MAX_MATCHES_PER_USER:
                params = build_params(api_key, steam_id, auth_code, known_code)
                try:
                    response = session.get(API_URL, params=params, timeout=15)
                except requests.RequestException as exc:
                    log(f"Steam API error for {steam_id}: {exc}")
                    break

                if response.status_code == 200:
                    payload = response.json()
                    share_code = parse_share_code(payload)
                    if not share_code:
                        log(f"No share code returned for {steam_id}.")
                        break

                    if share_code == known_code:
                        log(f"No new match for {steam_id}.")
                        break

                    known_code = share_code
                    update_last_known(cursor, share_code, steam_id)
                    conn.commit()
                    fetched += 1
                    if LATEST_ONLY:
                        latest_only_code = share_code
                    else:
                        insert_match(cursor, share_code, steam_id)
                        conn.commit()
                        log(f"New match for {steam_id}: {share_code}")
                    continue

                if response.status_code == 202:
                    if fetched == 0:
                        log(f"No new match for {steam_id}.")
                    if LATEST_ONLY and latest_only_code:
                        insert_match(cursor, latest_only_code, steam_id)
                        conn.commit()
                        log(
                            f"Latest match for {steam_id}: {latest_only_code}"
                        )
                    break

                if response.status_code in (401, 403):
                    flag_auth_invalid(cursor, steam_id, use_auth_code_valid)
                    conn.commit()
                    log(f"Auth invalid for {steam_id}; flagged.")
                    break

                safe_params = {
                    "steamid": str(steam_id),
                    "steamidkey": redact_value(auth_code),
                    "knowncode": redact_value(known_code),
                }
                body_preview = response.text[:500]
                if response.status_code == 412:
                    log(f"Precondition failed for {steam_id} (412).")
                    log(f"Params: {safe_params}")
                    log(f"Headers: {dict(response.headers)}")
                    log(f"Body: {body_preview}")
                    break

                log(
                    "Unexpected status for"
                    f" {steam_id}: {response.status_code}"
                )
                log(f"Params: {safe_params}")
                log(f"Body: {body_preview}")
                break

    return fetched


def poll_once(api_key: str, db_url: str) -> None:
    """Run a single poll cycle for all users concurrently."""
    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            use_auth_code_valid = has_column(cursor, "users", "auth_code_valid")
            users = fetch_users(cursor, use_auth_code_valid)

    if not users:
        log("No users with auth_code to poll.")
        return

    user_count = len(users)
    workers = min(POLL_CONCURRENCY, user_count)
    log(f"Polling {user_count} user(s) with {workers} worker(s)...")

    total_matches = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(poll_user, api_key, db_url, user, use_auth_code_valid): user
            for user in users
        }
        for future in as_completed(futures):
            user = futures[future]
            try:
                found = future.result()
                total_matches += found
            except Exception as exc:
                steam_id = user.get("steam_id", "?")
                log(f"Error polling user {steam_id}: {exc}")

    log(f"Cycle complete. {user_count} user(s) checked, {total_matches} new match(es).")


def main() -> None:
    api_key = os.getenv("STEAM_API_KEY")
    db_url = get_db_url()

    if not api_key:
        raise RuntimeError("STEAM_API_KEY must be set")
    if not db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL (or DATABASE_URL/SUPABASE_DATABASE_URL) must be set"
        )

    interval = max(POLL_INTERVAL_SECONDS, 60)  # min 60s between polls
    log("=" * 50)
    log("Match Poller daemon started")
    log(f"  Poll interval : {interval}s")
    log(f"  Concurrency   : {POLL_CONCURRENCY} workers")
    log(f"  Latest only   : {LATEST_ONLY}")
    log(f"  Max per user  : {MAX_MATCHES_PER_USER}")
    log("=" * 50)

    while True:
        try:
            poll_once(api_key, db_url)
        except Exception as exc:
            log(f"Poll cycle error: {exc}")
        log(f"Sleeping {interval}s...")
        time.sleep(interval)


if __name__ == "__main__":
    main()
