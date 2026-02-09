import os
from typing import Any, Dict, Iterable, Optional

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


def main() -> None:
    api_key = os.getenv("STEAM_API_KEY")
    db_url = get_db_url()

    if not api_key:
        raise RuntimeError("STEAM_API_KEY must be set")
    if not db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL (or DATABASE_URL/SUPABASE_DATABASE_URL) must be set"
        )

    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            use_auth_code_valid = has_column(cursor, "users", "auth_code_valid")
            users = fetch_users(cursor, use_auth_code_valid)
            if not users:
                print("No users with auth_code to poll.")
                return

            session = requests.Session()

            for user in users:
                steam_id = int(user["steam_id"])
                auth_code = str(user["auth_code"]).strip()
                known_code = user.get("last_known_match_code")
                if isinstance(known_code, str):
                    known_code = known_code.strip()

                fetched = 0
                if not known_code:
                    print(
                        f"Missing known match code for {steam_id}. "
                        "Set last_known_match_code in the users table first."
                    )
                    continue
                latest_only_code: Optional[str] = None
                while fetched < MAX_MATCHES_PER_USER:
                    params = build_params(api_key, steam_id, auth_code, known_code)
                    try:
                        response = session.get(API_URL, params=params, timeout=15)
                    except requests.RequestException as exc:
                        print(f"Steam API error for {steam_id}: {exc}")
                        break

                    if response.status_code == 200:
                        payload = response.json()
                        share_code = parse_share_code(payload)
                        if not share_code:
                            print(f"No share code returned for {steam_id}.")
                            break

                        if share_code == known_code:
                            print(f"No new match for {steam_id}.")
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
                            print(f"New match for {steam_id}: {share_code}")
                        continue

                    if response.status_code == 202:
                        if fetched == 0:
                            print(f"No new match for {steam_id}.")
                        if LATEST_ONLY and latest_only_code:
                            insert_match(cursor, latest_only_code, steam_id)
                            conn.commit()
                            print(
                                f"Latest match for {steam_id}: {latest_only_code}"
                            )
                        break

                    if response.status_code in (401, 403):
                        flag_auth_invalid(cursor, steam_id, use_auth_code_valid)
                        conn.commit()
                        print(f"Auth invalid for {steam_id}; flagged.")
                        break

                    safe_params = {
                        "steamid": str(steam_id),
                        "steamidkey": redact_value(auth_code),
                        "knowncode": redact_value(known_code),
                    }
                    body_preview = response.text[:500]
                    if response.status_code == 412:
                        print(f"Precondition failed for {steam_id} (412).")
                        print(f"Params: {safe_params}")
                        print(f"Headers: {dict(response.headers)}")
                        print(f"Body: {body_preview}")
                        break

                    print(
                        "Unexpected status for"
                        f" {steam_id}: {response.status_code}"
                    )
                    print(f"Params: {safe_params}")
                    print(f"Body: {body_preview}")
                    break


if __name__ == "__main__":
    main()
