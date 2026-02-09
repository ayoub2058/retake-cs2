import os
import sys
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

SCRIPTS_DIR = os.path.dirname(__file__)
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from parse_match import get_db_url, insert_match_row, insert_player_stats, parse_stats

load_dotenv(".env.local")
load_dotenv()


def fetch_matches(
    cursor: RealDictCursor,
    match_id: Optional[int],
    limit: int,
    force: bool,
) -> List[Dict[str, Any]]:
    if match_id is not None:
        cursor.execute(
            """
            select
                mtd.id,
                mtd.user_id,
                mtd.file_path,
                mtd.status,
                m.id as match_row_id,
                m.map_name,
                coalesce(stats.stats_count, 0) as stats_count
            from public.matches_to_download mtd
            left join public.matches m
                on m.match_id::text = mtd.id::text
            left join (
                select match_id, count(*) as stats_count
                from public.player_match_stats
                group by match_id
            ) stats
                on stats.match_id = m.id
            where mtd.id = %s
            """,
            (match_id,),
        )
        row = cursor.fetchone()
        return [row] if row else []

    cursor.execute(
        """
        select
            mtd.id,
            mtd.user_id,
            mtd.file_path,
            mtd.status,
            m.id as match_row_id,
            m.map_name,
            coalesce(stats.stats_count, 0) as stats_count
        from public.matches_to_download mtd
        left join public.matches m
            on m.match_id::text = mtd.id::text
        left join (
            select match_id, count(*) as stats_count
            from public.player_match_stats
            group by match_id
        ) stats
            on stats.match_id = m.id
        where mtd.status in ('downloaded', 'processed', 'parsed', 'notified')
          and (
            %s
            or m.id is null
            or m.map_name is null
            or stats.stats_count is null
            or stats.stats_count = 0
          )
        order by mtd.id asc
        limit %s
        """,
        (force, limit),
    )
    return cursor.fetchall()


def mark_parsed(cursor: RealDictCursor, match_id: int) -> None:
    cursor.execute(
        """
        update public.matches_to_download
        set status = 'parsed'
        where id = %s
        """,
        (match_id,),
    )


def should_parse(match: Dict[str, Any], force: bool) -> bool:
    if force:
        return True
    if match.get("match_row_id") is None:
        return True
    if match.get("map_name") is None:
        return True
    stats_count = match.get("stats_count")
    if stats_count is None:
        return True
    return int(stats_count) <= 0


def parse_match_row(cursor: RealDictCursor, match: Dict[str, Any]) -> None:
    match_id = int(match["id"])
    steam_id = str(match["user_id"])
    file_path = match.get("file_path")

    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"Demo file not found: {file_path}")

    stats = parse_stats(file_path, steam_id, match_id)
    match_meta = stats.get("match_meta", {})
    players_stats = stats.get("players_stats", [])

    match_row_id = insert_match_row(cursor, match_meta, match_id)
    insert_player_stats(cursor, match_row_id, players_stats)

    if match.get("status") == "downloaded":
        mark_parsed(cursor, match_id)


def main() -> None:
    db_url = get_db_url()
    if not db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL (or DATABASE_URL/SUPABASE_DATABASE_URL) must be set"
        )

    match_id = None
    if len(sys.argv) > 1:
        try:
            match_id = int(sys.argv[1])
        except ValueError as exc:
            raise RuntimeError("Match id must be an integer") from exc

    limit = int(os.getenv("PARSE_MATCH_DETAILS_LIMIT", "10"))
    force = os.getenv("PARSE_MATCH_DETAILS_FORCE", "0").strip().lower() in {
        "1",
        "true",
        "yes",
    }

    with psycopg2.connect(db_url) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            matches = fetch_matches(cursor, match_id, limit, force)
            if not matches:
                print("No downloaded matches to parse.")
                return

            for match in matches:
                try:
                    if not should_parse(match, force):
                        print(f"Skipping match {match['id']} (already complete).")
                        continue
                    parse_match_row(cursor, match)
                    conn.commit()
                    print(f"Parsed match {match['id']}.")
                except Exception as exc:
                    conn.rollback()
                    print(f"Failed to parse match {match['id']}: {exc}")


if __name__ == "__main__":
    main()
