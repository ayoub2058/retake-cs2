import os
import re
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

# Fix Windows console encoding for Unicode player names
if sys.platform == "win32":
    for _s in (sys.stdout, sys.stderr):
        if hasattr(_s, "reconfigure"):
            _s.reconfigure(encoding="utf-8", errors="replace")

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from groq import Groq
from awpy.demo import DemoParser
import pandas as pd
from sqlalchemy.pool import QueuePool

# Import stats_card from the same directory as this script
import importlib.util as _ilu
_stats_card_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stats_card.py")
if os.path.exists(_stats_card_path):
    _spec = _ilu.spec_from_file_location("stats_card", _stats_card_path)
    _mod = _ilu.module_from_spec(_spec)
    try:
        _spec.loader.exec_module(_mod)
        generate_stats_image = _mod.generate_stats_image
    except Exception as _e:
        print(f"Warning: could not load stats_card module: {_e}")
        generate_stats_image = None
else:
    generate_stats_image = None

load_dotenv(".env.local")
load_dotenv()

MODEL_NAME = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
PARSE_POLL_INTERVAL = int(os.getenv("PARSE_POLL_INTERVAL", "3"))
PARSE_BATCH_SIZE = int(os.getenv("PARSE_BATCH_SIZE", "15"))
PARSE_WORKERS = int(os.getenv("PARSE_WORKERS", "5"))
PARSE_RATE_LIMIT = int(os.getenv("PARSE_RATE_LIMIT", "15"))
PARSE_DB_POOL = int(os.getenv("PARSE_DB_POOL", "10"))
PARSE_DB_OVERFLOW = int(os.getenv("PARSE_DB_OVERFLOW", "20"))

_rate_limiter = None


def log(msg: str) -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [Parser] {msg}", flush=True)


def get_db_url() -> str:
    return (
        os.getenv("SUPABASE_DB_URL")
        or os.getenv("DATABASE_URL")
        or os.getenv("SUPABASE_DATABASE_URL")
        or ""
    )


def fetch_matches(
    cursor: RealDictCursor, match_id: Optional[int], limit: int = 10
) -> List[Dict[str, Any]]:
    if match_id is not None:
        cursor.execute(
            """
                        select m.id,
                               m.user_id,
                               m.file_path,
                               u.username,
                               u.language,
                               u.coach_style
                        from public.matches_to_download m
                        left join public.users u
                            on u.steam_id = m.user_id
                        where m.id = %s
            """,
            (match_id,),
        )
        row = cursor.fetchone()
        return [row] if row else []

    cursor.execute(
        """
                select m.id,
                       m.user_id,
                       m.file_path,
                       u.username,
                       u.language,
                       u.coach_style
                from public.matches_to_download m
                left join public.users u
                    on u.steam_id = m.user_id
                where m.status in ('downloaded', 'processed')
                    and m.coach_tip is null
                order by m.id asc
                limit %s
        """,
        (limit,),
    )
    return cursor.fetchall()


def mark_parsed(
    cursor: RealDictCursor,
    match_id: int,
    tip: str,
    tip_image_url: Optional[str] = None,
) -> None:
    cursor.execute(
        """
        update public.matches_to_download
        set coach_tip = %s,
            tip_image_url = %s,
            tip_sent = false,
            status = 'processed'
        where id = %s
        """,
        (tip, tip_image_url, match_id),
    )


def mark_error(cursor: RealDictCursor, match_id: int, reason: str) -> None:
    cursor.execute(
        """
        update public.matches_to_download
        set status = 'error'
        where id = %s
        """,
        (match_id,),
    )
    print(f"Match {match_id} failed: {reason}")


CLEANUP_INTERVAL = int(os.getenv("CLEANUP_INTERVAL", "300"))  # seconds between cleanup runs


def cleanup_demo_files(pool) -> int:
    """Delete demo files for matches that are fully processed (notified).

    Only deletes when:
      - status = 'notified' (tip sent to player)
      - tip_sent = true
      - file_path exists on disk

    Returns the number of files deleted.
    """
    conn = pool.connect()
    deleted = 0
    try:
        db_conn = conn.driver_connection
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                select id, file_path
                from public.matches_to_download
                where status = 'notified'
                  and tip_sent = true
                  and file_path is not null
                """
            )
            rows = cursor.fetchall()
            for row in rows:
                fpath = row.get("file_path")
                if fpath and os.path.exists(fpath):
                    try:
                        os.remove(fpath)
                        deleted += 1
                    except OSError as exc:
                        log(f"Failed to delete {fpath}: {exc}")
    finally:
        conn.close()
    return deleted


def extract_match_id_from_path(file_path: str, fallback_id: int) -> int:
    base_name = os.path.basename(file_path)
    match = re.search(r"(\d+)", base_name)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return fallback_id
    return fallback_id


def extract_map_from_path(file_path: str) -> Optional[str]:
    base_name = os.path.basename(file_path).lower()
    match = re.search(r"(de_[a-z0-9_]+)", base_name)
    if match:
        return match.group(1)
    return None


def extract_map_from_header(header: Any) -> Optional[str]:
    if not isinstance(header, dict):
        return None
    for key in ("mapName", "map_name", "map", "mapname"):
        value = header.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def insert_match_row(
    cursor: RealDictCursor,
    match_meta: Dict[str, Any],
    fallback_match_id: int,
) -> int:
    match_id_value = match_meta.get("match_id", fallback_match_id)
    match_id_text = str(match_id_value)
    cursor.execute(
        """
        select id from public.matches
        where match_id::text = %s
        """,
        (match_id_text,),
    )
    existing = cursor.fetchone()
    if existing and "id" in existing:
        cursor.execute(
            """
            update public.matches
            set map_name = coalesce(%s, map_name),
                score_t = coalesce(%s, score_t),
                score_ct = coalesce(%s, score_ct),
                winner = coalesce(%s, winner),
                duration = coalesce(%s, duration),
                match_date = coalesce(%s, match_date)
            where id = %s
            """,
            (
                match_meta.get("map_name"),
                match_meta.get("score_t"),
                match_meta.get("score_ct"),
                match_meta.get("winner"),
                match_meta.get("duration"),
                match_meta.get("match_date"),
                int(existing["id"]),
            ),
        )
        return int(existing["id"])

    cursor.execute(
        """
        insert into public.matches
        (match_id, map_name, score_t, score_ct, winner, duration, match_date)
        values (%s, %s, %s, %s, %s, %s, %s)
        on conflict (match_id) do update set
            map_name = coalesce(excluded.map_name, matches.map_name),
            score_t = coalesce(excluded.score_t, matches.score_t),
            score_ct = coalesce(excluded.score_ct, matches.score_ct),
            winner = coalesce(excluded.winner, matches.winner),
            duration = coalesce(excluded.duration, matches.duration),
            match_date = coalesce(excluded.match_date, matches.match_date)
        returning id
        """,
        (
            match_id_text,
            match_meta.get("map_name"),
            match_meta.get("score_t"),
            match_meta.get("score_ct"),
            match_meta.get("winner"),
            match_meta.get("duration"),
            match_meta.get("match_date"),
        ),
    )
    inserted = cursor.fetchone()
    if not inserted or "id" not in inserted:
        raise RuntimeError("Failed to insert match row")
    return int(inserted["id"])


def insert_player_stats(
    cursor: RealDictCursor,
    match_row_id: int,
    player_stats: List[Dict[str, Any]],
) -> None:
    if not player_stats:
        return
    cursor.execute(
        """
        delete from public.player_match_stats
        where match_id = %s
        """,
        (match_row_id,),
    )
    rows = []
    for player in player_stats:
        rows.append(
            (
                match_row_id,
                str(player.get("steam_id") or ""),
                player.get("player_name"),
                player.get("team_side"),
                player.get("player_team"),
                int(player.get("kills") or 0),
                int(player.get("deaths") or 0),
                int(player.get("assists") or 0),
                float(player.get("adr") or 0.0),
                float(player.get("hs_percent") or 0.0),
                int(player.get("opening_kills") or 0),
                int(player.get("opening_deaths") or 0),
                int(player.get("trade_kills") or 0),
                int(player.get("utility_damage") or 0),
            )
        )

    cursor.executemany(
        """
        insert into public.player_match_stats
        (match_id, steam_id, player_name, team_side, player_team, kills, deaths, assists, adr,
         hs_percent, opening_kills, opening_deaths, trade_kills, utility_damage)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


def insert_rounds(
    cursor: RealDictCursor,
    match_row_id: int,
    rounds: List[Dict[str, Any]],
) -> None:
    if not rounds:
        return
    cursor.execute(
        """
        delete from public.rounds
        where match_id = %s
        """,
        (match_row_id,),
    )
    rows = []
    for round_info in rounds:
        rows.append(
            (
                match_row_id,
                int(round_info.get("round_number"))
                if round_info.get("round_number") is not None
                else None,
                round_info.get("winner_side"),
                round_info.get("reason"),
                int(round_info.get("ct_score"))
                if round_info.get("ct_score") is not None
                else None,
                int(round_info.get("t_score"))
                if round_info.get("t_score") is not None
                else None,
            )
        )

    cursor.executemany(
        """
        insert into public.rounds
        (match_id, round_number, winner_side, reason, ct_score, t_score)
        values (%s, %s, %s, %s, %s, %s)
        """,
        rows,
    )


class RateLimiter:
    def __init__(self, max_calls: int, period_seconds: int) -> None:
        self.max_calls = max_calls
        self.period_seconds = period_seconds
        self.lock = threading.Lock()
        self.calls: Deque[float] = deque()

    def acquire(self) -> None:
        while True:
            with self.lock:
                now = time.time()
                while self.calls and now - self.calls[0] >= self.period_seconds:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                sleep_for = self.period_seconds - (now - self.calls[0])
            if sleep_for > 0:
                time.sleep(sleep_for)


def is_quota_error(error: Exception) -> bool:
    return "RESOURCE_EXHAUSTED" in str(error)


def parse_retry_after_seconds(error: Exception) -> Optional[int]:
    message = str(error)
    for token in ("retryDelay", "retry in"):
        if token in message:
            break
    else:
        return None

    for part in message.replace("'", " ").replace("\"", " ").split():
        if part.endswith("s") and part[:-1].replace(".", "", 1).isdigit():
            try:
                return max(1, int(float(part[:-1])))
            except ValueError:
                continue
    return None


def set_rate_limiter(limiter: Optional[RateLimiter]) -> None:
    global _rate_limiter
    _rate_limiter = limiter


def parse_stats(
    demo_path: str, steam_id: str, match_id: int, player_name: Optional[str] = None
) -> Dict[str, Any]:
    try:
        parser = DemoParser(demo_path, demo_id=str(match_id), parse_rate=128)
    except TypeError:
        parser = DemoParser(demo_path)

    def find_col(df, candidates):
        for name in candidates:
            if name in df.columns:
                return name
        return None

    def normalize_side(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            value_upper = value.strip().upper()
            if value_upper in {"CT", "COUNTER-TERRORIST", "COUNTERTERRORIST"}:
                return "CT"
            if value_upper in {"T", "TERRORIST"}:
                return "T"
        if isinstance(value, (int, float)):
            if int(value) == 3:
                return "CT"
            if int(value) == 2:
                return "T"
        return None

    def normalize_reason(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            reason_value = value.strip()
            return reason_value or None
        if isinstance(value, (int, float)):
            return str(int(value))
        return str(value)

    def parse_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    def extract_side(entry: Dict[str, Any], prefix: str) -> Optional[str]:
        return (
            normalize_side(entry.get(f"{prefix}Side"))
            or normalize_side(entry.get(f"{prefix}Team"))
            or normalize_side(entry.get(f"{prefix}TeamName"))
            or normalize_side(entry.get(f"{prefix}TeamSide"))
        )

    def starting_ct_plays_as(round_num: int, mr: int = 12) -> str:
        """Return the side the 'starting-CT team' is playing on in a given round.

        CS2 uses MR12 (Competitive) or MR8 (Wingman).
        - Rounds 1..mr           → first half (starting CT stays CT)
        - Rounds mr+1..mr*2      → second half (starting CT plays T)
        - Overtime MR3: each OT has two 3-round halves that alternate.
        """
        regulation = mr * 2
        if round_num <= mr:
            return "CT"
        elif round_num <= regulation:
            return "T"
        else:
            # Overtime: MR3 (6 rounds per OT, sides swap every 3)
            ot_round_0 = round_num - regulation - 1  # 0-indexed within OT
            ot_half = ot_round_0 // 3
            return "CT" if ot_half % 2 == 0 else "T"

    def extract_time_value(entry: Dict[str, Any]) -> float:
        for key in ("tick", "tickNum", "tick_num", "tickNumber", "time", "timeMs"):
            if key in entry and entry[key] is not None:
                try:
                    return float(entry[key])
                except (TypeError, ValueError):
                    continue
        return float("inf")

    def normalize_id(value: Any) -> Optional[str]:
        if value is None:
            return None
        value_str = str(value).strip()
        if not value_str or value_str.lower() in {"nan", "none", "null"}:
            return None
        return value_str

    def normalize_name(value: Any) -> Optional[str]:
        if value is None:
            return None
        name = str(value).strip()
        if not name or name.lower() in {"nan", "none", "null"}:
            return None
        return name

    def normalize_name_key(value: Any) -> Optional[str]:
        name = normalize_name(value)
        if not name:
            return None
        key = "".join(char.lower() for char in name if char.isalnum())
        return key or None

    def extract_player_name(entry: Dict[str, Any], prefix: str) -> Optional[str]:
        for key in (
            f"{prefix}Name",
            f"{prefix}_name",
            f"{prefix}PlayerName",
            f"{prefix}playerName",
        ):
            if key in entry:
                return normalize_name(entry.get(key))
        return None

    def extract_player_id(entry: Dict[str, Any], prefix: str) -> Optional[str]:
        for key in (
            f"{prefix}SteamID",
            f"{prefix}SteamId",
            f"{prefix}steamid",
            f"{prefix}_steamid",
            f"{prefix}SteamID64",
        ):
            if key in entry:
                return normalize_id(entry.get(key))
        return None

    def parse_event_with_fallback(parser_obj: DemoParser, event_names: List[str]) -> pd.DataFrame:
        for event_name in event_names:
            df = parser_obj.parse_event(event_name)
            if df is not None and not df.empty:
                return df
        return pd.DataFrame()

    def categorize_item(item_name: str) -> Optional[str]:
        name = item_name.strip().lower()
        if not name:
            return None
        rifles = {
            "ak47",
            "m4a1",
            "m4a1_silencer",
            "m4a4",
            "aug",
            "sg556",
            "galilar",
            "famas",
            "awp",
            "ssg08",
            "g3sg1",
            "scar20",
        }
        smgs = {"mac10", "mp9", "mp7", "mp5sd", "ump45", "p90", "bizon"}
        heavies = {"nova", "xm1014", "mag7", "m249", "negev", "sawedoff"}
        pistols = {
            "glock",
            "hkp2000",
            "usp_silencer",
            "p250",
            "fiveseven",
            "tec9",
            "cz75a",
            "deagle",
            "revolver",
            "elite",
        }
        utility = {
            "hegrenade",
            "flashbang",
            "smokegrenade",
            "molotov",
            "incgrenade",
            "decoy",
        }
        if name in rifles:
            return "primary"
        if name in smgs:
            return "primary"
        if name in heavies:
            return "primary"
        if name in pistols:
            return "pistol"
        if name in utility:
            return "utility"
        if name.endswith("knife") or name.startswith("knife"):
            return None
        return None

    def is_utility_weapon(item_name: Optional[str]) -> bool:
        if not item_name:
            return False
        utility_items = {
            "hegrenade",
            "flashbang",
            "smokegrenade",
            "molotov",
            "incgrenade",
            "decoy",
        }
        return item_name.strip().lower() in utility_items

    def attach_round_numbers(
        deaths_source: pd.DataFrame,
        round_key: Optional[str],
        time_key: Optional[str],
        rounds_source: pd.DataFrame,
        rounds_round_key: Optional[str],
        rounds_time_key: Optional[str],
    ) -> tuple[pd.DataFrame, Optional[str]]:
        if deaths_source.empty:
            return deaths_source, round_key
        if round_key:
            deaths_local = deaths_source.copy()
            deaths_local[round_key] = pd.to_numeric(deaths_local[round_key], errors="coerce")
            deaths_local = deaths_local.dropna(subset=[round_key])
            if not deaths_local.empty:
                return deaths_local, round_key
        if not time_key or rounds_source.empty or not rounds_time_key:
            return deaths_source, None
        deaths_local = deaths_source.copy()
        deaths_local[time_key] = pd.to_numeric(deaths_local[time_key], errors="coerce")
        deaths_local = deaths_local.dropna(subset=[time_key]).sort_values(time_key)
        rounds_local = rounds_source[[rounds_time_key]].copy()
        rounds_local[rounds_time_key] = pd.to_numeric(rounds_local[rounds_time_key], errors="coerce")
        rounds_local = rounds_local.dropna(subset=[rounds_time_key]).sort_values(rounds_time_key)
        rounds_local = rounds_local.reset_index(drop=True)
        rounds_local["round_index"] = rounds_local.index + 1
        if deaths_local.empty or rounds_local.empty:
            return deaths_source, None
        deaths_local = pd.merge_asof(
            deaths_local,
            rounds_local,
            left_on=time_key,
            right_on=rounds_time_key,
            direction="forward",
        )
        round_key = "round_index"
        return deaths_local.dropna(subset=[round_key]), round_key

    target_steam_id = normalize_id(steam_id)
    db_player_name = normalize_name(player_name)
    leetify_name = normalize_name(os.getenv("LEETIFY_NAME"))

    deaths_df = parser.parse_event("player_death")
    hurts_df = parser.parse_event("player_hurt")
    rounds_df = parser.parse_event("round_end")
    purchases_df = parse_event_with_fallback(parser, ["item_purchase", "item_buy"])

    if deaths_df is None:
        deaths_df = pd.DataFrame()
    if hurts_df is None:
        hurts_df = pd.DataFrame()
    if rounds_df is None:
        rounds_df = pd.DataFrame()
    if purchases_df is None:
        purchases_df = pd.DataFrame()

    data = parser.parse() if hasattr(parser, "parse") else {}
    rounds = data.get("gameRounds", []) or []
    header = None
    if hasattr(parser, "parse_header"):
        try:
            header = parser.parse_header()
        except Exception:
            header = None
    if header is None and hasattr(parser, "header"):
        header = getattr(parser, "header")
    if header is None and hasattr(parser, "demo_header"):
        header = getattr(parser, "demo_header")
    debug_demo = os.getenv("DEBUG_DEMO") == "1"

    players_by_id: Dict[str, str] = {}
    ids_by_name: Dict[str, str] = {}
    team_counts_by_id: Dict[str, Dict[str, int]] = {}
    last_team_by_id: Dict[str, str] = {}

    def record_player(name: Optional[str], steamid: Optional[str]) -> None:
        normalized_id = normalize_id(steamid)
        normalized_name = normalize_name(name)
        if normalized_id:
            existing_name = players_by_id.get(normalized_id)
            if not existing_name and normalized_name:
                players_by_id[normalized_id] = normalized_name
            elif normalized_id not in players_by_id:
                players_by_id[normalized_id] = normalized_name or ""
        if normalized_name and normalized_id:
            name_key = normalize_name_key(normalized_name)
            if name_key and name_key not in ids_by_name:
                ids_by_name[name_key] = normalized_id

    def record_player_team(steamid: Optional[str], team_value: Any) -> None:
        normalized_id = normalize_id(steamid)
        team_side = normalize_side(team_value)
        if not normalized_id or not team_side:
            return
        team_counts_by_id.setdefault(normalized_id, {"CT": 0, "T": 0})
        team_counts_by_id[normalized_id][team_side] += 1
        last_team_by_id[normalized_id] = team_side

    if isinstance(data.get("players"), list):
        for player in data.get("players", []) or []:
            if isinstance(player, dict):
                record_player_team(
                    player.get("steamID")
                    or player.get("steamId")
                    or player.get("steamid"),
                    player.get("team")
                    or player.get("teamNum")
                    or player.get("teamnum")
                    or player.get("team_side")
                    or player.get("teamSide")
                    or player.get("side"),
                )
                record_player(
                    player.get("name")
                    or player.get("playerName")
                    or player.get("username"),
                    player.get("steamID")
                    or player.get("steamId")
                    or player.get("steamid"),
                )

    for round_data in rounds:
        players_list = round_data.get("players") if isinstance(round_data, dict) else None
        if isinstance(players_list, list):
            for player in players_list:
                if isinstance(player, dict):
                    record_player_team(
                        player.get("steamID")
                        or player.get("steamId")
                        or player.get("steamid"),
                        player.get("team")
                        or player.get("teamNum")
                        or player.get("teamnum")
                        or player.get("team_side")
                        or player.get("teamSide")
                        or player.get("side"),
                    )
                    record_player(
                        player.get("name")
                        or player.get("playerName")
                        or player.get("username"),
                        player.get("steamID")
                        or player.get("steamId")
                        or player.get("steamid"),
                    )
        elif isinstance(players_list, dict):
            for player in players_list.values():
                if isinstance(player, dict):
                    record_player_team(
                        player.get("steamID")
                        or player.get("steamId")
                        or player.get("steamid"),
                        player.get("team")
                        or player.get("teamNum")
                        or player.get("teamnum")
                        or player.get("team_side")
                        or player.get("teamSide")
                        or player.get("side"),
                    )
                    record_player(
                        player.get("name")
                        or player.get("playerName")
                        or player.get("username"),
                        player.get("steamID")
                        or player.get("steamId")
                        or player.get("steamid"),
                    )

        kills_list = round_data.get("kills", []) if isinstance(round_data, dict) else []
        for kill in kills_list or []:
            if isinstance(kill, dict):
                record_player(
                    extract_player_name(kill, "attacker"),
                    extract_player_id(kill, "attacker"),
                )
                record_player(
                    extract_player_name(kill, "victim"),
                    extract_player_id(kill, "victim"),
                )

    def collect_from_events(df: pd.DataFrame) -> None:
        if df.empty:
            return
        for col in df.columns:
            col_lower = col.lower()
            if "steamid" not in col_lower:
                continue
            if "steamid" in col:
                prefix = col.rsplit("steamid", 1)[0]
            else:
                prefix = col.rsplit("SteamID", 1)[0]
            name_candidates = [
                f"{prefix}name",
                f"{prefix}Name",
                f"{prefix}_name",
                f"{prefix}_playername",
                f"{prefix}PlayerName",
            ]
            name_col = next((c for c in name_candidates if c in df.columns), None)
            if name_col:
                for steam_id_value, name_value in zip(df[col], df[name_col]):
                    record_player(name_value, steam_id_value)
            else:
                for steam_id_value in df[col]:
                    record_player(None, steam_id_value)

    if not players_by_id:
        collect_from_events(deaths_df)
        collect_from_events(hurts_df)

    if players_by_id:
        for steamid, name in sorted(
            players_by_id.items(), key=lambda item: (item[1].lower() if item[1] else "", item[0])
        ):
            display_name = name or "unknown"
            print(f"FOUND PLAYER: '{display_name}' (ID: {steamid})")
    else:
        print("FOUND PLAYER: 'unknown' (ID: none)")
        if isinstance(data, dict):
            top_keys = sorted(data.keys())
            print(f"DEBUG: parse keys={top_keys}")
            print(f"DEBUG: gameRounds count={len(rounds)}")
            if rounds:
                first_round = rounds[0] if isinstance(rounds[0], dict) else None
                if isinstance(first_round, dict):
                    print(f"DEBUG: first round keys={sorted(first_round.keys())}")
        if not deaths_df.empty:
            print(f"DEBUG: deaths_df columns={sorted(map(str, deaths_df.columns))}")
        if not hurts_df.empty:
            print(f"DEBUG: hurts_df columns={sorted(map(str, hurts_df.columns))}")

    name_for_match = db_player_name or leetify_name
    if name_for_match:
        match_key = normalize_name_key(name_for_match)
        matched_id = ids_by_name.get(match_key) if match_key else None
        if matched_id and matched_id != target_steam_id:
            print(
                f"Auto-matching name '{name_for_match}' to ID {matched_id} (was {target_steam_id})"
            )
            target_steam_id = matched_id
        elif not matched_id:
            print(f"Auto-match failed for name '{name_for_match}'")

    if target_steam_id:
        print(f"Using target SteamID: {target_steam_id}")

    attacker_col = find_col(
        deaths_df,
        [
            "attacker_steamid",
            "attackerSteamID",
            "attacker_steam_id",
            "attacker",
        ],
    )
    victim_col = find_col(
        deaths_df,
        [
            "user_steamid",
            "userid_steamid",
            "victim_steamid",
            "victim_steam_id",
            "userid",
        ],
    )
    headshot_col = find_col(
        deaths_df,
        ["headshot", "is_headshot", "isHeadshot", "headshot_bool"],
    )
    round_col = find_col(
        deaths_df,
        ["round", "round_num", "round_number", "roundNum"],
    )
    time_col = find_col(
        deaths_df,
        ["tick", "tickNum", "tick_num", "tickNumber", "time", "timeMs"],
    )
    dmg_round_col = find_col(
        hurts_df,
        ["round", "round_num", "round_number", "roundNum"],
    )
    rounds_end_tick_col = find_col(
        rounds_df,
        ["tick", "tickNum", "tick_num", "tickNumber", "time", "timeMs"],
    )
    rounds_num_col = find_col(
        rounds_df,
        ["round", "round_num", "round_number", "roundNum"],
    )
    deaths_with_round, round_key = attach_round_numbers(
        deaths_df,
        round_col,
        time_col,
        rounds_df,
        rounds_num_col,
        rounds_end_tick_col,
    )
    purchases_time_col = find_col(
        purchases_df,
        ["tick", "tickNum", "tick_num", "tickNumber", "time", "timeMs"],
    )
    purchases_round_col = find_col(
        purchases_df,
        ["round", "round_num", "round_number", "roundNum"],
    )
    purchases_with_round, purchases_round_key = attach_round_numbers(
        purchases_df,
        purchases_round_col,
        purchases_time_col,
        rounds_df,
        rounds_num_col,
        rounds_end_tick_col,
    )
    weapon_col = find_col(
        deaths_df,
        ["weapon", "weapon_name", "weaponName", "weapon_type"],
    )
    trade_col = find_col(
        deaths_df,
        ["trade", "is_trade", "isTrade", "traded"],
    )

    kills = 0
    deaths = 0
    headshots = 0
    opening_kills = 0
    opening_deaths = 0
    ct_kills = 0
    ct_deaths = 0
    t_kills = 0
    t_deaths = 0
    weapon_counts: Dict[str, int] = {}
    if attacker_col and not deaths_df.empty:
        kills = int(
            (deaths_df[attacker_col].astype(str).str.strip() == target_steam_id).sum()
        )
    if victim_col and not deaths_df.empty:
        deaths = int(
            (deaths_df[victim_col].astype(str).str.strip() == target_steam_id).sum()
        )
    if attacker_col and headshot_col and not deaths_df.empty:
        headshots = int(
            (
                (deaths_df[attacker_col].astype(str).str.strip() == target_steam_id)
                & deaths_df[headshot_col].astype(bool)
            ).sum()
        )

    dmg_attacker_col = find_col(
        hurts_df,
        ["attacker_steamid", "attackerSteamID", "attacker_steam_id", "attacker"],
    )
    dmg_value_col = find_col(
        hurts_df,
        ["dmg_health", "dmg_health_real", "health_damage", "hpDamage", "dmg"],
    )
    hurt_weapon_col = find_col(
        hurts_df,
        ["weapon", "weapon_name", "weaponName", "weapon_type", "attacker_weapon"],
    )
    damage_total = 0.0
    if dmg_attacker_col and dmg_value_col and not hurts_df.empty:
        damage_total = float(
            hurts_df.loc[
                hurts_df[dmg_attacker_col].astype(str).str.strip() == target_steam_id,
                dmg_value_col,
            ]
            .fillna(0)
            .astype(float)
            .sum()
        )

    round_count = int(len(rounds_df)) if not rounds_df.empty else 0
    if round_count == 0 and round_key and not deaths_with_round.empty:
        round_count = int(pd.to_numeric(deaths_with_round[round_key], errors="coerce").nunique())
    if round_count == 0 and dmg_round_col and not hurts_df.empty:
        round_count = int(pd.to_numeric(hurts_df[dmg_round_col], errors="coerce").nunique())

    adr = damage_total / round_count if round_count else 0.0
    hs_percent = (headshots / kills * 100) if kills else 0.0
    kd_ratio = kills / deaths if deaths else float(kills)

    common_death_round = None
    first_death_round = None
    common_death_weapon = None
    death_hs_percent = 0.0
    top_death_rounds: List[int] = []
    avg_death_time_sec: Optional[float] = None
    median_death_time_sec: Optional[float] = None
    avg_death_distance: Optional[float] = None
    if victim_col and not deaths_df.empty:
        victim_rows = deaths_with_round[
            deaths_with_round[victim_col].astype(str).str.strip() == target_steam_id
        ] if not deaths_with_round.empty else deaths_df[
            deaths_df[victim_col].astype(str).str.strip() == target_steam_id
        ]
        if round_key and not victim_rows.empty:
            round_numbers = pd.to_numeric(
                victim_rows[round_key], errors="coerce"
            ).dropna()
            if not round_numbers.empty:
                round_counts = round_numbers.value_counts()
                common_death_round = int(round_counts.idxmax())
                first_death_round = int(round_numbers.min())
                top_death_rounds = [int(value) for value in round_counts.head(3).index]
        if weapon_col and not victim_rows.empty:
            weapon_series = victim_rows[weapon_col].dropna().astype(str)
            if not weapon_series.empty:
                common_death_weapon = str(weapon_series.value_counts().idxmax())
        if headshot_col and deaths:
            death_hs_percent = (
                victim_rows[headshot_col].astype(bool).sum() / deaths * 100
            )
        if time_col and rounds_end_tick_col and not rounds_df.empty and not victim_rows.empty:
            rounds_ticks = rounds_df[[rounds_end_tick_col]].copy()
            rounds_ticks[rounds_end_tick_col] = pd.to_numeric(
                rounds_ticks[rounds_end_tick_col], errors="coerce"
            )
            rounds_ticks = rounds_ticks.dropna(subset=[rounds_end_tick_col]).sort_values(
                rounds_end_tick_col
            )
            rounds_ticks = rounds_ticks.reset_index(drop=True)
            round_starts = [0] + rounds_ticks[rounds_end_tick_col].tolist()[:-1]
            if round_key and round_key in victim_rows.columns:
                victim_rows_local = victim_rows.copy()
                victim_rows_local[time_col] = pd.to_numeric(
                    victim_rows_local[time_col], errors="coerce"
                )
                victim_rows_local = victim_rows_local.dropna(subset=[time_col])
                if not victim_rows_local.empty:
                    ticks = []
                    for _, row in victim_rows_local.iterrows():
                        round_index = int(row.get(round_key)) if row.get(round_key) else None
                        if round_index and 1 <= round_index <= len(round_starts):
                            start_tick = round_starts[round_index - 1]
                            ticks.append(float(row[time_col]) - float(start_tick))
                    if ticks:
                        avg_death_time_sec = round(sum(ticks) / len(ticks) / 128, 1)
                        median_death_time_sec = round(
                            float(pd.Series(ticks).median()) / 128, 1
                        )
        if "distance" in victim_rows.columns and not victim_rows.empty:
            distances = pd.to_numeric(victim_rows["distance"], errors="coerce").dropna()
            if not distances.empty:
                avg_death_distance = round(float(distances.mean()), 1)

    # ── Per-round analysis: kills, multi-kills, clutches ──
    multi_kill_rounds: List[Dict[str, Any]] = []
    clutch_rounds: List[Dict[str, Any]] = []
    user_round_kills: Dict[int, int] = {}  # round_num → kills by user
    user_round_deaths: List[int] = []      # rounds where user died

    for rnd_idx, round_data in enumerate(rounds):
        round_num = rnd_idx + 1
        kills_list = round_data.get("kills", []) or []
        if kills_list:
            opening_kill = min(kills_list, key=extract_time_value)
            opening_attacker = normalize_id(opening_kill.get("attackerSteamID"))
            opening_victim = normalize_id(opening_kill.get("victimSteamID"))
            if opening_attacker and opening_attacker == target_steam_id:
                opening_kills += 1
            if opening_victim and opening_victim == target_steam_id:
                opening_deaths += 1

        user_kills_this_round = 0
        user_died_this_round = False
        for kill in kills_list:
            attacker = normalize_id(kill.get("attackerSteamID"))
            victim = normalize_id(kill.get("victimSteamID"))
            weapon = kill.get("weapon") or kill.get("weaponName") or kill.get("weapon_name")
            if attacker and attacker == target_steam_id and weapon:
                weapon_key = str(weapon)
                weapon_counts[weapon_key] = weapon_counts.get(weapon_key, 0) + 1
            if attacker and attacker == target_steam_id:
                user_kills_this_round += 1
                attacker_side = extract_side(kill, "attacker")
                if attacker_side == "CT":
                    ct_kills += 1
                elif attacker_side == "T":
                    t_kills += 1
            if victim and victim == target_steam_id:
                user_died_this_round = True
                victim_side = extract_side(kill, "victim")
                if victim_side == "CT":
                    ct_deaths += 1
                elif victim_side == "T":
                    t_deaths += 1

        if user_kills_this_round > 0:
            user_round_kills[round_num] = user_kills_this_round
        if user_kills_this_round >= 3:
            multi_kill_label = {3: "3K", 4: "4K", 5: "ACE"}.get(
                user_kills_this_round, f"{user_kills_this_round}K"
            )
            multi_kill_rounds.append({
                "round": round_num,
                "kills": user_kills_this_round,
                "label": multi_kill_label,
            })
        if user_died_this_round:
            user_round_deaths.append(round_num)

        # ── Clutch detection ──
        # A clutch = user is last alive on their team facing 2+ enemies, then gets kills
        if kills_list and len(kills_list) >= 2:
            sorted_kills = sorted(kills_list, key=extract_time_value)
            # Determine user's CURRENT side this round
            user_current_side = None
            for k in sorted_kills:
                kid = normalize_id(k.get("attackerSteamID"))
                vid = normalize_id(k.get("victimSteamID"))
                if kid == target_steam_id:
                    user_current_side = extract_side(k, "attacker")
                    break
                if vid == target_steam_id:
                    user_current_side = extract_side(k, "victim")
                    break
            if user_current_side:
                t_size = team_size if team_size else 5
                teammates_alive = t_size  # including user
                enemies_alive = t_size
                user_still_alive = True
                clutch_triggered = False
                clutch_enemies = 0
                clutch_kills = 0
                for k in sorted_kills:
                    v_side = extract_side(k, "victim")
                    v_id = normalize_id(k.get("victimSteamID"))
                    a_id = normalize_id(k.get("attackerSteamID"))
                    if v_side == user_current_side:
                        if v_id == target_steam_id:
                            user_still_alive = False
                        else:
                            teammates_alive -= 1
                    elif v_side:
                        enemies_alive -= 1
                    # Check if user is now alone vs 2+ enemies
                    if (teammates_alive == 1 and user_still_alive
                            and enemies_alive >= 2 and not clutch_triggered):
                        clutch_triggered = True
                        clutch_enemies = enemies_alive
                    if clutch_triggered and a_id == target_steam_id:
                        clutch_kills += 1
                if clutch_triggered and user_still_alive and clutch_kills > 0:
                    clutch_rounds.append({
                        "round": round_num,
                        "situation": f"1v{clutch_enemies}",
                        "kills": clutch_kills,
                        "survived": True,
                    })

    if not rounds and time_col and attacker_col and victim_col and not deaths_with_round.empty and round_key:
        deaths_df_local = deaths_with_round.copy()
        deaths_df_local[time_col] = pd.to_numeric(deaths_df_local[time_col], errors="coerce")
        deaths_df_local = deaths_df_local.dropna(subset=[time_col])
        if not deaths_df_local.empty:
            first_kills = deaths_df_local.sort_values(time_col).groupby(round_key).first()
            opening_kills = int(
                (first_kills[attacker_col].astype(str).str.strip() == target_steam_id).sum()
            )
            opening_deaths = int(
                (first_kills[victim_col].astype(str).str.strip() == target_steam_id).sum()
            )
            print(
                "DEBUG: opening duels from deaths_df: "
                f"rounds={len(first_kills)}, opening_kills={opening_kills}, "
                f"opening_deaths={opening_deaths}"
            )
            sample_rows = first_kills[[attacker_col, victim_col]].head(5)
            print("DEBUG: first_kills sample:")
            print(sample_rows.to_string(index=True))
    elif debug_demo and not rounds:
        print(
            "DEBUG: opening duel fallback skipped: "
            f"round_col={round_col}, time_col={time_col}, "
            f"attacker_col={attacker_col}, victim_col={victim_col}, "
            f"deaths_rows={len(deaths_df)}"
        )

    if debug_demo and not deaths_df.empty:
        print(f"DEBUG: deaths_df columns={sorted(map(str, deaths_df.columns))}")
        print("DEBUG: deaths_df head:")
        print(deaths_df.head(5).to_string(index=False))
    if debug_demo and not rounds_df.empty:
        print(f"DEBUG: rounds_df columns={sorted(map(str, rounds_df.columns))}")
        print("DEBUG: rounds_df head:")
        print(rounds_df.head(5).to_string(index=False))

    opening_total = opening_kills + opening_deaths
    opening_win_rate = (opening_kills / opening_total * 100) if opening_total else 0.0
    side_stats_available = bool(rounds)
    ct_kd = (ct_kills / ct_deaths) if ct_deaths else float(ct_kills)
    t_kd = (t_kills / t_deaths) if t_deaths else float(t_kills)
    if not side_stats_available:
        ct_kd = None
        t_kd = None
        ct_kills = None
        t_kills = None
        ct_deaths = None
        t_deaths = None
    if debug_demo:
        print(
            "DEBUG: computed stats: "
            f"kills={kills}, deaths={deaths}, adr={round(adr,1)}, "
            f"opening_kills={opening_kills}, opening_deaths={opening_deaths}, "
            f"opening_win_rate={round(opening_win_rate,1)}, ct_kd={ct_kd}, t_kd={t_kd}"
        )
    fav_weapon = max(weapon_counts, key=weapon_counts.get) if weapon_counts else None

    match_meta: Dict[str, Any] = {}
    match_meta["match_id"] = extract_match_id_from_path(demo_path, match_id)
    match_meta["map_name"] = (
        data.get("mapName")
        or data.get("map")
        or data.get("map_name")
        or extract_map_from_header(header)
        or extract_map_from_path(demo_path)
        or None
    )
    if isinstance(match_meta["map_name"], str):
        map_label = match_meta["map_name"].strip()
        if not map_label or map_label.lower().startswith("unknown"):
            match_meta["map_name"] = None
        else:
            match_meta["map_name"] = map_label
    match_meta["match_date"] = datetime.fromtimestamp(
        os.path.getmtime(demo_path), tz=timezone.utc
    )
    match_meta["duration"] = None
    if rounds_end_tick_col and not rounds_df.empty:
        max_tick = pd.to_numeric(rounds_df[rounds_end_tick_col], errors="coerce").dropna()
        if not max_tick.empty:
            match_meta["duration"] = float(max_tick.max()) / 128
    # Determine MR from player count (MR12 competitive, MR8 wingman)
    _quick_ids: set = set()
    if attacker_col and not deaths_df.empty:
        _quick_ids.update(deaths_df[attacker_col].dropna().astype(str).str.strip().tolist())
    if victim_col and not deaths_df.empty:
        _quick_ids.update(deaths_df[victim_col].dropna().astype(str).str.strip().tolist())
    _quick_ids.discard("")
    mr = 8 if len(_quick_ids) == 4 else 12

    score_ct = None
    score_t = None
    winner = None
    winner_col = find_col(rounds_df, ["winner", "winnerSide", "winnerTeam", "winner_team"])
    if winner_col and not rounds_df.empty:
        winner_series = rounds_df[winner_col].map(normalize_side)

        # Compute TEAM-based scores (not side-based)
        # "score_ct" = score of the team that STARTED on CT side
        # "score_t"  = score of the team that STARTED on T side
        team_ct_score = 0
        team_t_score = 0
        for rnd_idx, winner_side in enumerate(winner_series):
            rnd_num = rnd_idx + 1  # 1-indexed
            if winner_side not in ("CT", "T"):
                continue
            ct_team_plays_as = starting_ct_plays_as(rnd_num, mr)
            if winner_side == ct_team_plays_as:
                team_ct_score += 1
            else:
                team_t_score += 1

        score_ct = team_ct_score
        score_t = team_t_score
        if score_ct > score_t:
            winner = "CT"
        elif score_t > score_ct:
            winner = "T"
        else:
            winner = "Tie"
    match_meta["score_ct"] = score_ct
    match_meta["score_t"] = score_t
    match_meta["winner"] = winner

    rounds_history: List[Dict[str, Any]] = []
    if not rounds_df.empty:
        round_end_tick_col = find_col(
            rounds_df,
            ["tick", "tickNum", "tick_num", "tickNumber", "time", "timeMs"],
        )
        winner_col = find_col(
            rounds_df,
            ["winner", "winnerSide", "winnerTeam", "winner_team", "winningSide"],
        )
        reason_col = find_col(
            rounds_df,
            ["reason", "end_reason", "roundEndReason", "win_reason", "round_end_reason"],
        )
        total_rounds_col = find_col(
            rounds_df,
            ["total_rounds_played", "totalRoundsPlayed", "round", "round_num", "round_number", "roundNum", "roundNumber"],
        )
        rounds_local = rounds_df.copy()
        if round_end_tick_col:
            rounds_local[round_end_tick_col] = pd.to_numeric(
                rounds_local[round_end_tick_col], errors="coerce"
            )
            rounds_local = rounds_local.dropna(subset=[round_end_tick_col])
            rounds_local = rounds_local.sort_values(round_end_tick_col)
        rounds_local = rounds_local.reset_index(drop=True)

        ct_running = 0  # running score of team that STARTED CT
        t_running = 0   # running score of team that STARTED T
        for index, row in rounds_local.iterrows():
            round_number = parse_int(row.get(total_rounds_col)) if total_rounds_col else None
            if round_number is None:
                round_number = index + 1
            winner_side = normalize_side(row.get(winner_col)) if winner_col else None
            reason = normalize_reason(row.get(reason_col)) if reason_col else None

            # Use team-based scoring (accounting for halftime side swap)
            if winner_side in ("CT", "T"):
                ct_team_plays_as = starting_ct_plays_as(round_number, mr)
                if winner_side == ct_team_plays_as:
                    ct_running += 1
                else:
                    t_running += 1

            rounds_history.append(
                {
                    "round_number": round_number,
                    "winner_side": winner_side,
                    "reason": reason,
                    "ct_score": ct_running,
                    "t_score": t_running,
                }
            )

    opening_kills_by_id: Dict[str, int] = {}
    opening_deaths_by_id: Dict[str, int] = {}
    side_by_id: Dict[str, Dict[str, int]] = {}
    trade_kills_by_id: Dict[str, int] = {}
    if round_key and time_col and not deaths_with_round.empty and attacker_col and victim_col:
        first_kills_all = deaths_with_round.sort_values(time_col).groupby(round_key).first()
        for _, row in first_kills_all.iterrows():
            attacker_id = normalize_id(row.get(attacker_col))
            victim_id = normalize_id(row.get(victim_col))
            if attacker_id:
                opening_kills_by_id[attacker_id] = opening_kills_by_id.get(attacker_id, 0) + 1
            if victim_id:
                opening_deaths_by_id[victim_id] = opening_deaths_by_id.get(victim_id, 0) + 1

    first_half_side_by_id: Dict[str, str] = {}  # player_id → starting side (from first-half kills)
    for rnd_idx, round_data in enumerate(rounds):
        is_first_half = rnd_idx < mr
        kills_list = round_data.get("kills", []) if isinstance(round_data, dict) else []
        for kill in kills_list or []:
            if not isinstance(kill, dict):
                continue
            attacker_id = normalize_id(kill.get("attackerSteamID"))
            victim_id = normalize_id(kill.get("victimSteamID"))
            attacker_side = extract_side(kill, "attacker")
            victim_side = extract_side(kill, "victim")
            if attacker_id and attacker_side:
                side_by_id.setdefault(attacker_id, {"CT": 0, "T": 0})
                side_by_id[attacker_id][attacker_side] += 1
                if is_first_half and attacker_id not in first_half_side_by_id:
                    first_half_side_by_id[attacker_id] = attacker_side
            if victim_id and victim_side:
                side_by_id.setdefault(victim_id, {"CT": 0, "T": 0})
                side_by_id[victim_id][victim_side] += 1
                if is_first_half and victim_id not in first_half_side_by_id:
                    first_half_side_by_id[victim_id] = victim_side

    team_graph: Dict[str, List[str]] = {}
    if attacker_col and victim_col and not deaths_df.empty:
        for attacker_id, victim_id in zip(
            deaths_df[attacker_col].astype(str).str.strip(),
            deaths_df[victim_col].astype(str).str.strip(),
        ):
            if not attacker_id or not victim_id:
                continue
            team_graph.setdefault(attacker_id, []).append(victim_id)
            team_graph.setdefault(victim_id, []).append(attacker_id)

    team_map: Dict[str, int] = {}
    for node in team_graph:
        if node in team_map:
            continue
        team_map[node] = 0
        stack = [node]
        while stack:
            current = stack.pop()
            for neighbor in team_graph.get(current, []):
                if neighbor not in team_map:
                    team_map[neighbor] = 1 - team_map[current]
                    stack.append(neighbor)

    def resolve_team_side(player_id: str) -> Optional[str]:
        last_team = last_team_by_id.get(player_id)
        if last_team in {"CT", "T"}:
            return last_team
        if player_id in team_counts_by_id:
            ct_count = team_counts_by_id[player_id].get("CT", 0)
            t_count = team_counts_by_id[player_id].get("T", 0)
            if ct_count > t_count:
                return "CT"
            if t_count > ct_count:
                return "T"
        if player_id in side_by_id:
            ct_count = side_by_id[player_id].get("CT", 0)
            t_count = side_by_id[player_id].get("T", 0)
            if ct_count > t_count:
                return "CT"
            if t_count > ct_count:
                return "T"
        if player_id in team_map:
            return "CT" if team_map[player_id] == 0 else "T"
        return None

    def resolve_starting_side(player_id: str) -> Optional[str]:
        """Return the side the player STARTED the match on.

        Primary: use first-half kill events — in the first half the side
        a player plays on IS their starting side (reliable).
        Fallback: invert the last-recorded side when the match went
        past halftime.
        """
        # Best source: first-half kill events (always correct)
        if player_id in first_half_side_by_id:
            return first_half_side_by_id[player_id]
        # Fallback: invert last-recorded side if past halftime
        current = resolve_team_side(player_id)
        if not current:
            return None
        if round_count > mr:
            return "T" if current == "CT" else "CT"
        return current

    if trade_col and attacker_col and not deaths_df.empty:
        trade_flags = deaths_df[trade_col].astype(bool)
        for attacker_id, is_trade in zip(
            deaths_df[attacker_col].astype(str).str.strip(), trade_flags
        ):
            if attacker_id and is_trade:
                trade_kills_by_id[attacker_id] = trade_kills_by_id.get(attacker_id, 0) + 1
    elif time_col and attacker_col and victim_col and not deaths_df.empty:
        kills_sorted = deaths_df.copy()
        kills_sorted[time_col] = pd.to_numeric(kills_sorted[time_col], errors="coerce")
        kills_sorted = kills_sorted.dropna(subset=[time_col]).sort_values(time_col)
        window_ticks = 5 * 128
        recent: List[Tuple[str, str, float]] = []
        for _, row in kills_sorted.iterrows():
            tick_value = float(row[time_col])
            attacker_id = normalize_id(row.get(attacker_col))
            victim_id = normalize_id(row.get(victim_col))
            if not attacker_id or not victim_id:
                continue
            recent = [entry for entry in recent if tick_value - entry[2] <= window_ticks]
            attacker_team = resolve_team_side(attacker_id)
            if attacker_team:
                for killer_id, killed_id, _ in recent:
                    if killer_id == victim_id:
                        killed_team = resolve_team_side(killed_id)
                        if killed_team and killed_team == attacker_team:
                            trade_kills_by_id[attacker_id] = trade_kills_by_id.get(attacker_id, 0) + 1
                            break
            recent.append((attacker_id, victim_id, tick_value))

    player_ids = set()
    if attacker_col and not deaths_df.empty:
        player_ids.update(deaths_df[attacker_col].dropna().astype(str).str.strip().tolist())
    if victim_col and not deaths_df.empty:
        player_ids.update(deaths_df[victim_col].dropna().astype(str).str.strip().tolist())
    if dmg_attacker_col and not hurts_df.empty:
        player_ids.update(hurts_df[dmg_attacker_col].dropna().astype(str).str.strip().tolist())
    player_ids = {pid for pid in player_ids if pid}

    player_count = len(player_ids)
    team_size = player_count // 2 if player_count and player_count % 2 == 0 else None
    if player_count == 4:
        game_mode = "Wingman"
    elif player_count == 10:
        game_mode = "Competitive"
    elif player_count == 6:
        game_mode = "Short-handed"
    else:
        game_mode = "Unknown"

    assists_col = find_col(
        deaths_df,
        ["assister_steamid", "assisterSteamID", "assister_steam_id", "assister"],
    )

    damage_by_id: Dict[str, float] = {}
    utility_damage_by_id: Dict[str, float] = {}
    if dmg_attacker_col and dmg_value_col and not hurts_df.empty:
        hurt_attackers = hurts_df[dmg_attacker_col].astype(str).str.strip()
        dmg_values = hurts_df[dmg_value_col].fillna(0).astype(float)
        for attacker_id, dmg_value in zip(hurt_attackers, dmg_values):
            if attacker_id:
                damage_by_id[attacker_id] = damage_by_id.get(attacker_id, 0.0) + float(dmg_value)
        if hurt_weapon_col:
            hurt_weapons = hurts_df[hurt_weapon_col].astype(str)
            for attacker_id, dmg_value, weapon_value in zip(
                hurt_attackers, dmg_values, hurt_weapons
            ):
                if attacker_id and is_utility_weapon(weapon_value):
                    utility_damage_by_id[attacker_id] = utility_damage_by_id.get(attacker_id, 0.0) + float(dmg_value)

    player_stats: List[Dict[str, Any]] = []
    for player_id in sorted(player_ids):
        kills_total = 0
        deaths_total = 0
        assists_total = 0
        headshots_total = 0
        team_side = resolve_starting_side(player_id)
        if attacker_col and not deaths_df.empty:
            kills_total = int(
                (deaths_df[attacker_col].astype(str).str.strip() == player_id).sum()
            )
            if headshot_col:
                headshots_total = int(
                    (
                        (deaths_df[attacker_col].astype(str).str.strip() == player_id)
                        & deaths_df[headshot_col].astype(bool)
                    ).sum()
                )
        if victim_col and not deaths_df.empty:
            deaths_total = int(
                (deaths_df[victim_col].astype(str).str.strip() == player_id).sum()
            )
        if assists_col and not deaths_df.empty:
            assists_total = int(
                (deaths_df[assists_col].astype(str).str.strip() == player_id).sum()
            )

        hs_ratio = (headshots_total / kills_total * 100) if kills_total else 0.0
        damage_total_player = damage_by_id.get(player_id, 0.0)
        adr_player = damage_total_player / round_count if round_count else 0.0
        player_stats.append(
            {
                "steam_id": player_id,
                "player_name": players_by_id.get(player_id),
                "team_side": team_side,
                "player_team": team_side,
                "kills": kills_total,
                "deaths": deaths_total,
                "assists": assists_total,
                "adr": round(adr_player, 1),
                "hs_percent": round(hs_ratio, 1),
                "opening_kills": opening_kills_by_id.get(player_id, 0),
                "opening_deaths": opening_deaths_by_id.get(player_id, 0),
                "trade_kills": trade_kills_by_id.get(player_id, 0),
                "utility_damage": round(utility_damage_by_id.get(player_id, 0.0), 1),
            }
        )

    buy_summary: Dict[str, Any] = {}
    if not purchases_with_round.empty:
        item_col = find_col(
            purchases_with_round,
            ["weapon", "item", "item_name", "weapon_name", "weaponName"],
        )
        team_col = find_col(
            purchases_with_round,
            ["team", "team_name", "teamNum", "teamnum", "team_side", "side"],
        )
        if item_col and team_col and purchases_round_key:
            purchases_local = purchases_with_round.copy()
            purchases_local["team_norm"] = purchases_local[team_col].map(normalize_side)
            purchases_local = purchases_local.dropna(subset=["team_norm", purchases_round_key])
            purchases_local["item_cat"] = purchases_local[item_col].astype(str).map(categorize_item)
            buy_counts: Dict[str, Dict[str, int]] = {"CT": {}, "T": {}}
            for (round_value, team_value), group in purchases_local.groupby(
                [purchases_round_key, "team_norm"]
            ):
                if team_value not in {"CT", "T"}:
                    continue
                primary_count = int((group["item_cat"] == "primary").sum())
                buy_type = None
                if team_size:
                    if primary_count >= team_size:
                        buy_type = "full"
                    elif primary_count == 0:
                        buy_type = "eco"
                    else:
                        buy_type = "force"
                if buy_type:
                    buy_counts[team_value][buy_type] = buy_counts[team_value].get(buy_type, 0) + 1
            buy_summary = buy_counts

    # ── Determine user's STARTING side (first half) ──
    # resolve_team_side returns the last-recorded side which is the second-half
    # side.  We need the starting side so it aligns with score_ct / score_t.
    user_starting_side: Optional[str] = None
    if target_steam_id:
        # Strategy 1: Look at kill events in the first mr rounds
        for rnd_idx, round_data in enumerate(rounds):
            if rnd_idx >= mr:  # only first half
                break
            for kill in (round_data.get("kills", []) or []):
                attacker_id = normalize_id(kill.get("attackerSteamID"))
                victim_id = normalize_id(kill.get("victimSteamID"))
                if attacker_id == target_steam_id:
                    side = extract_side(kill, "attacker")
                    if side in ("CT", "T"):
                        user_starting_side = side
                        break
                if victim_id == target_steam_id:
                    side = extract_side(kill, "victim")
                    if side in ("CT", "T"):
                        user_starting_side = side
                        break
            if user_starting_side:
                break
        # Strategy 2: Use resolve_starting_side (inverts second-half side when needed)
        if not user_starting_side:
            user_starting_side = resolve_starting_side(target_steam_id)

    return {
        "kills": kills,
        "deaths": deaths,
        "adr": round(adr, 1),
        "hs_percent": round(hs_percent, 1),
        "kd_ratio": round(kd_ratio, 2),
        "common_death_round": common_death_round,
        "first_death_round": first_death_round,
        "common_death_weapon": common_death_weapon,
        "death_headshot_rate": round(death_hs_percent, 1),
        "avg_death_time_sec": avg_death_time_sec,
        "median_death_time_sec": median_death_time_sec,
        "avg_death_distance": avg_death_distance,
        "top_death_rounds": top_death_rounds,
        "opening_kills": opening_kills,
        "opening_deaths": opening_deaths,
        "opening_win_rate": round(opening_win_rate, 1),
        "ct_kd": round(ct_kd, 2) if ct_kd is not None else None,
        "t_kd": round(t_kd, 2) if t_kd is not None else None,
        "ct_kills": ct_kills,
        "t_kills": t_kills,
        "ct_deaths": ct_deaths,
        "t_deaths": t_deaths,
        "game_mode": game_mode,
        "team_size": team_size,
        "buy_summary": buy_summary,
        "fav_weapon": fav_weapon,
        "match_meta": match_meta,
        "players_stats": player_stats,
        "rounds": rounds_history,
        "user_team_side": user_starting_side,
        "score_ct": score_ct,
        "score_t": score_t,
        "winner": winner,
        "map_name": match_meta.get("map_name"),
        "multi_kill_rounds": multi_kill_rounds,
        "clutch_rounds": clutch_rounds,
        "user_round_kills": user_round_kills,
        "user_round_deaths": user_round_deaths,
    }


def get_ai_coaching_tip(
    stats: Dict[str, Any],
    language: Optional[str],
    style: Optional[str],
    match_id: Optional[int] = None,
) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY must be set")

    if _rate_limiter is not None:
        _rate_limiter.acquire()

    client = Groq(api_key=api_key)

    # ── Extract all available data from parsed stats ──
    opening_win_rate = stats.get("opening_win_rate")
    ct_kd = stats.get("ct_kd")
    t_kd = stats.get("t_kd")
    ct_kills = stats.get("ct_kills")
    t_kills = stats.get("t_kills")
    ct_deaths = stats.get("ct_deaths")
    t_deaths = stats.get("t_deaths")
    fav_weapon = stats.get("fav_weapon")
    top_death_rounds = stats.get("top_death_rounds") or []
    common_death_weapon = stats.get("common_death_weapon")
    death_headshot_rate = stats.get("death_headshot_rate")
    game_mode = stats.get("game_mode")
    team_size = stats.get("team_size")
    avg_death_time_sec = stats.get("avg_death_time_sec")
    median_death_time_sec = stats.get("median_death_time_sec")
    avg_death_distance = stats.get("avg_death_distance")
    buy_summary = stats.get("buy_summary") or {}
    user_team_side = stats.get("user_team_side")
    score_ct = stats.get("score_ct")
    score_t = stats.get("score_t")
    winner = stats.get("winner")
    map_name = stats.get("map_name")
    players_stats = stats.get("players_stats") or []
    multi_kill_rounds = stats.get("multi_kill_rounds") or []
    clutch_rounds = stats.get("clutch_rounds") or []
    user_round_kills = stats.get("user_round_kills") or {}
    user_round_deaths = stats.get("user_round_deaths") or []

    if team_size:
        opening_advantage = f"{team_size}v{team_size - 1}"
    else:
        opening_advantage = "man advantage"

    # Determine match result for the user
    match_result = "Unknown"
    if user_team_side and winner:
        if winner == "Tie":
            match_result = "Tie"
        elif winner == user_team_side:
            match_result = "Win"
        else:
            match_result = "Loss"

    user_score = None
    enemy_score = None
    if user_team_side and score_ct is not None and score_t is not None:
        if user_team_side == "CT":
            user_score = score_ct
            enemy_score = score_t
        else:
            user_score = score_t
            enemy_score = score_ct

    # ── Format map name for display ──
    display_map = map_name or "Unknown Map"
    if display_map.startswith("de_"):
        display_map = display_map[3:].capitalize()

    # ── Additional data from stats ──
    rounds_history = stats.get("rounds") or []
    common_death_round = stats.get("common_death_round")
    first_death_round = stats.get("first_death_round")
    total_rounds = len(rounds_history) if rounds_history else (
        (user_score + enemy_score) if user_score is not None and enemy_score is not None else 0
    )

    # ── Build comprehensive stats block ──
    stats_lines: List[str] = []

    # Header line with map + result + score
    header_parts = [f"Map: {display_map}"]
    if match_result != "Unknown":
        header_parts.append(f"Result: {match_result}")
    if user_score is not None and enemy_score is not None:
        header_parts.append(f"Score: {user_score}-{enemy_score}")
    stats_lines.append(" | ".join(header_parts))

    if game_mode:
        mode_line = f"Game mode: {game_mode}"
        if team_size:
            mode_line += f" ({team_size}v{team_size})"
        stats_lines.append(mode_line)
    if user_team_side:
        stats_lines.append(f"Player's starting side: {user_team_side}")

    stats_lines.append("")
    stats_lines.append("-- Core Stats --")
    stats_lines.extend([
        f"K/D: {stats['kills']}/{stats['deaths']} (ratio: {stats['kd_ratio']})",
        f"ADR: {stats['adr']}",
        f"HS%: {stats['hs_percent']}%",
        f"Most used weapon: {fav_weapon if fav_weapon else 'unknown'}",
    ])

    stats_lines.append("")
    stats_lines.append("-- Opening Duels --")
    stats_lines.extend([
        f"Opening Kills: {stats.get('opening_kills', 0)}",
        f"Opening Deaths: {stats.get('opening_deaths', 0)}",
        f"Opening Duel Win Rate: {opening_win_rate}% (creates a {opening_advantage})",
    ])

    if ct_kd is not None or t_kd is not None:
        stats_lines.append("")
        stats_lines.append("-- Side Performance --")
        if ct_kills is not None and ct_deaths is not None:
            stats_lines.append(f"CT side: {ct_kills}K/{ct_deaths}D (K/D: {ct_kd})")
        if t_kills is not None and t_deaths is not None:
            stats_lines.append(f"T side: {t_kills}K/{t_deaths}D (K/D: {t_kd})")

    # ── Round-by-round breakdown ──
    if user_round_kills or user_round_deaths:
        stats_lines.append("")
        stats_lines.append("-- Per-Round Kill/Death Log --")
        all_round_nums = sorted(
            set(user_round_kills.keys()) | set(user_round_deaths)
        )
        for rn in all_round_nums:
            k = user_round_kills.get(rn, 0)
            d = "died" if rn in user_round_deaths else "survived"
            stats_lines.append(f"Round {rn}: {k} kill{'s' if k != 1 else ''}, {d}")

    # ── Round highlights (multi-kills, clutches) ──
    if multi_kill_rounds or clutch_rounds:
        stats_lines.append("")
        stats_lines.append("-- Notable Rounds --")
        for mk in multi_kill_rounds:
            stats_lines.append(
                f"Round {mk['round']}: {mk['label']} ({mk['kills']} kills)"
            )
        for cl in clutch_rounds:
            stats_lines.append(
                f"Round {cl['round']}: Clutch {cl['situation']} "
                f"({cl['kills']} kill{'s' if cl['kills'] > 1 else ''}, "
                f"{'survived' if cl.get('survived') else 'died'})"
            )

    # ── Death analysis ──
    stats_lines.append("")
    stats_lines.append("-- Death Analysis --")
    if user_round_deaths:
        death_rounds_str = ", ".join(str(r) for r in sorted(user_round_deaths))
        stats_lines.append(f"Died in rounds: {death_rounds_str}")
        rounds_survived = [
            rn for rn in range(1, (total_rounds or 1) + 1)
            if rn not in user_round_deaths
        ]
        if rounds_survived:
            stats_lines.append(f"Survived rounds: {', '.join(str(r) for r in rounds_survived)}")
    if first_death_round is not None:
        stats_lines.append(f"First death occurred in round: {first_death_round}")
    if common_death_round is not None:
        stats_lines.append(f"Most common death round: {common_death_round}")
    if top_death_rounds:
        rounds_list = ", ".join(str(v) for v in top_death_rounds)
        stats_lines.append(f"Rounds with most deaths: {rounds_list}")
    if common_death_weapon:
        stats_lines.append(f"Most killed by weapon: {common_death_weapon}")
    if death_headshot_rate is not None:
        stats_lines.append(f"Death HS rate (killed by HS): {death_headshot_rate}%")
    if avg_death_time_sec is not None:
        stats_lines.append(f"Avg death time into round: {avg_death_time_sec}s")
    if median_death_time_sec is not None:
        stats_lines.append(f"Median death time into round: {median_death_time_sec}s")
    if avg_death_distance is not None:
        stats_lines.append(f"Avg death distance from attacker: {avg_death_distance} units")

    # ── Round flow: score progression + win reasons ──
    if rounds_history:
        stats_lines.append("")
        stats_lines.append("-- Round-by-Round Score/Flow --")
        reason_map = {
            "1": "Target Bombed",
            "7": "Bomb Defused",
            "8": "CT Elimination",
            "9": "T Elimination",
            "12": "Time Ran Out",
            "17": "T Surrender",
            "18": "CT Surrender",
        }
        for rh in rounds_history:
            rn = rh.get("round_number", "?")
            ws = rh.get("winner_side", "?")
            raw_reason = rh.get("reason")
            reason = reason_map.get(str(raw_reason), raw_reason) if raw_reason else ""
            ct_s = rh.get("ct_score", "?")
            t_s = rh.get("t_score", "?")
            rline = f"Round {rn}: {ws} wins"
            if reason:
                rline += f" - {reason}"
            rline += f" (CT-team {ct_s}, T-team {t_s})"
            stats_lines.append(rline)

    # ── Teammate/enemy scoreboard context ──
    teammates = []
    enemies = []
    for p in players_stats:
        line = (
            f"{p.get('player_name') or p.get('steam_id')}: "
            f"{p.get('kills',0)}K/{p.get('deaths',0)}D/{p.get('assists',0)}A "
            f"ADR:{p.get('adr',0)} HS:{p.get('hs_percent',0)}% "
            f"Opening:{p.get('opening_kills',0)}OK/{p.get('opening_deaths',0)}OD "
            f"Trades:{p.get('trade_kills',0)} UtilDmg:{p.get('utility_damage',0)}"
        )
        if user_team_side and p.get("team_side") == user_team_side:
            teammates.append(line)
        else:
            enemies.append(line)

    if teammates:
        stats_lines.append("")
        stats_lines.append("-- Teammates (full scoreboard) --")
        stats_lines.extend(f"  {t}" for t in teammates)
    if enemies:
        stats_lines.append("")
        stats_lines.append("-- Opponents (full scoreboard) --")
        stats_lines.extend(f"  {e}" for e in enemies)

    # ── Economy data ──
    facts_lines: List[str] = []
    if buy_summary:
        ct_buy = buy_summary.get("CT", {})
        t_buy = buy_summary.get("T", {})
        if ct_buy:
            facts_lines.append(
                "CT economy rounds: " + ", ".join(f"{k}={v}" for k, v in ct_buy.items())
            )
        if t_buy:
            facts_lines.append(
                "T economy rounds: " + ", ".join(f"{k}={v}" for k, v in t_buy.items())
            )

    style_value = (style or "narrative").strip().lower()
    language_value = (language or "english").strip().lower()
    if language_value in {"ar", "arabic", "العربية"}:
        language_value = "arabic"
    else:
        language_value = "english"

    if style_value == "stats_only":
        style_prompt = (
            "Return ONLY a bulleted list of ALL available stats and facts. "
            "No advice text, no narrative."
        )
    elif style_value == "short":
        style_prompt = "Give advice in 1-2 sentences maximum. Be direct."
    else:
        style_prompt = (
            "Write a DEEP, DETAILED coaching analysis as if you are the player's personal coach reviewing their VOD.\n"
            "This is NOT a summary — it is in-depth coaching. Use EVERY piece of data provided. Be thorough.\n\n"
            "Structure the response like this:\n\n"
            "1. MATCH OVERVIEW — One-line header with map, result, score. Then a 2-3 sentence narrative of how the match played out "
            "(reference the round-by-round score flow: when they took leads, lost momentum, had comeback runs, or collapsed).\n\n"
            "2. ROUND-BY-ROUND STORY — Walk through the key moments of the match chronologically. "
            "Group consecutive rounds into phases (e.g., 'Rounds 1-4: strong CT start' or 'Rounds 8-12: the collapse'). "
            "For each phase, mention:\n"
            "  - What the player did (kills, deaths, survived or died)\n"
            "  - Opening duels won or lost in those rounds\n"
            "  - Multi-kills or clutches that happened\n"
            "  - How the score shifted\n"
            "  - Round end reasons (bomb plant, elimination, defuse, time) and what that says about playstyle\n\n"
            "3. TIMING AND POSITIONING ANALYSIS — Use the death timing data:\n"
            "  - If avg death time is low (under 30s), the player is over-peeking or dying to early aggression\n"
            "  - If avg death time is high (over 60s), the player is passive but dying in late-round scrambles\n"
            "  - Analyze death distance: close range = bad positioning, long range = getting picked\n"
            "  - What weapon they keep dying to and what that reveals about their positioning\n\n"
            "4. SIDE BREAKDOWN — Compare CT vs T performance in detail.\n"
            "  - Which side were they stronger on? Why?\n"
            "  - Did they contribute differently on attack vs defense?\n\n"
            "5. COMPARED TO OTHERS — Compare the player's stats to their teammates and opponents.\n"
            "  - ADR ranking within team\n"
            "  - Who had better opening duels, trade kills, utility damage?\n"
            "  - Was the player carried or carrying?\n"
            "  - Identify the enemy player who caused the most damage and what to learn from it\n\n"
            "6. ECONOMY OBSERVATIONS — If economy data is available, comment on buy patterns "
            "(too many ecos? too many force buys? proper full buys?).\n\n"
            "7. WHAT WENT WELL — Specific praise with round numbers. Clutches, multi-kills, high-impact rounds.\n\n"
            "8. WHAT WENT WRONG — Specific criticism with round numbers. Death streaks, "
            "opening duel losses, low-impact rounds where the player had 0 kills and died.\n\n"
            "9. COACHING PLAN — 4-6 specific, actionable improvements:\n"
            "  - Reference the exact situations from THIS match\n"
            "  - Give concrete numbers (e.g., 'stay within 5-8 units of a teammate for trade potential')\n"
            "  - Suggest specific positions, angles, or timing adjustments for the MAP played\n"
            "  - Address the weapon they keep dying to with counter-play advice\n"
            "  - If utility damage is low, suggest specific utility lineups for the map\n\n"
            "Be honest and direct. Don't sugarcoat. Use the tone of a real coach in a post-match review.\n"
            "Reference specific round numbers throughout — do NOT speak in generalities.\n"
            "The message can be as LONG as needed to cover everything. Do not cut corners."
        )

    language_prompt = ""
    if language_value == "arabic":
        language_prompt = (
            "Output the ENTIRE response in Arabic. Very important rules for Arabic output:\n"
            "- Use Modern Standard Arabic or generic dialect.\n"
            "- Do NOT include any English words or Latin letters.\n"
            "- Use western digits 0-9 for all numbers.\n"
            "- Do NOT use parentheses ( ), slashes / \\ , or colons : inside sentences.\n"
            "- Instead of parentheses, use dashes - or commas , to separate info.\n"
            "- Use line breaks between sections for readability.\n"
            "- Use simple bullet points with - instead of * or other markers.\n"
            "- Place numbers after Arabic words: 'نسبة الفوز 71.4%'.\n"
            "- Start the message with a right-to-left mark.\n"
            "- Translate map names: de_dust2 = دست 2, de_nuke = نوك, de_ancient = انشنت, "
            "de_mirage = ميراج, de_inferno = انفيرنو, de_anubis = انوبيس, de_vertigo = فيرتيجو.\n"
            "- Keep section headers short and clear.\n"
            "- Use emojis to mark each section header."
        )
    else:
        language_prompt = "Output the entire response in English."

    prompt = (
        "You are an elite CS2 coach performing a detailed post-match review for your student. "
        "You have FULL access to the demo file stats below — every number is real, nothing is estimated. "
        "Your job is to analyze this match deeply, cover every mistake, praise every good play, "
        "and give coaching that references SPECIFIC ROUNDS and SPECIFIC NUMBERS.\n\n"
        "CRITICAL RULES:\n"
        "- This is a Steam chat message, NOT a web page.\n"
        "- Do NOT use markdown: no ### headers, no **bold**, no *italics*.\n"
        "- Use simple text: emojis for section markers, dashes - for bullet points, line breaks for structure.\n"
        "- Use ALL the data provided — round flow, death timing, weapons, teammate comparison, economy.\n"
        "- Be thorough. The player wants to understand EXACTLY what happened and what to fix.\n"
        "- Do NOT fabricate data. Only reference what is provided below.\n"
        "- The message can be long — there is no character limit. Cover everything.\n\n"
        + style_prompt
        + "\n\n"
        + language_prompt
        + "\n\nMatch Stats (from demo):\n"
        + "\n".join(stats_lines)
        + ("\n\nEconomy Data:\n- " + "\n- ".join(facts_lines) if facts_lines else "")
    )

    try:
        completion = client.chat.completions.create(
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are an elite professional CS2 Coach with years of experience coaching "
                        "at the highest level. You analyze demos in extreme detail and leave no stone unturned. "
                        "Your reviews are honest, data-driven, and actionable. You reference specific round numbers "
                        "and exact statistics. You never give vague advice."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            model=MODEL_NAME,
        )
    except Exception as exc:
        raise RuntimeError(f"Groq request failed: {exc}") from exc

    response_text = completion.choices[0].message.content.strip()

    # Remove any markdown formatting the model might have included
    response_text = response_text.replace("**", "").replace("###", "").replace("##", "").replace("# ", "")

    # ── Append match detail link ──
    match_link = ""
    if match_id is not None:
        base_url = os.getenv("NEXT_PUBLIC_BASE_URL", "https://retake-cs2.vercel.app")
        match_url = f"{base_url}/dashboard/matches/{match_id}"
        if language_value == "arabic":
            match_link = (
                f"\n\n\u200F\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                f"\U0001f4ca \u0634\u0648\u0641 \u0625\u062d\u0635\u0627\u0626\u064a\u0627\u062a\u0643 \u0627\u0644\u0643\u0627\u0645\u0644\u0629 \u0647\u0646\u0627\n"
                f"{match_url}"
            )
        else:
            match_link = (
                f"\n\n\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\n"
                f"\U0001f4ca View your full match stats here\n"
                f"{match_url}"
            )

    if language_value == "arabic":
        return f"\u200F{response_text}{match_link}"
    return f"{response_text}{match_link}"


def parse_match_logic(
    match: Dict[str, Any], pool: QueuePool
) -> Tuple[int, bool, Optional[str]]:
    match_id_value = int(match["id"])
    steam_id = str(match["user_id"])
    username = match.get("username")
    language = match.get("language")
    coach_style = match.get("coach_style")
    file_path = match.get("file_path")
    if os.getenv("DEBUG_DEMO") == "1":
        print(
            f"DEBUG: user settings language={language}, coach_style={coach_style}"
        )

    conn = pool.connect()
    try:
        db_conn = conn.driver_connection
        with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
            if not file_path or not os.path.exists(file_path):
                mark_error(cursor, match_id_value, "missing demo file")
                db_conn.commit()
                return match_id_value, False, "missing demo file"

            try:
                stats = parse_stats(file_path, steam_id, match_id_value, username)
                match_meta = stats.get("match_meta", {})
                players_stats = stats.get("players_stats", [])
                rounds_history = stats.get("rounds", [])
                match_row_id = insert_match_row(cursor, match_meta, match_id_value)
                insert_player_stats(cursor, match_row_id, players_stats)
                insert_rounds(cursor, match_row_id, rounds_history)

                tip = get_ai_coaching_tip(stats, language, coach_style, match_id=match_id_value)
                if not tip:
                    raise RuntimeError("Coach tip was empty")

                # Generate stats card image (non-blocking — failure won't stop the tip)
                tip_image_url = None
                if generate_stats_image is not None:
                    try:
                        tip_image_url = generate_stats_image(stats, match_id=match_id_value)
                    except Exception as img_exc:
                        log(f"Stats card generation failed for match {match_id_value}: {img_exc}")

                mark_parsed(cursor, match_id_value, tip, tip_image_url=tip_image_url)
                db_conn.commit()
                return match_id_value, True, None
            except Exception as exc:
                if is_quota_error(exc):
                    db_conn.rollback()
                    return match_id_value, False, f"QUOTA_EXCEEDED::{exc}"
                db_conn.rollback()
                mark_error(cursor, match_id_value, str(exc))
                db_conn.commit()
                return match_id_value, False, str(exc)
    finally:
        conn.close()


def main() -> None:
    db_url = get_db_url()
    if not db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL (or DATABASE_URL/SUPABASE_DATABASE_URL) must be set"
        )

    match_id = None
    if len(os.sys.argv) > 1:
        try:
            match_id = int(os.sys.argv[1])
        except ValueError:
            raise RuntimeError("Match id must be an integer")

    pool = QueuePool(
        lambda: psycopg2.connect(db_url),
        pool_size=PARSE_DB_POOL,
        max_overflow=PARSE_DB_OVERFLOW,
    )
    set_rate_limiter(RateLimiter(max_calls=PARSE_RATE_LIMIT, period_seconds=60))

    # Single match mode (triggered by replay_downloader)
    if match_id is not None:
        log(f"Parsing single match {match_id}...")
        conn = pool.connect()
        try:
            db_conn = conn.driver_connection
            with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                matches = fetch_matches(cursor, match_id, limit=1)
        finally:
            conn.close()
        if not matches:
            log("No matches to parse.")
            return
        with ThreadPoolExecutor(max_workers=PARSE_WORKERS) as executor:
            futures = [executor.submit(parse_match_logic, match, pool) for match in matches]
            for future in as_completed(futures):
                match_id_value, success, reason = future.result()
                if success:
                    log(f"Saved coach tip for match {match_id_value}.")
                else:
                    log(f"Match {match_id_value} failed: {reason}")
        return

    # Daemon mode — run forever
    log("=" * 50)
    log("Demo Parser daemon started")
    log(f"  Model          : {MODEL_NAME}")
    log(f"  Workers        : {PARSE_WORKERS}")
    log(f"  Batch size     : {PARSE_BATCH_SIZE}")
    log(f"  Rate limit     : {PARSE_RATE_LIMIT} calls/60s")
    log(f"  DB pool        : {PARSE_DB_POOL} + {PARSE_DB_OVERFLOW} overflow")
    log(f"  Poll interval  : {PARSE_POLL_INTERVAL}s")
    log(f"  Cleanup interval: {CLEANUP_INTERVAL}s")
    log(f"  Waiting for downloaded demos...")
    log("=" * 50)

    last_cleanup = 0.0
    while True:
        try:
            conn = pool.connect()
            try:
                db_conn = conn.driver_connection
                with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    matches = fetch_matches(cursor, None, limit=PARSE_BATCH_SIZE)
            finally:
                conn.close()

            if not matches:
                time.sleep(PARSE_POLL_INTERVAL)
                continue

            log(f"Found {len(matches)} match(es) to parse.")

            with ThreadPoolExecutor(max_workers=PARSE_WORKERS) as executor:
                futures = [executor.submit(parse_match_logic, match, pool) for match in matches]
                quota_hit = False
                retry_after = None
                for future in as_completed(futures):
                    match_id_value, success, reason = future.result()
                    if success:
                        log(f"Saved coach tip for match {match_id_value}.")
                    else:
                        log(f"Match {match_id_value} failed: {reason}")
                        if reason and str(reason).startswith("QUOTA_EXCEEDED::"):
                            quota_hit = True
                            retry_after = parse_retry_after_seconds(Exception(str(reason)))

                if quota_hit:
                    sleep_seconds = retry_after or 45
                    log(f"Quota exceeded. Sleeping {sleep_seconds}s...")
                    time.sleep(sleep_seconds)
                    continue

        except Exception as exc:
            log(f"Parse cycle error: {exc}")

        # Periodically clean up demo files that are fully processed
        now = time.time()
        if now - last_cleanup >= CLEANUP_INTERVAL:
            try:
                cleaned = cleanup_demo_files(pool)
                if cleaned:
                    log(f"Cleaned up {cleaned} demo file(s).")
            except Exception as exc:
                log(f"Cleanup error: {exc}")
            last_cleanup = now

        time.sleep(PARSE_POLL_INTERVAL)


if __name__ == "__main__":
    main()
