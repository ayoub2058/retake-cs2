import os
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from groq import Groq
from awpy.demo import DemoParser
import pandas as pd
from sqlalchemy.pool import QueuePool

load_dotenv(".env.local")
load_dotenv()

MODEL_NAME = "openai/gpt-oss-120b"

_rate_limiter = None


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


def mark_parsed(cursor: RealDictCursor, match_id: int, tip: str) -> None:
    cursor.execute(
        """
        update public.matches_to_download
        set coach_tip = %s,
            tip_sent = false,
            status = 'processed'
        where id = %s
        """,
        (tip, match_id),
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

    for round_data in rounds:
        kills_list = round_data.get("kills", []) or []
        if kills_list:
            opening_kill = min(kills_list, key=extract_time_value)
            opening_attacker = normalize_id(opening_kill.get("attackerSteamID"))
            opening_victim = normalize_id(opening_kill.get("victimSteamID"))
            if opening_attacker and opening_attacker == target_steam_id:
                opening_kills += 1
            if opening_victim and opening_victim == target_steam_id:
                opening_deaths += 1

        for kill in kills_list:
            attacker = normalize_id(kill.get("attackerSteamID"))
            victim = normalize_id(kill.get("victimSteamID"))
            weapon = kill.get("weapon") or kill.get("weaponName") or kill.get("weapon_name")
            if attacker and attacker == target_steam_id and weapon:
                weapon_key = str(weapon)
                weapon_counts[weapon_key] = weapon_counts.get(weapon_key, 0) + 1
            if attacker and attacker == target_steam_id:
                attacker_side = extract_side(kill, "attacker")
                if attacker_side == "CT":
                    ct_kills += 1
                elif attacker_side == "T":
                    t_kills += 1
            if victim and victim == target_steam_id:
                victim_side = extract_side(kill, "victim")
                if victim_side == "CT":
                    ct_deaths += 1
                elif victim_side == "T":
                    t_deaths += 1

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
    score_ct = None
    score_t = None
    winner = None
    winner_col = find_col(rounds_df, ["winner", "winnerSide", "winnerTeam", "winner_team"])
    if winner_col and not rounds_df.empty:
        winner_series = rounds_df[winner_col].map(normalize_side)
        score_ct = int((winner_series == "CT").sum())
        score_t = int((winner_series == "T").sum())
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

        ct_running = 0
        t_running = 0
        for index, row in rounds_local.iterrows():
            round_number = parse_int(row.get(total_rounds_col)) if total_rounds_col else None
            if round_number is None:
                round_number = index + 1
            winner_side = normalize_side(row.get(winner_col)) if winner_col else None
            reason = normalize_reason(row.get(reason_col)) if reason_col else None

            if winner_side == "CT":
                ct_running += 1
            elif winner_side == "T":
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

    for round_data in rounds:
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
            if victim_id and victim_side:
                side_by_id.setdefault(victim_id, {"CT": 0, "T": 0})
                side_by_id[victim_id][victim_side] += 1

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
        team_side = resolve_team_side(player_id)
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

    player_ids = set()
    if attacker_col and not deaths_df.empty:
        player_ids.update(
            deaths_df[attacker_col].dropna().astype(str).str.strip().tolist()
        )
    if victim_col and not deaths_df.empty:
        player_ids.update(
            deaths_df[victim_col].dropna().astype(str).str.strip().tolist()
        )
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
    }


def get_ai_coaching_tip(
    stats: Dict[str, Any], language: Optional[str], style: Optional[str]
) -> str:
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        raise RuntimeError("GROQ_API_KEY must be set")

    if _rate_limiter is not None:
        _rate_limiter.acquire()

    client = Groq(api_key=api_key)

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
    if team_size:
        opening_advantage = f"{team_size}v{team_size - 1}"
    else:
        opening_advantage = "man advantage"

    stats_lines = [
        f"Overall K/D: {stats['kd_ratio']}",
        f"ADR: {stats['adr']}",
        f"Opening Duel Win Rate: {opening_win_rate}% (opening kill creates a {opening_advantage})",
        f"Kills: {stats['kills']}",
        f"Deaths: {stats['deaths']}",
        f"CT K/D: {ct_kd if ct_kd is not None else 'N/A'}",
        f"T K/D: {t_kd if t_kd is not None else 'N/A'}",
    ]
    if ct_kills is not None and t_kills is not None:
        stats_lines.extend([f"CT Kills: {ct_kills}", f"T Kills: {t_kills}"])
    if ct_deaths is not None and t_deaths is not None:
        stats_lines.extend([f"CT Deaths: {ct_deaths}", f"T Deaths: {t_deaths}"])
    if game_mode:
        mode_line = f"Game mode: {game_mode}"
        if team_size:
            mode_line += f" ({team_size}v{team_size})"
        stats_lines.append(mode_line)
    if top_death_rounds:
        rounds_list = ", ".join(str(value) for value in top_death_rounds)
        stats_lines.append(f"Common death rounds (index): {rounds_list}")
    if common_death_weapon:
        stats_lines.append(f"Common death weapon: {common_death_weapon}")
    if death_headshot_rate is not None:
        stats_lines.append(f"Death headshot rate: {death_headshot_rate}%")
    stats_lines.append(f"Most used weapon: {fav_weapon if fav_weapon else 'unknown'}")

    facts_lines = []
    if top_death_rounds:
        facts_lines.append(
            f"Death rounds (index): {', '.join(str(value) for value in top_death_rounds)}"
        )
    if common_death_weapon:
        facts_lines.append(f"Common death weapon: {common_death_weapon}")
    if death_headshot_rate is not None:
        facts_lines.append(f"Death headshot rate: {death_headshot_rate}%")
    if avg_death_time_sec is not None:
        facts_lines.append(f"Avg death time: {avg_death_time_sec}s")
    if median_death_time_sec is not None:
        facts_lines.append(f"Median death time: {median_death_time_sec}s")
    if avg_death_distance is not None:
        facts_lines.append(f"Avg death distance: {avg_death_distance}m")
    if buy_summary:
        ct_buy = buy_summary.get("CT", {})
        t_buy = buy_summary.get("T", {})
        if ct_buy:
            facts_lines.append(
                "CT buy mix: " + ", ".join(f"{k}={v}" for k, v in ct_buy.items())
            )
        if t_buy:
            facts_lines.append(
                "T buy mix: " + ", ".join(f"{k}={v}" for k, v in t_buy.items())
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
            "Use a detailed Match Report format with three sections in this exact order: "
            "THE GOOD, THE BAD, THE FIX. "
            "Each section must include a clear coaching takeaway and a concrete action. "
            "Add a few tasteful emojis to emphasize key points. "
            "Include 2-4 short factual bullets before the narrative."
        )

    language_prompt = ""
    if language_value == "arabic":
        language_prompt = (
            "Output the entire response in Arabic (Modern Standard or generic dialect). "
            "Do not include any English words or Latin letters. "
            "Use western digits (0-9) for all numbers. "
            "Place numbers after the Arabic words (e.g., 'نسبة الفوز 71.4%'). "
            "Avoid parentheses and slashes to reduce RTL alignment issues."
        )
    else:
        language_prompt = "Output the entire response in English."

    prompt = (
        "You are a CS2 coach writing a Steam chat message. "
        "Do not guess about economy or round type; if data is unavailable, say it is unavailable. "
        "No markdown headers like ###. Use bold sparingly for key numbers/phrases. "
        + style_prompt
        + " "
        + language_prompt
        + "\n\nUse these stats:\n"
        + "\n".join(stats_lines)
        + ("\n\nFacts:\n- " + "\n- ".join(facts_lines) if facts_lines else "")
    )

    try:
        completion = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a professional CS2 Coach."},
                {"role": "user", "content": prompt},
            ],
            model=MODEL_NAME,
        )
    except Exception as exc:
        raise RuntimeError(f"Groq request failed: {exc}") from exc

    response_text = completion.choices[0].message.content.strip()
    if language_value == "arabic":
        return f"\u200F{response_text}\u200F"
    return response_text


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

                tip = get_ai_coaching_tip(stats, language, coach_style)
                if not tip:
                    raise RuntimeError("Coach tip was empty")
                mark_parsed(cursor, match_id_value, tip)
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
        pool_size=10,
        max_overflow=20,
    )
    set_rate_limiter(RateLimiter(max_calls=15, period_seconds=60))

    if match_id is not None:
        conn = pool.connect()
        try:
            db_conn = conn.driver_connection
            with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                matches = fetch_matches(cursor, match_id, limit=1)
        finally:
            conn.close()
        if not matches:
            print("No matches to parse.")
            return
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(parse_match_logic, match, pool) for match in matches]
            for future in as_completed(futures):
                match_id_value, success, reason = future.result()
                if success:
                    print(f"Saved coach tip for match {match_id_value}.")
                else:
                    print(f"Match {match_id_value} failed: {reason}")
        return

    while True:
        conn = pool.connect()
        try:
            db_conn = conn.driver_connection
            with db_conn.cursor(cursor_factory=RealDictCursor) as cursor:
                matches = fetch_matches(cursor, None, limit=10)
        finally:
            conn.close()

        if not matches:
            time.sleep(3)
            continue

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(parse_match_logic, match, pool) for match in matches]
            quota_hit = False
            retry_after = None
            for future in as_completed(futures):
                match_id_value, success, reason = future.result()
                if success:
                    print(f"Saved coach tip for match {match_id_value}.")
                else:
                    print(f"Match {match_id_value} failed: {reason}")
                    if reason and str(reason).startswith("QUOTA_EXCEEDED::"):
                        quota_hit = True
                        retry_after = parse_retry_after_seconds(Exception(str(reason)))

            if quota_hit:
                sleep_seconds = retry_after or 45
                print(f"Quota exceeded. Sleeping for {sleep_seconds}s before retrying.")
                time.sleep(sleep_seconds)
                continue

        time.sleep(2)


if __name__ == "__main__":
    main()
