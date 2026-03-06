"""
Generate a beautiful HTML stats card, screenshot it, and upload to Supabase Storage.
Returns a public URL for the image.
"""

import os
import json
import tempfile
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv(".env.local")
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL") or os.getenv("NEXT_PUBLIC_SUPABASE_URL") or ""
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""
STORAGE_BUCKET = "stats-cards"


def _ensure_bucket() -> None:
    """Create the storage bucket if it doesn't exist (idempotent)."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return
    url = f"{SUPABASE_URL}/storage/v1/bucket"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
    }
    # List existing buckets
    resp = requests.get(url, headers=headers, timeout=10)
    if resp.ok:
        buckets = [b.get("name") for b in resp.json()]
        if STORAGE_BUCKET in buckets:
            return
    # Create bucket (public)
    requests.post(
        url,
        headers=headers,
        json={"id": STORAGE_BUCKET, "name": STORAGE_BUCKET, "public": True},
        timeout=10,
    )


def _upload_to_supabase(image_bytes: bytes, filename: str) -> Optional[str]:
    """Upload PNG bytes to Supabase Storage and return the public URL."""
    if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
        return None

    _ensure_bucket()

    upload_url = f"{SUPABASE_URL}/storage/v1/object/{STORAGE_BUCKET}/{filename}"
    headers = {
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "image/png",
        "x-upsert": "true",
    }
    resp = requests.post(upload_url, headers=headers, data=image_bytes, timeout=30)
    if not resp.ok:
        print(f"Supabase upload failed: {resp.status_code} {resp.text}")
        # Try PUT if POST fails (upsert)
        resp = requests.put(upload_url, headers=headers, data=image_bytes, timeout=30)
        if not resp.ok:
            print(f"Supabase upload PUT also failed: {resp.status_code} {resp.text}")
            return None

    return f"{SUPABASE_URL}/storage/v1/object/public/{STORAGE_BUCKET}/{filename}"


def _build_round_dots(
    rounds_history: List[Dict],
    user_team_side: Optional[str],
    user_round_kills: Dict[int, int],
    user_round_deaths: List[int],
) -> str:
    """Build HTML for the round timeline dots."""
    if not rounds_history:
        return '<div class="no-data">No round data</div>'

    dots = []
    for rh in rounds_history:
        rn = rh.get("round_number", 0)
        ws = rh.get("winner_side")
        # Determine if user's team won this round
        if user_team_side and ws:
            user_won = ws == user_team_side
        else:
            user_won = None

        kills = user_round_kills.get(rn, 0)
        died = rn in user_round_deaths

        if user_won is True:
            dot_class = "round-win"
        elif user_won is False:
            dot_class = "round-loss"
        else:
            dot_class = "round-unknown"

        # Tooltip
        tip = f"R{rn}"
        if kills:
            tip += f" | {kills}K"
        if died:
            tip += " | Died"

        kill_indicator = ""
        if kills >= 3:
            kill_indicator = f'<span class="kill-badge">{kills}K</span>'

        death_indicator = ""
        if died:
            death_indicator = '<span class="death-mark">×</span>'

        dots.append(
            f'<div class="round-dot {dot_class}" title="{tip}">'
            f'<span class="round-num">{rn}</span>'
            f'{kill_indicator}{death_indicator}'
            f'</div>'
        )

    # Add halftime marker
    half = len(rounds_history) // 2 if len(rounds_history) > 1 else 0
    if half > 0:
        dots.insert(half, '<div class="halftime-marker">HT</div>')

    return "\n".join(dots)


def _build_scoreboard_rows(
    players_stats: List[Dict],
    user_team_side: Optional[str],
    target_steam_id: str,
) -> str:
    """Build HTML table rows for the scoreboard."""
    if not players_stats:
        return ""

    teammates = [p for p in players_stats if p.get("team_side") == user_team_side]
    enemies = [p for p in players_stats if p.get("team_side") != user_team_side]

    # Sort by kills descending
    teammates.sort(key=lambda p: p.get("kills", 0), reverse=True)
    enemies.sort(key=lambda p: p.get("kills", 0), reverse=True)

    rows = []

    if teammates:
        rows.append('<tr class="team-header"><td colspan="8">YOUR TEAM</td></tr>')
        for p in teammates:
            is_user = str(p.get("steam_id", "")) == str(target_steam_id)
            row_class = "player-row user-row" if is_user else "player-row"
            name = p.get("player_name") or "Unknown"
            if len(name) > 16:
                name = name[:15] + "…"
            user_tag = ' <span class="you-tag">YOU</span>' if is_user else ""
            kd = p.get("kills", 0) / max(p.get("deaths", 1), 1)
            rows.append(
                f'<tr class="{row_class}">'
                f'<td class="name-cell">{name}{user_tag}</td>'
                f'<td>{p.get("kills", 0)}</td>'
                f'<td>{p.get("deaths", 0)}</td>'
                f'<td>{p.get("assists", 0)}</td>'
                f'<td>{p.get("adr", 0)}</td>'
                f'<td>{p.get("hs_percent", 0)}%</td>'
                f'<td>{kd:.2f}</td>'
                f'<td>{p.get("opening_kills", 0)}/{p.get("opening_deaths", 0)}</td>'
                f'</tr>'
            )

    if enemies:
        rows.append('<tr class="team-header enemy-header"><td colspan="8">ENEMY TEAM</td></tr>')
        for p in enemies:
            name = p.get("player_name") or "Unknown"
            if len(name) > 16:
                name = name[:15] + "…"
            kd = p.get("kills", 0) / max(p.get("deaths", 1), 1)
            rows.append(
                f'<tr class="player-row enemy-row">'
                f'<td class="name-cell">{name}</td>'
                f'<td>{p.get("kills", 0)}</td>'
                f'<td>{p.get("deaths", 0)}</td>'
                f'<td>{p.get("assists", 0)}</td>'
                f'<td>{p.get("adr", 0)}</td>'
                f'<td>{p.get("hs_percent", 0)}%</td>'
                f'<td>{kd:.2f}</td>'
                f'<td>{p.get("opening_kills", 0)}/{p.get("opening_deaths", 0)}</td>'
                f'</tr>'
            )

    return "\n".join(rows)


def _build_highlights(
    multi_kill_rounds: List[Dict],
    clutch_rounds: List[Dict],
) -> str:
    """Build HTML for notable round highlights."""
    items = []
    for mk in multi_kill_rounds:
        label = mk.get("label", "")
        emoji = "🔥" if mk.get("kills", 0) >= 4 else "⚡"
        items.append(
            f'<div class="highlight-item">'
            f'<span class="highlight-emoji">{emoji}</span>'
            f'<span class="highlight-round">R{mk["round"]}</span>'
            f'<span class="highlight-text">{label} — {mk["kills"]} kills</span>'
            f'</div>'
        )
    for cl in clutch_rounds:
        survived = "Won" if cl.get("survived") else "Lost"
        items.append(
            f'<div class="highlight-item clutch">'
            f'<span class="highlight-emoji">🎯</span>'
            f'<span class="highlight-round">R{cl["round"]}</span>'
            f'<span class="highlight-text">Clutch {cl["situation"]} — {survived}</span>'
            f'</div>'
        )
    if not items:
        return '<div class="no-data">No multi-kills or clutches</div>'
    return "\n".join(items)


def _get_map_image_class(map_name: Optional[str]) -> str:
    """Return CSS class based on map name for background theming."""
    if not map_name:
        return "map-default"
    name = map_name.lower().replace("de_", "")
    known = {"dust2", "mirage", "inferno", "nuke", "ancient", "anubis", "vertigo", "overpass", "train"}
    return f"map-{name}" if name in known else "map-default"


def build_stats_card_html(stats: Dict[str, Any], match_id: Optional[int] = None) -> str:
    """Build a complete HTML page for the stats card."""

    # Extract data
    kills = stats.get("kills", 0)
    deaths_count = stats.get("deaths", 0)
    adr = stats.get("adr", 0)
    hs_percent = stats.get("hs_percent", 0)
    kd_ratio = stats.get("kd_ratio", 0)
    opening_kills = stats.get("opening_kills", 0)
    opening_deaths = stats.get("opening_deaths", 0)
    opening_win_rate = stats.get("opening_win_rate", 0)
    ct_kd = stats.get("ct_kd")
    t_kd = stats.get("t_kd")
    ct_kills = stats.get("ct_kills")
    ct_deaths = stats.get("ct_deaths")
    t_kills = stats.get("t_kills")
    t_deaths = stats.get("t_deaths")
    fav_weapon = stats.get("fav_weapon") or "Unknown"
    common_death_weapon = stats.get("common_death_weapon") or "Unknown"
    death_headshot_rate = stats.get("death_headshot_rate", 0)
    avg_death_time = stats.get("avg_death_time_sec")
    avg_death_dist = stats.get("avg_death_distance")
    map_name = stats.get("map_name") or "Unknown"
    user_team_side = stats.get("user_team_side")
    score_ct = stats.get("score_ct")
    score_t = stats.get("score_t")
    winner = stats.get("winner")
    players_stats = stats.get("players_stats") or []
    rounds_history = stats.get("rounds") or []
    multi_kill_rounds = stats.get("multi_kill_rounds") or []
    clutch_rounds = stats.get("clutch_rounds") or []
    user_round_kills = stats.get("user_round_kills") or {}
    user_round_deaths = stats.get("user_round_deaths") or []

    # Derived
    display_map = map_name
    if display_map.startswith("de_"):
        display_map = display_map[3:].capitalize()

    match_result = "UNKNOWN"
    result_class = "result-unknown"
    if user_team_side and winner:
        if winner == "Tie":
            match_result = "DRAW"
            result_class = "result-draw"
        elif winner == user_team_side:
            match_result = "VICTORY"
            result_class = "result-win"
        else:
            match_result = "DEFEAT"
            result_class = "result-loss"

    user_score = "?"
    enemy_score = "?"
    if user_team_side and score_ct is not None and score_t is not None:
        if user_team_side == "CT":
            user_score = score_ct
            enemy_score = score_t
        else:
            user_score = score_t
            enemy_score = score_ct

    # Find the user in player stats
    target_steam_id = ""
    user_player = None
    for p in players_stats:
        if p.get("team_side") == user_team_side:
            if user_player is None or p.get("adr", 0) == adr:
                # Match by ADR to find the user
                if abs(p.get("adr", 0) - adr) < 0.2:
                    user_player = p
                    target_steam_id = str(p.get("steam_id", ""))
                    break

    # If couldn't find by ADR, try matching by kills/deaths
    if not user_player:
        for p in players_stats:
            if p.get("kills") == kills and p.get("deaths") == deaths_count:
                user_player = p
                target_steam_id = str(p.get("steam_id", ""))
                break

    player_name = (user_player.get("player_name") if user_player else None) or "Player"

    # ADR comparison
    team_adrs = [p.get("adr", 0) for p in players_stats if p.get("team_side") == user_team_side]
    team_adrs.sort(reverse=True)
    adr_rank = (team_adrs.index(adr) + 1) if adr in team_adrs else len(team_adrs)

    # Round timeline
    round_dots_html = _build_round_dots(rounds_history, user_team_side, user_round_kills, user_round_deaths)
    scoreboard_html = _build_scoreboard_rows(players_stats, user_team_side, target_steam_id)
    highlights_html = _build_highlights(multi_kill_rounds, clutch_rounds)

    # Side stats
    ct_stat_html = ""
    t_stat_html = ""
    if ct_kills is not None and ct_deaths is not None:
        ct_stat_html = f"{ct_kills}K / {ct_deaths}D"
    if t_kills is not None and t_deaths is not None:
        t_stat_html = f"{t_kills}K / {t_deaths}D"

    map_class = _get_map_image_class(map_name)
    death_time_display = f"{avg_death_time}s" if avg_death_time is not None else "N/A"
    death_dist_display = f"{avg_death_dist}u" if avg_death_dist is not None else "N/A"

    # Count rounds survived vs died
    total_rounds = len(rounds_history)
    survived_count = total_rounds - len(user_round_deaths)
    survival_rate = round(survived_count / total_rounds * 100, 1) if total_rounds else 0

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800;900&display=swap');

  * {{ margin: 0; padding: 0; box-sizing: border-box; }}

  body {{
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
    background: #0a0e17;
    color: #e2e8f0;
    width: 720px;
    padding: 0;
  }}

  .card {{
    background: linear-gradient(180deg, #0f1525 0%, #0a0e17 100%);
    border: 1px solid #1e293b;
    border-radius: 16px;
    overflow: hidden;
  }}

  /* ── Header ── */
  .header {{
    background: linear-gradient(135deg, #1a1f3a 0%, #0f1525 100%);
    padding: 28px 32px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid #1e293b;
    position: relative;
    overflow: hidden;
  }}
  .header::before {{
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0; bottom: 0;
    background: linear-gradient(135deg, rgba(99, 102, 241, 0.08) 0%, transparent 60%);
    pointer-events: none;
  }}
  .header-left {{
    z-index: 1;
  }}
  .map-name {{
    font-size: 13px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 2px;
    color: #94a3b8;
    margin-bottom: 6px;
  }}
  .result-line {{
    font-size: 36px;
    font-weight: 900;
    letter-spacing: -1px;
  }}
  .result-win {{ color: #4ade80; }}
  .result-loss {{ color: #f87171; }}
  .result-draw {{ color: #fbbf24; }}
  .result-unknown {{ color: #94a3b8; }}

  .header-right {{
    text-align: right;
    z-index: 1;
  }}
  .score {{
    font-size: 48px;
    font-weight: 900;
    letter-spacing: -2px;
  }}
  .score-user {{ color: #e2e8f0; }}
  .score-sep {{ color: #475569; margin: 0 4px; }}
  .score-enemy {{ color: #64748b; }}
  .player-name {{
    font-size: 14px;
    color: #94a3b8;
    margin-top: 4px;
  }}

  /* ── Stats Grid ── */
  .stats-grid {{
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1px;
    background: #1e293b;
    margin: 0;
  }}
  .stat-box {{
    background: #0f1525;
    padding: 20px 16px;
    text-align: center;
  }}
  .stat-value {{
    font-size: 28px;
    font-weight: 800;
    color: #f1f5f9;
    line-height: 1;
    margin-bottom: 6px;
  }}
  .stat-label {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #64748b;
  }}
  .stat-good {{ color: #4ade80; }}
  .stat-bad {{ color: #f87171; }}
  .stat-neutral {{ color: #f1f5f9; }}

  /* ── Section ── */
  .section {{
    padding: 20px 28px;
    border-top: 1px solid #1e293b;
  }}
  .section-title {{
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 2px;
    color: #64748b;
    margin-bottom: 14px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .section-title::after {{
    content: '';
    flex: 1;
    height: 1px;
    background: #1e293b;
  }}

  /* ── Opening Duels + Side ── */
  .two-col {{
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 1px;
    background: #1e293b;
  }}
  .col-box {{
    background: #0f1525;
    padding: 18px 24px;
  }}
  .mini-stat {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 0;
  }}
  .mini-label {{
    font-size: 12px;
    color: #94a3b8;
    font-weight: 500;
  }}
  .mini-value {{
    font-size: 14px;
    font-weight: 700;
    color: #e2e8f0;
  }}
  .side-ct {{ color: #60a5fa; }}
  .side-t {{ color: #fbbf24; }}

  /* ── Round Timeline ── */
  .round-timeline {{
    display: flex;
    flex-wrap: wrap;
    gap: 4px;
    align-items: center;
    justify-content: center;
  }}
  .round-dot {{
    width: 28px;
    height: 36px;
    border-radius: 4px;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    font-size: 9px;
    font-weight: 600;
    position: relative;
    transition: transform 0.1s;
  }}
  .round-win {{
    background: rgba(74, 222, 128, 0.15);
    border: 1px solid rgba(74, 222, 128, 0.3);
    color: #4ade80;
  }}
  .round-loss {{
    background: rgba(248, 113, 113, 0.15);
    border: 1px solid rgba(248, 113, 113, 0.3);
    color: #f87171;
  }}
  .round-unknown {{
    background: rgba(148, 163, 184, 0.1);
    border: 1px solid rgba(148, 163, 184, 0.2);
    color: #94a3b8;
  }}
  .round-num {{
    font-size: 8px;
    opacity: 0.7;
  }}
  .kill-badge {{
    font-size: 7px;
    font-weight: 800;
    color: #fbbf24;
  }}
  .death-mark {{
    font-size: 8px;
    color: #f87171;
    font-weight: 800;
  }}
  .halftime-marker {{
    width: 2px;
    height: 36px;
    background: #475569;
    border-radius: 1px;
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 7px;
    color: #475569;
    margin: 0 4px;
    position: relative;
  }}

  /* ── Scoreboard ── */
  .scoreboard {{
    width: 100%;
    border-collapse: collapse;
    font-size: 12px;
  }}
  .scoreboard th {{
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #64748b;
    padding: 8px 10px;
    text-align: center;
    border-bottom: 1px solid #1e293b;
  }}
  .scoreboard th:first-child {{
    text-align: left;
  }}
  .scoreboard td {{
    padding: 9px 10px;
    text-align: center;
    border-bottom: 1px solid rgba(30, 41, 59, 0.5);
    font-weight: 500;
    color: #cbd5e1;
  }}
  .name-cell {{
    text-align: left !important;
    font-weight: 600 !important;
    color: #e2e8f0 !important;
  }}
  .team-header td {{
    font-size: 10px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: #60a5fa;
    padding: 12px 10px 6px;
    border-bottom: 1px solid rgba(96, 165, 250, 0.2);
    background: rgba(96, 165, 250, 0.05);
  }}
  .enemy-header td {{
    color: #f87171;
    border-bottom-color: rgba(248, 113, 113, 0.2);
    background: rgba(248, 113, 113, 0.05);
  }}
  .user-row {{
    background: rgba(99, 102, 241, 0.08);
  }}
  .user-row td {{
    color: #f1f5f9 !important;
    font-weight: 600 !important;
  }}
  .you-tag {{
    display: inline-block;
    font-size: 8px;
    font-weight: 800;
    color: #818cf8;
    background: rgba(99, 102, 241, 0.15);
    padding: 1px 5px;
    border-radius: 3px;
    margin-left: 6px;
    vertical-align: middle;
  }}

  /* ── Highlights ── */
  .highlights-grid {{
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
  }}
  .highlight-item {{
    display: flex;
    align-items: center;
    gap: 8px;
    background: rgba(251, 191, 36, 0.08);
    border: 1px solid rgba(251, 191, 36, 0.2);
    border-radius: 8px;
    padding: 8px 14px;
    font-size: 12px;
    font-weight: 600;
  }}
  .highlight-item.clutch {{
    background: rgba(168, 85, 247, 0.08);
    border-color: rgba(168, 85, 247, 0.2);
  }}
  .highlight-emoji {{
    font-size: 16px;
  }}
  .highlight-round {{
    font-weight: 800;
    color: #fbbf24;
    font-size: 11px;
  }}
  .clutch .highlight-round {{
    color: #a855f7;
  }}
  .highlight-text {{
    color: #cbd5e1;
  }}

  /* ── Death Analysis ── */
  .death-grid {{
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
  }}
  .death-stat {{
    background: rgba(248, 113, 113, 0.05);
    border: 1px solid rgba(248, 113, 113, 0.15);
    border-radius: 8px;
    padding: 14px;
    text-align: center;
  }}
  .death-stat-value {{
    font-size: 20px;
    font-weight: 800;
    color: #f87171;
    margin-bottom: 4px;
  }}
  .death-stat-label {{
    font-size: 10px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1px;
    color: #94a3b8;
  }}

  .no-data {{
    color: #475569;
    font-size: 12px;
    font-style: italic;
    text-align: center;
    padding: 8px;
  }}

  /* ── Footer ── */
  .footer {{
    padding: 16px 28px;
    border-top: 1px solid #1e293b;
    display: flex;
    justify-content: space-between;
    align-items: center;
  }}
  .footer-brand {{
    font-size: 11px;
    font-weight: 700;
    color: #475569;
    text-transform: uppercase;
    letter-spacing: 2px;
  }}
  .footer-brand span {{
    color: #6366f1;
  }}
  .footer-date {{
    font-size: 11px;
    color: #475569;
  }}
</style>
</head>
<body>
<div class="card">

  <!-- Header -->
  <div class="header">
    <div class="header-left">
      <div class="map-name">📍 {display_map}</div>
      <div class="result-line {result_class}">{match_result}</div>
    </div>
    <div class="header-right">
      <div class="score">
        <span class="score-user">{user_score}</span>
        <span class="score-sep">:</span>
        <span class="score-enemy">{enemy_score}</span>
      </div>
      <div class="player-name">{player_name}</div>
    </div>
  </div>

  <!-- Core Stats -->
  <div class="stats-grid">
    <div class="stat-box">
      <div class="stat-value">{kills}<span style="color:#64748b;font-size:16px">/{deaths_count}</span></div>
      <div class="stat-label">K / D</div>
    </div>
    <div class="stat-box">
      <div class="stat-value {'stat-good' if adr >= 80 else 'stat-bad' if adr < 60 else 'stat-neutral'}">{adr}</div>
      <div class="stat-label">ADR</div>
    </div>
    <div class="stat-box">
      <div class="stat-value">{hs_percent}%</div>
      <div class="stat-label">HS %</div>
    </div>
    <div class="stat-box">
      <div class="stat-value {'stat-good' if kd_ratio >= 1.0 else 'stat-bad'}">{kd_ratio}</div>
      <div class="stat-label">K/D Ratio</div>
    </div>
  </div>

  <!-- Opening Duels + Side Performance -->
  <div class="two-col">
    <div class="col-box">
      <div class="section-title">Opening Duels</div>
      <div class="mini-stat">
        <span class="mini-label">Won</span>
        <span class="mini-value stat-good">{opening_kills}</span>
      </div>
      <div class="mini-stat">
        <span class="mini-label">Lost</span>
        <span class="mini-value stat-bad">{opening_deaths}</span>
      </div>
      <div class="mini-stat">
        <span class="mini-label">Win Rate</span>
        <span class="mini-value">{opening_win_rate}%</span>
      </div>
    </div>
    <div class="col-box">
      <div class="section-title">Side Performance</div>
      <div class="mini-stat">
        <span class="mini-label side-ct">CT Side</span>
        <span class="mini-value">{ct_stat_html or 'N/A'}</span>
      </div>
      <div class="mini-stat">
        <span class="mini-label side-t">T Side</span>
        <span class="mini-value">{t_stat_html or 'N/A'}</span>
      </div>
      <div class="mini-stat">
        <span class="mini-label">Survival</span>
        <span class="mini-value">{survival_rate}%</span>
      </div>
    </div>
  </div>

  <!-- Round Timeline -->
  <div class="section">
    <div class="section-title">Round Timeline</div>
    <div class="round-timeline">
      {round_dots_html}
    </div>
  </div>

  <!-- Highlights -->
  <div class="section">
    <div class="section-title">Notable Rounds</div>
    <div class="highlights-grid">
      {highlights_html}
    </div>
  </div>

  <!-- Death Analysis -->
  <div class="section">
    <div class="section-title">Death Analysis</div>
    <div class="death-grid">
      <div class="death-stat">
        <div class="death-stat-value">{death_time_display}</div>
        <div class="death-stat-label">Avg Death Time</div>
      </div>
      <div class="death-stat">
        <div class="death-stat-value">{death_dist_display}</div>
        <div class="death-stat-label">Avg Death Dist</div>
      </div>
      <div class="death-stat">
        <div class="death-stat-value">{common_death_weapon}</div>
        <div class="death-stat-label">Killed By Most</div>
      </div>
    </div>
  </div>

  <!-- Scoreboard -->
  <div class="section">
    <div class="section-title">Scoreboard</div>
    <table class="scoreboard">
      <thead>
        <tr>
          <th style="text-align:left">Player</th>
          <th>K</th>
          <th>D</th>
          <th>A</th>
          <th>ADR</th>
          <th>HS%</th>
          <th>K/D</th>
          <th>FK/FD</th>
        </tr>
      </thead>
      <tbody>
        {scoreboard_html}
      </tbody>
    </table>
  </div>

  <!-- Footer -->
  <div class="footer">
    <div class="footer-brand"><span>Retake</span>AI — CS2 Match Intelligence</div>
    <div class="footer-date">Match #{match_id or '?'}</div>
  </div>

</div>
</body>
</html>"""
    return html


async def _screenshot_html(html: str, output_path: str) -> None:
    """Use playwright to take a screenshot of the HTML."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 720, "height": 100})
        await page.set_content(html, wait_until="networkidle")
        # Wait for fonts
        await page.wait_for_timeout(500)
        # Screenshot the card element
        card = page.locator(".card")
        await card.screenshot(path=output_path, type="png")
        await browser.close()


def generate_stats_image(stats: Dict[str, Any], match_id: Optional[int] = None) -> Optional[str]:
    """
    Generate a stats card image and upload to Supabase Storage.
    Returns the public URL or None on failure.
    """
    try:
        html = build_stats_card_html(stats, match_id)

        # Write HTML to temp file and screenshot
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            png_path = tmp.name

        # Run playwright screenshot
        asyncio.run(_screenshot_html(html, png_path))

        # Read the image bytes
        with open(png_path, "rb") as f:
            image_bytes = f.read()

        # Clean up temp file
        try:
            os.unlink(png_path)
        except OSError:
            pass

        if not image_bytes:
            print("Stats card screenshot produced empty image")
            return None

        # Upload to Supabase Storage
        filename = f"match_{match_id}_{int(datetime.now().timestamp())}.png"
        public_url = _upload_to_supabase(image_bytes, filename)

        if public_url:
            print(f"Stats card uploaded: {public_url}")
        else:
            print("Stats card upload failed — Supabase Storage unavailable")

        return public_url

    except Exception as exc:
        print(f"Stats card generation failed: {exc}")
        import traceback
        traceback.print_exc()
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Arabic Coaching Tip Image Generation
# ─────────────────────────────────────────────────────────────────────────────

def _build_arabic_tip_html(tip_text: str, map_name: Optional[str] = None, result: Optional[str] = None) -> str:
    """Build HTML for rendering an Arabic coaching tip as a beautiful image."""
    
    # Clean up display values
    display_map = map_name or "Unknown"
    if display_map.startswith("de_"):
        display_map = display_map[3:].capitalize()
    display_result = result or ""
    
    # Process the tip text: convert plain text to structured HTML
    # Split by emoji headers to detect sections
    import re
    
    # Escape HTML entities
    def escape_html(text: str) -> str:
        return (text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))
    
    # Process sections
    lines = tip_text.strip().split('\n')
    html_content = []
    current_section = []
    
    # Emoji pattern for section headers
    emoji_header_pattern = re.compile(r'^[\u200F\u200E]?\s*([\U0001F300-\U0001F9FF]|[\u2600-\u26FF])')
    
    for line in lines:
        line = line.strip()
        if not line:
            if current_section:
                html_content.append('<div class="paragraph">' + '<br>'.join(current_section) + '</div>')
                current_section = []
            continue
        
        # Check if line starts with an emoji (section header)
        if emoji_header_pattern.match(line):
            if current_section:
                html_content.append('<div class="paragraph">' + '<br>'.join(current_section) + '</div>')
                current_section = []
            html_content.append(f'<div class="section">{escape_html(line)}</div>')
        else:
            current_section.append(escape_html(line))
    
    if current_section:
        html_content.append('<div class="paragraph">' + '<br>'.join(current_section) + '</div>')
    
    content_html = '\n'.join(html_content)
    
    return f'''<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
  <meta charset="UTF-8">
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+Arabic:wght@400;600;700&display=swap');
    
    * {{
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }}
    
    body {{
      font-family: 'Noto Sans Arabic', 'Segoe UI', Tahoma, sans-serif;
      background: linear-gradient(135deg, #0f0f23 0%, #1a1a3e 50%, #0f0f23 100%);
      color: #e4e4e7;
      padding: 32px;
      width: 800px;
      direction: rtl;
      line-height: 1.9;
    }}
    
    .card {{
      background: rgba(30, 30, 60, 0.95);
      border-radius: 16px;
      padding: 28px;
      border: 1px solid rgba(139, 92, 246, 0.3);
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
    }}
    
    .header {{
      display: flex;
      align-items: center;
      gap: 16px;
      margin-bottom: 24px;
      padding-bottom: 16px;
      border-bottom: 2px solid rgba(139, 92, 246, 0.4);
    }}
    
    .logo {{
      width: 48px;
      height: 48px;
      background: linear-gradient(135deg, #8b5cf6 0%, #6366f1 100%);
      border-radius: 12px;
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 24px;
    }}
    
    .title {{
      font-size: 22px;
      font-weight: 700;
      background: linear-gradient(90deg, #8b5cf6, #c4b5fd);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }}
    
    .subtitle {{
      font-size: 13px;
      color: #a1a1aa;
      margin-top: 4px;
    }}
    
    .content {{
      font-size: 15px;
    }}
    
    .section {{
      margin: 20px 0 12px 0;
      padding: 12px 16px;
      background: rgba(139, 92, 246, 0.12);
      border-radius: 8px;
      border-right: 4px solid #8b5cf6;
      font-weight: 600;
      font-size: 17px;
      color: #c4b5fd;
    }}
    
    .paragraph {{
      margin-bottom: 14px;
      padding: 0 8px;
      color: #d4d4d8;
    }}
    
    .footer {{
      margin-top: 24px;
      padding-top: 16px;
      border-top: 1px solid rgba(139, 92, 246, 0.2);
      display: flex;
      justify-content: space-between;
      align-items: center;
      color: #71717a;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="header">
      <div class="logo">🎮</div>
      <div>
        <div class="title">RetakeAI - تحليل المباراة</div>
        <div class="subtitle">نصائح تكتيكية مخصصة لك</div>
      </div>
    </div>
    <div class="content">
      {content_html}
    </div>
    <div class="footer">
      <span>RetakeAI Coach</span>
      <span>{display_map}{" • " + display_result if display_result else ""}</span>
    </div>
  </div>
</body>
</html>'''


async def _screenshot_arabic_tip(html: str, output_path: str) -> None:
    """Use playwright to take a screenshot of the Arabic tip HTML."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 800, "height": 100})
        await page.set_content(html, wait_until="networkidle")
        # Wait for fonts to load
        await page.wait_for_timeout(800)
        # Screenshot the card element
        card = page.locator(".card")
        await card.screenshot(path=output_path, type="png")
        await browser.close()


def generate_arabic_tip_image(
    tip_text: str,
    match_id: Optional[int] = None,
    map_name: Optional[str] = None,
    result: Optional[str] = None,
) -> Optional[str]:
    """
    Generate an Arabic coaching tip as a beautiful image and upload to Supabase Storage.
    Returns the public URL or None on failure.
    """
    if not tip_text or not tip_text.strip():
        return None
    
    try:
        html = _build_arabic_tip_html(tip_text, map_name, result)

        # Write to temp file and screenshot
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
            png_path = tmp.name

        # Run playwright screenshot
        asyncio.run(_screenshot_arabic_tip(html, png_path))

        # Read the image bytes
        with open(png_path, "rb") as f:
            image_bytes = f.read()

        # Clean up temp file
        try:
            os.unlink(png_path)
        except OSError:
            pass

        if not image_bytes:
            print("Arabic tip screenshot produced empty image")
            return None

        # Upload to Supabase Storage (use same bucket as stats cards)
        filename = f"tip_ar_{match_id}_{int(datetime.now().timestamp())}.png"
        public_url = _upload_to_supabase(image_bytes, filename)

        if public_url:
            print(f"Arabic tip image uploaded: {public_url}")
        else:
            print("Arabic tip upload failed — Supabase Storage unavailable")

        return public_url

    except Exception as exc:
        print(f"Arabic tip image generation failed: {exc}")
        import traceback
        traceback.print_exc()
        return None
