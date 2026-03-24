"""
Lichess Tactics & Rating Tracker
Team: hessische-schachjugend

Fetches for every team member:
  - Average rating (Bullet, Blitz, Rapid)
  - Tactic rating, total puzzles solved
  - Last-30-day puzzle success rate & average difficulty

Results are appended to  data/tactics_history.csv
Run daily via GitHub Actions (see .github/workflows/lichess_tracker.yml)
"""

import csv
import json
import os
import time
import datetime
import sys
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_KEY  = os.environ.get("LICHESS_API_KEY", "")
if not API_KEY:
    print("ERROR: LICHESS_API_KEY environment variable is not set.")
    sys.exit(1)

TEAM_ID  = "hessische-schachjugend"
BASE_URL = "https://lichess.org/api"
HEADERS  = {"Authorization": f"Bearer {API_KEY}"}
OUT_FILE = "data/tactics_history.csv"

FIELDNAMES = [
    "date",
    "username",
    "bullet_rating",
    "blitz_rating",
    "rapid_rating",
    "avg_bullet_blitz_rapid",
    "puzzle_rating",
    "puzzles_solved_total",
    "puzzles_30d_attempted",
    "puzzles_30d_solved",
    "success_rate_30d_pct",
    "avg_difficulty_30d",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_team_members(team_id: str) -> list:
    """Return a list of usernames for every member of the team."""
    url = f"{BASE_URL}/team/{team_id}/users"
    resp = requests.get(
        url,
        headers={**HEADERS, "Accept": "application/x-ndjson"},
        stream=True,
        timeout=60,
    )
    resp.raise_for_status()
    usernames = []
    for line in resp.iter_lines():
        if line:
            obj = json.loads(line)
            usernames.append(obj["username"])
    print(f"Found {len(usernames)} members in team '{team_id}'.")
    return usernames


def get_user_data(username: str) -> dict:
    """Fetch public user data (ratings, puzzle stats)."""
    url = f"{BASE_URL}/user/{username}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    if resp.status_code == 404:
        print(f"  [WARN] User '{username}' not found - skipping.")
        return {}
    resp.raise_for_status()
    return resp.json()


def get_puzzle_activity(username: str, max_puzzles: int = 200) -> list:
    """
    Fetch recent puzzle activity for a user.
    Returns entries from the last 30 days only.
    """
    url = f"{BASE_URL}/user/{username}/puzzle-activity"
    params = {"max": max_puzzles}
    try:
        resp = requests.get(
            url,
            headers={**HEADERS, "Accept": "application/x-ndjson"},
            params=params,
            stream=True,
            timeout=30,
        )
        if resp.status_code in (401, 403, 404):
            print(f"  [WARN] Puzzle activity unavailable for '{username}' (HTTP {resp.status_code}).")
            return []
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  [WARN] Could not fetch puzzle activity for '{username}': {e}")
        return []

    cutoff_ms = (
        datetime.datetime.utcnow() - datetime.timedelta(days=30)
    ).timestamp() * 1000

    entries = []
    for line in resp.iter_lines():
        if line:
            obj = json.loads(line)
            if obj.get("date", 0) >= cutoff_ms:
                entries.append(obj)
    return entries


def extract_rating(perf: dict) -> Optional[int]:
    """Safely extract rating from a performance dict."""
    if not perf:
        return None
    return perf.get("rating")


def compute_puzzle_30d_stats(activity: list) -> dict:
    """Summarise puzzle activity from the last 30 days."""
    attempted    = len(activity)
    solved       = sum(1 for e in activity if e.get("win"))
    difficulties = [
        e["puzzle"]["rating"]
        for e in activity
        if "puzzle" in e and "rating" in e["puzzle"]
    ]

    success_pct = round(solved / attempted * 100, 1) if attempted else None
    avg_diff    = round(sum(difficulties) / len(difficulties), 1) if difficulties else None

    return {
        "puzzles_30d_attempted": attempted,
        "puzzles_30d_solved":    solved,
        "success_rate_30d_pct":  success_pct,
        "avg_difficulty_30d":    avg_diff,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    today = datetime.date.today().isoformat()

    os.makedirs("data", exist_ok=True)
    file_exists = os.path.isfile(OUT_FILE) and os.path.getsize(OUT_FILE) > 0

    members = get_team_members(TEAM_ID)
    if not members:
        print("No members found - nothing to do.")
        sys.exit(0)

    rows = []
    for username in members:
        print(f"  Processing {username} ...")
        user = get_user_data(username)
        if not user:
            continue

        perfs = user.get("perfs", {})

        bullet_r = extract_rating(perfs.get("bullet"))
        blitz_r  = extract_rating(perfs.get("blitz"))
        rapid_r  = extract_rating(perfs.get("rapid"))
        puzzle_r = extract_rating(perfs.get("puzzle"))

        available  = [r for r in [bullet_r, blitz_r, rapid_r] if r is not None]
        avg_rating = round(sum(available) / len(available), 1) if available else None

        puzzles_total = perfs.get("puzzle", {}).get("games", 0)

        activity  = get_puzzle_activity(username)
        stats_30d = compute_puzzle_30d_stats(activity)

        row = {
            "date":                   today,
            "username":               username,
            "bullet_rating":          bullet_r,
            "blitz_rating":           blitz_r,
            "rapid_rating":           rapid_r,
            "avg_bullet_blitz_rapid": avg_rating,
            "puzzle_rating":          puzzle_r,
            "puzzles_solved_total":   puzzles_total,
            **stats_30d,
        }
        rows.append(row)
        print(
            f"    -> puzzle rating: {puzzle_r}, "
            f"solved (30d): {stats_30d['puzzles_30d_solved']}/{stats_30d['puzzles_30d_attempted']}"
        )

        # Stay within Lichess rate limits (~20 req/s for OAuth tokens)
        time.sleep(0.5)

    with open(OUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

    print(f"\nDone. Wrote {len(rows)} rows to '{OUT_FILE}'.")


if __name__ == "__main__":
    main()
