"""
Lichess Puzzle Leaderboard Generator
=====================================
Reads data/tactics_history.csv and produces data/leaderboard.csv.

For each user it finds:
  - Their OLDEST recorded row  -> puzzles_solved_total at tracking start
  - Their NEWEST recorded row  -> puzzles_solved_total today
  - Difference = puzzles solved since tracking began

Output is sorted descending by puzzles solved since tracking.

Run this after fetch_lichess_stats.py in the same GitHub Actions job,
or standalone: python generate_leaderboard.py
"""

import csv
import os
import sys
import datetime

IN_FILE  = "data/tactics_history.csv"
OUT_FILE = "data/leaderboard.csv"

LEADERBOARD_FIELDS = [
    "rank",
    "username",
    "puzzles_since_tracking",
    "puzzles_total_now",
    "puzzle_rating_now",
    "puzzle_rating_progress",   # prog from most recent snapshot
    "avg_bullet_blitz_rapid",
    "storm_best_score",
    "racer_best_score",
    "first_seen",
    "last_seen",
]


def load_history(path: str) -> dict:
    """
    Load CSV and return a dict keyed by username.
    Each value is a list of rows sorted by date ascending.
    """
    if not os.path.isfile(path):
        print(f"ERROR: {path} not found. Run fetch_lichess_stats.py first.")
        sys.exit(1)

    users = {}
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            username = row["username"]
            if username not in users:
                users[username] = []
            users[username].append(row)

    # Sort each user's rows by date ascending
    for username in users:
        users[username].sort(key=lambda r: r["date"])

    return users


def safe_int(value, default=None):
    try:
        return int(value) if value not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def safe_float(value, default=None):
    try:
        return float(value) if value not in (None, "", "None") else default
    except (ValueError, TypeError):
        return default


def build_leaderboard(users: dict) -> list:
    entries = []

    for username, rows in users.items():
        # Deduplicate: keep only one row per date (the last written that day)
        by_date = {}
        for row in rows:
            by_date[row["date"]] = row
        deduped = sorted(by_date.values(), key=lambda r: r["date"])

        oldest = deduped[0]
        newest = deduped[-1]

        total_now    = safe_int(newest.get("puzzles_solved_total"), 0)
        total_start  = safe_int(oldest.get("puzzles_solved_total"), 0)
        multi_day    = oldest["date"] != newest["date"]

        # Only show a delta when we have snapshots from genuinely different days.
        # On day 1 (or if history is corrupted), fall back to total count.
        solved_since = (total_now - total_start) if multi_day else total_now

        entries.append({
            "username":               username,
            "puzzles_since_tracking": solved_since,
            "puzzles_total_now":      total_now,
            "puzzle_rating_now":      safe_int(newest.get("puzzle_rating")),
            "puzzle_rating_progress": safe_int(newest.get("puzzle_rating_progress")),
            "avg_bullet_blitz_rapid": safe_float(newest.get("avg_bullet_blitz_rapid")),
            "storm_best_score":       safe_int(newest.get("storm_best_score")),
            "racer_best_score":       safe_int(newest.get("racer_best_score")),
            "first_seen":             oldest["date"],
            "last_seen":              newest["date"],
        })

    # Sort: most puzzles solved since tracking first
    entries.sort(key=lambda e: e["puzzles_since_tracking"], reverse=True)

    # Add rank
    for i, entry in enumerate(entries, start=1):
        entry["rank"] = i

    return entries


def print_leaderboard(entries: list):
    print(f"\n{'RANK':<5} {'USERNAME':<20} {'SOLVED':<8} {'RATING':<8} {'PROG':<6}")
    print("-" * 55)
    for e in entries[:20]:  # print top 20 to console
        prog = e["puzzle_rating_progress"]
        prog_str = f"{prog:+d}" if prog is not None else "?"
        print(
            f"{e['rank']:<5} {e['username']:<20} "
            f"{e['puzzles_since_tracking']:<8} "
            f"{str(e['puzzle_rating_now'] or '?'):<8} "
            f"{prog_str:<6}"
        )


def main():
    users   = load_history(IN_FILE)
    entries = build_leaderboard(users)

    os.makedirs("data", exist_ok=True)
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEADERBOARD_FIELDS)
        writer.writeheader()
        writer.writerows(entries)

    print_leaderboard(entries)
    print(f"\nLeaderboard written to '{OUT_FILE}' — {len(entries)} players ranked.")


if __name__ == "__main__":
    main()
