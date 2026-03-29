"""
generate_leaderboard.py
=======================
Liest data/tactics_history.csv und data/baselines.csv.

Für jeden Spieler:
  puzzles_since_tracking = puzzles_solved_total (aktuell)
                         - puzzles_solved_baseline (aus baselines.csv)

Falls kein Baseline-Eintrag vorhanden:
  Fallback → ältester History-Eintrag als Baseline.
"""

import csv
import os
import sys

HISTORY_FILE  = "data/tactics_history.csv"
BASELINE_FILE = "data/baselines.csv"
OUT_FILE      = "data/leaderboard.csv"

LEADERBOARD_FIELDS = [
    "rank",
    "username",
    "puzzles_since_tracking",
    "puzzles_total_now",
    "puzzle_rating_now",
    "puzzle_rating_progress",
    "avg_bullet_blitz_rapid",
    "storm_best_score",
    "racer_best_score",
    "first_seen",
    "last_seen",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_timestamp(row):
    return row.get("timestamp") or row.get("date") or ""

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

# ---------------------------------------------------------------------------
# Load baselines  (username.lower() → dict)
# ---------------------------------------------------------------------------

def load_baselines() -> dict:
    baselines = {}
    if not os.path.isfile(BASELINE_FILE):
        print(f"[INFO] Keine {BASELINE_FILE} gefunden – nutze History-Fallback.")
        return baselines
    with open(BASELINE_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = row.get("username", "").lower()
            if u and u not in baselines:   # keep first (= earliest) entry
                baselines[u] = {
                    "puzzles_solved_baseline": safe_int(row.get("puzzles_solved_baseline"), 0),
                    "joined_at":               row.get("joined_at", ""),
                }
    print(f"[INFO] {len(baselines)} Baselines geladen.")
    return baselines

# ---------------------------------------------------------------------------
# Load history
# ---------------------------------------------------------------------------

def load_history() -> dict:
    if not os.path.isfile(HISTORY_FILE):
        print(f"ERROR: {HISTORY_FILE} nicht gefunden.")
        sys.exit(1)

    users = {}
    with open(HISTORY_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = row["username"].lower()  # case-insensitive
            users.setdefault(u, []).append(row)

    for u in users:
        users[u].sort(key=get_timestamp)

    print(f"[INFO] History für {len(users)} Spieler geladen.")
    return users

# ---------------------------------------------------------------------------
# Build leaderboard
# ---------------------------------------------------------------------------

def build_leaderboard(users: dict, baselines: dict) -> list:
    entries = []

    for username, rows in users.items():
        # deduplicate: keep latest row per timestamp string
        by_ts = {}
        for row in rows:
            by_ts[get_timestamp(row)] = row
        deduped = sorted(by_ts.values(), key=get_timestamp)

        if not deduped:
            continue

        oldest    = deduped[0]
        newest    = deduped[-1]
        total_now = safe_int(newest.get("puzzles_solved_total"), 0)

        # Baseline: prefer baselines.csv entry, fall back to oldest history row
        key = username.lower()
        if key in baselines:
            baseline_total = baselines[key]["puzzles_solved_baseline"]
            first_seen     = baselines[key]["joined_at"] or get_timestamp(oldest)
        else:
            baseline_total = safe_int(oldest.get("puzzles_solved_total"), 0)
            first_seen     = get_timestamp(oldest)

        solved_since = total_now - baseline_total

        entries.append({
            "username":               username,
            "puzzles_since_tracking": solved_since,
            "puzzles_total_now":      total_now,
            "puzzle_rating_now":      safe_int(newest.get("puzzle_rating")),
            "puzzle_rating_progress": safe_int(newest.get("puzzle_rating_progress")),
            "avg_bullet_blitz_rapid": safe_float(newest.get("avg_bullet_blitz_rapid")),
            "storm_best_score":       safe_int(newest.get("storm_best_score")),
            "racer_best_score":       safe_int(newest.get("racer_best_score")),
            "first_seen":             first_seen,
            "last_seen":              get_timestamp(newest),
        })

    # sort: most solved first, then by total as tiebreaker
    entries.sort(key=lambda e: (-(e["puzzles_since_tracking"] or 0), -(e["puzzles_total_now"] or 0)))

    # stable rank
    rank = 1
    prev = None
    for i, e in enumerate(entries):
        val = (e["puzzles_since_tracking"], e["puzzles_total_now"])
        if val != prev:
            rank = i + 1
        e["rank"] = rank
        prev = val

    return entries

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def print_leaderboard(entries):
    print(f"\n{'RANG':<5} {'SPIELER':<22} {'GELÖST':<10} {'WERTUNG':<8} {'PROG':<6}")
    print("-" * 58)
    for e in entries[:20]:
        prog = e["puzzle_rating_progress"]
        print(
            f"{e['rank']:<5} {e['username']:<22} "
            f"{str(e['puzzles_since_tracking']):<10} "
            f"{str(e['puzzle_rating_now'] or '?'):<8} "
            f"{(f'{prog:+d}' if prog is not None else '?'):<6}"
        )

def main():
    baselines = load_baselines()
    users     = load_history()
    entries   = build_leaderboard(users, baselines)

    os.makedirs("data", exist_ok=True)
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=LEADERBOARD_FIELDS)
        writer.writeheader()
        writer.writerows(entries)

    print_leaderboard(entries)
    print(f"\nRangliste geschrieben: '{OUT_FILE}' — {len(entries)} Spieler.")

if __name__ == "__main__":
    main()