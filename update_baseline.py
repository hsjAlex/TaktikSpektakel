"""
update_baselines.py
====================
Läuft bei jedem 10-Minuten-Check (detect-joins Job im Workflow).

Was dieses Skript macht:
  1. Holt alle aktuellen Team-Mitglieder von Lichess
  2. Für Spieler die noch KEINE Baseline haben:
       → Ruft puzzles_solved_total direkt von der Lichess API ab
       → Speichert diesen Wert als Baseline in data/baselines.csv
  3. Für Spieler die bereits eine Baseline haben: nichts tun

Die Baseline wird NIE überschrieben — sie ist der einmalige Startwert.
Das Delta in der Rangliste ist dann immer: aktuell − Baseline.

Format baselines.csv:
  username, puzzles_solved_baseline, joined_at
"""

import csv
import json
import os
import sys
import time
import datetime
from typing import Optional

import requests

# ── Config ──────────────────────────────────────────────────────────────────
API_KEY  = os.environ.get("LICHESS_API_KEY", "")
if not API_KEY:
    print("ERROR: LICHESS_API_KEY nicht gesetzt.")
    sys.exit(1)

TEAM_ID       = "taktikspektakel"
BASE_URL      = "https://lichess.org/api"
HEADERS       = {"Authorization": f"Bearer {API_KEY}"}
BASELINE_FILE = "data/baselines.csv"
BASELINE_FIELDS = ["username", "puzzles_solved_baseline", "joined_at"]

# ── Helpers ─────────────────────────────────────────────────────────────────

def fetch_with_retry(url, headers, retries=3):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"  [429] Rate limit — warte {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)


def get_team_members() -> set:
    url  = f"{BASE_URL}/team/{TEAM_ID}/users"
    resp = fetch_with_retry(url, {**HEADERS, "Accept": "application/x-ndjson"})
    members = set()
    for line in resp.iter_lines():
        if not line:
            continue
        try:
            obj      = json.loads(line)
            username = obj.get("username") or obj.get("id")
            if username:
                members.add(username)
        except json.JSONDecodeError:
            continue
    return members


def load_baselines() -> dict:
    """Gibt bestehende Baselines zurück: username.lower() → dict"""
    if not os.path.isfile(BASELINE_FILE):
        return {}
    baselines = {}
    with open(BASELINE_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            u = row.get("username", "").lower()
            if u:
                baselines[u] = row
    return baselines


def fetch_puzzle_total(username: str) -> Optional[int]:
    """Holt puzzles_solved_total direkt von der Lichess API."""
    try:
        resp = fetch_with_retry(f"{BASE_URL}/user/{username}", HEADERS)
        data = resp.json()
        return data.get("perfs", {}).get("puzzle", {}).get("games")
    except Exception as e:
        print(f"  [WARN] Konnte {username} nicht abrufen: {e}")
        return None


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    os.makedirs("data", exist_ok=True)

    print(f"[{timestamp}] Baseline-Update gestartet...")

    team_members = get_team_members()
    baselines    = load_baselines()

    print(f"  Team: {len(team_members)} Mitglieder | Baselines vorhanden: {len(baselines)}")

    # Spieler ohne Baseline
    missing = [m for m in team_members if m.lower() not in baselines]

    if not missing:
        print("  Alle Mitglieder haben bereits eine Baseline. Fertig.")
        return

    print(f"  {len(missing)} Spieler ohne Baseline: {', '.join(sorted(missing))}")

    new_baselines = []
    for username in sorted(missing):
        print(f"  → Baseline für '{username}' von API abrufen...")
        total = fetch_puzzle_total(username)

        if total is None:
            print(f"    ✗ Kein Wert erhalten — übersprungen.")
            continue

        baselines[username.lower()] = {
            "username":                username,
            "puzzles_solved_baseline": total,
            "joined_at":               timestamp,
        }
        new_baselines.append(username)
        print(f"    ✓ Baseline = {total} Aufgaben (Stand: {timestamp})")
        time.sleep(0.3)

    if not new_baselines:
        print("  Keine neuen Baselines gespeichert.")
        return

    # Schreibe baselines.csv (alle Einträge, sortiert)
    with open(BASELINE_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BASELINE_FIELDS)
        writer.writeheader()
        for u in sorted(baselines.keys()):
            row = baselines[u]
            writer.writerow({
                "username":                row["username"],
                "puzzles_solved_baseline": row["puzzles_solved_baseline"],
                "joined_at":               row["joined_at"],
            })

    print(f"\n  ✓ {len(new_baselines)} neue Baseline(s) in '{BASELINE_FILE}' gespeichert.")


if __name__ == "__main__":
    main()