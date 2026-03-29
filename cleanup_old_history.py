"""
cleanup_old_history.py
=======================
Entfernt Einträge aus data/tactics_history.csv die älter als 90 Tage sind.
Die Baselines in data/baselines.csv werden NICHT verändert — die Startwerte
bleiben erhalten, egal wie alt sie sind.

Läuft wöchentlich via GitHub Actions (cleanup.yml).
Kann auch manuell ausgeführt werden: python cleanup_old_history.py
"""

import csv
import os
import sys
import datetime
import shutil

HISTORY_FILE = "data/tactics_history.csv"
KEEP_DAYS    = 90

def get_timestamp(row):
    return row.get("timestamp") or row.get("date") or ""

def parse_ts(ts: str) -> datetime.datetime | None:
    for fmt in ("%Y-%m-%d %H:%M UTC", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.datetime.strptime(ts[:len(fmt)], fmt)
        except ValueError:
            continue
    return None

def main():
    if not os.path.isfile(HISTORY_FILE):
        print(f"[INFO] {HISTORY_FILE} nicht gefunden — nichts zu tun.")
        return

    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=KEEP_DAYS)
    print(f"[INFO] Behalte Einträge ab {cutoff.strftime('%Y-%m-%d')} (letzte {KEEP_DAYS} Tage).")

    with open(HISTORY_FILE, newline="", encoding="utf-8") as f:
        reader   = csv.DictReader(f)
        fieldnames = reader.fieldnames
        all_rows = list(reader)

    before = len(all_rows)

    kept    = []
    removed = 0
    for row in all_rows:
        ts = parse_ts(get_timestamp(row))
        if ts is None or ts >= cutoff:
            kept.append(row)
        else:
            removed += 1

    if removed == 0:
        print(f"[INFO] Keine alten Einträge gefunden ({before} Zeilen behalten).")
        return

    # Backup
    backup = HISTORY_FILE.replace(".csv", f"_backup_{datetime.date.today()}.csv")
    shutil.copy(HISTORY_FILE, backup)
    print(f"[INFO] Backup gespeichert: {backup}")

    with open(HISTORY_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(kept)

    print(f"✓ {removed} alte Einträge entfernt, {len(kept)} behalten.")
    print(f"  Dateigröße vorher: {before} Zeilen → nachher: {len(kept)} Zeilen")

if __name__ == "__main__":
    main()