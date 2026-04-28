"""
snapshot_popularity.py — Snapshot diario de view_count + last_used_at por card.

Metabase OSS no expone view_log, así que no hay unique_viewers ni actividad
por ventana. Lo que sí expone GET /api/card/{id} son los contadores lifetime:
view_count (vistas acumuladas) y last_used_at (última ejecución).

Este script guarda un snapshot diario para que compute_popularity.py pueda
calcular deltas 7d/30d (popularidad real, no sesgada por edad de la card).

Uso:
    python -m src.core.extraction.snapshot_popularity              # snapshot real
    python -m src.core.extraction.snapshot_popularity --dry-run    # 5 cards, verifica auth

Output: data/processed/diccionario/popularity_snapshots/YYYY-MM-DD.json
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from src.utils.metabase_client import METABASE_URL, make_headers

ROOT = Path(__file__).resolve().parents[3]
METRICS_PATH = ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
SNAPSHOTS_DIR = ROOT / "data" / "processed" / "diccionario" / "popularity_snapshots"
THROTTLE_SECONDS = 0.3
RETENTION_DAYS = 90


def _fetch_card(card_id: int, headers: dict) -> dict:
    req = urllib.request.Request(
        f"{METABASE_URL}/api/card/{card_id}",
        headers=headers,
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"_error": f"HTTP {e.code}", "card_id": card_id}
    except Exception as e:
        return {"_error": str(e), "card_id": card_id}


def _target_card_ids(metrics: list[dict]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for m in metrics:
        if not m.get("is_primary", True):
            continue
        cid = m.get("primary_card_id")
        if cid is None or cid in seen:
            continue
        seen.add(int(cid))
        out.append(int(cid))
    return out


def _load_metrics_from_master() -> list[dict]:
    if not METRICS_PATH.exists():
        print(f"ERROR: no existe {METRICS_PATH}")
        sys.exit(1)
    data = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
    return data.get("metrics", [])


def _rotate_old_snapshots(keep_days: int = RETENTION_DAYS) -> int:
    if not SNAPSHOTS_DIR.exists():
        return 0
    cutoff = _dt.date.today() - _dt.timedelta(days=keep_days)
    removed = 0
    for f in SNAPSHOTS_DIR.glob("*.json"):
        try:
            file_date = _dt.date.fromisoformat(f.stem)
        except ValueError:
            continue
        if file_date < cutoff:
            f.unlink()
            removed += 1
    return removed


def run(metrics: list[dict] | None = None, dry_run: bool = False) -> Path | None:
    """Ejecuta el snapshot. Retorna la ruta del archivo escrito (o None en dry-run)."""
    if metrics is None:
        metrics = _load_metrics_from_master()

    card_ids = _target_card_ids(metrics)
    if not card_ids:
        print("AVISO: no hay cards primarias para snapshotear")
        return None

    headers = make_headers()

    if dry_run:
        card_ids = card_ids[:5]
        print(f"[dry-run] Consultando {len(card_ids)} cards de muestra...")
    else:
        print(f"Consultando {len(card_ids)} cards en Metabase (throttle {THROTTLE_SECONDS}s)...")

    rows: list[dict] = []
    errors: list[dict] = []

    for i, cid in enumerate(card_ids, 1):
        data = _fetch_card(cid, headers)
        if "_error" in data:
            errors.append({"card_id": cid, "error": data["_error"]})
            if dry_run or i % 100 == 0:
                print(f"  [{i}/{len(card_ids)}] #{cid} ERROR {data['_error']}")
        else:
            rows.append({
                "card_id": data.get("id"),
                "view_count": data.get("view_count") or 0,
                "last_used_at": data.get("last_used_at"),
                "archived": bool(data.get("archived", False)),
            })
            if dry_run:
                print(f"  [{i}/{len(card_ids)}] #{cid} views={rows[-1]['view_count']} last={rows[-1]['last_used_at']}")
            elif i % 200 == 0:
                print(f"  [{i}/{len(card_ids)}] OK")
        time.sleep(THROTTLE_SECONDS)

    if dry_run:
        print(f"\n[dry-run] {len(rows)} ok, {len(errors)} errores — no se escribió snapshot.")
        return None

    today = _dt.date.today().isoformat()
    SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SNAPSHOTS_DIR / f"{today}.json"
    payload = {
        "fetched_at": _dt.datetime.now().isoformat(timespec="seconds"),
        "snapshot_date": today,
        "cards": rows,
        "errors": errors,
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    removed = _rotate_old_snapshots()
    print(f"\nOK: {out_path.relative_to(ROOT)} ({len(rows)} cards, {len(errors)} errores)")
    if removed:
        print(f"  Rotación: {removed} snapshot(s) >{RETENTION_DAYS}d eliminados")
    return out_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Snapshot diario de popularidad Metabase")
    parser.add_argument("--dry-run", action="store_true", help="5 cards de muestra, no escribe")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
