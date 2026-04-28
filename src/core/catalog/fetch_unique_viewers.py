"""
src/core/catalog/fetch_unique_viewers.py — Extrae unique_viewers_* por card desde Metabase.

Produce data/raw/metabase_unique_viewers.csv con columnas:
    card_id, unique_viewers_30d, unique_viewers_90d, unique_viewers_lifetime, total_views_30d

Estrategia de extracción (fallback escalonado):

1. **Audit/Analytics DB** (preferido). Busca en GET /api/database una base con
   flag `is_audit=True` o nombre "Metabase Analytics" / "Audit". Corre SQL nativo
   sobre `view_log`:
       SELECT model_id, COUNT(DISTINCT user_id)
       FROM view_log
       WHERE model='card' AND timestamp >= now()-interval '30 days'
       GROUP BY model_id
   (se corre 3 veces: 30d / 90d / lifetime).

2. **Fallback silencioso**: si no existe Audit DB accesible, el CSV se escribe
   con `unique_viewers_* = null`. El auditor consumidor debe marcar flag
   `uniq_viewers_unavailable` en el reporte.

Requiere: METABASE_SESSION en config/api_key.env (via src/utils/metabase_client.py).

Uso:
    python -m src.core.catalog.fetch_unique_viewers
    python -m src.core.catalog.fetch_unique_viewers --dry-run   # No escribe, solo diagnostica
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import requests

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

from src.utils.metabase_client import METABASE_URL, get_session_token, make_headers  # noqa: E402

MASTER_FILE = _ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
OUTPUT_CSV = _ROOT / "data" / "raw" / "metabase_unique_viewers.csv"

AUDIT_DB_NAME_HINTS = ("audit", "analytics", "usage", "instance")

SQL_UNIQUE_VIEWERS_WINDOW = """
SELECT model_id AS card_id,
       COUNT(DISTINCT user_id) AS unique_viewers,
       COUNT(*) AS total_views
FROM view_log
WHERE model = 'card'
  AND timestamp >= NOW() - INTERVAL '{days} days'
GROUP BY model_id
"""

SQL_UNIQUE_VIEWERS_LIFETIME = """
SELECT model_id AS card_id,
       COUNT(DISTINCT user_id) AS unique_viewers
FROM view_log
WHERE model = 'card'
GROUP BY model_id
"""


def find_audit_database(headers: dict) -> dict | None:
    """Busca una DB en Metabase que exponga tablas de auditoría (view_log)."""
    url = f"{METABASE_URL}/api/database"
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except (requests.RequestException, requests.HTTPError) as e:
        print(f"  ! Error consultando /api/database: {e}")
        return None

    payload = resp.json()
    dbs = payload.get("data", payload) if isinstance(payload, dict) else payload
    if not isinstance(dbs, list):
        return None

    for db in dbs:
        if not isinstance(db, dict):
            continue
        if db.get("is_audit"):
            return db
        name = (db.get("name") or "").lower()
        if any(hint in name for hint in AUDIT_DB_NAME_HINTS):
            return db
    return None


def run_native_query(headers: dict, db_id: int, sql: str) -> list[dict] | None:
    """Ejecuta SQL nativo via POST /api/dataset. Devuelve rows o None si falla."""
    url = f"{METABASE_URL}/api/dataset"
    body = {
        "database": db_id,
        "type": "native",
        "native": {"query": sql.strip()},
    }
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=120)
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  ! Error POST /api/dataset: {e}")
        return None

    data = resp.json()
    if data.get("status") == "failed" or "error" in data:
        print(f"  ! Query falló: {data.get('error', '<sin detalle>')}")
        return None

    rows = data.get("data", {}).get("rows") or []
    cols = [c.get("name") for c in data.get("data", {}).get("cols", [])]
    return [dict(zip(cols, row)) for row in rows]


def load_card_ids() -> list[int]:
    if not MASTER_FILE.exists():
        print(f"ERROR: no existe {MASTER_FILE}. Corre pipeline.py primero.")
        sys.exit(1)
    data = json.loads(MASTER_FILE.read_text(encoding="utf-8"))
    ids: set[int] = set()
    for m in data.get("metrics", []):
        primary = m.get("primary_card_id")
        if primary is not None:
            ids.add(int(primary))
        for cid in m.get("duplicate_card_ids") or []:
            if cid is not None:
                ids.add(int(cid))
        for cid in m.get("variation_card_ids") or []:
            if cid is not None:
                ids.add(int(cid))
    return sorted(ids)


def write_empty_csv(card_ids: list[int], reason: str) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "card_id",
            "unique_viewers_30d",
            "unique_viewers_90d",
            "unique_viewers_lifetime",
            "total_views_30d",
            "_source",
        ])
        for cid in card_ids:
            w.writerow([cid, "", "", "", "", "unavailable"])
    print(f"\nOK: CSV escrito con NULLs en {OUTPUT_CSV.relative_to(_ROOT)}")
    print(f"    Razón: {reason}")
    print(f"    Filas: {len(card_ids)}")


def write_csv(card_ids: list[int], data_by_card: dict[int, dict], source_label: str) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    non_null = 0
    with OUTPUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "card_id",
            "unique_viewers_30d",
            "unique_viewers_90d",
            "unique_viewers_lifetime",
            "total_views_30d",
            "_source",
        ])
        for cid in card_ids:
            row = data_by_card.get(cid, {})
            v30 = row.get("u30")
            v90 = row.get("u90")
            vlt = row.get("ult")
            t30 = row.get("t30")
            if any(x is not None for x in (v30, v90, vlt, t30)):
                non_null += 1
            w.writerow([
                cid,
                "" if v30 is None else int(v30),
                "" if v90 is None else int(v90),
                "" if vlt is None else int(vlt),
                "" if t30 is None else int(t30),
                source_label,
            ])
    print(f"\nOK: CSV escrito en {OUTPUT_CSV.relative_to(_ROOT)}")
    print(f"    Filas totales:   {len(card_ids)}")
    print(f"    Con datos:       {non_null}")
    print(f"    Sin datos (NULL): {len(card_ids) - non_null}")
    print(f"    Fuente:          {source_label}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Extrae unique_viewers por card desde Metabase")
    parser.add_argument("--dry-run", action="store_true", help="No escribe CSV, solo diagnostica")
    args = parser.parse_args()

    print("=" * 60)
    print("  fetch_unique_viewers — extracción Metabase")
    print("=" * 60)

    card_ids = load_card_ids()
    print(f"Cards a cubrir: {len(card_ids)}")

    token = get_session_token()
    headers = make_headers(token)

    print("\nPaso 1/3: buscar Audit/Analytics DB en Metabase…")
    audit_db = find_audit_database(headers)

    if not audit_db:
        msg = "No se encontró Audit/Analytics DB accesible (probablemente OSS sin Pro/Enterprise)."
        print(f"  ! {msg}")
        if args.dry_run:
            print("\n[dry-run] NO se escribe CSV.")
            return 0
        write_empty_csv(card_ids, reason=msg)
        return 0

    db_id = audit_db.get("id")
    print(f"  OK: Audit DB encontrada — id={db_id} · name='{audit_db.get('name')}'")

    print("\nPaso 2/3: probar acceso a view_log…")
    probe = run_native_query(headers, db_id, "SELECT 1 AS ok FROM view_log LIMIT 1")
    if probe is None:
        msg = "DB encontrada pero view_log no accesible (permisos o tabla inexistente)."
        print(f"  ! {msg}")
        if args.dry_run:
            return 0
        write_empty_csv(card_ids, reason=msg)
        return 0
    print("  OK: view_log accesible.")

    print("\nPaso 3/3: extracción (3 ventanas)…")
    data_by_card: dict[int, dict] = {cid: {} for cid in card_ids}

    for label, days, key_u, key_t in [
        ("30d", 30, "u30", "t30"),
        ("90d", 90, "u90", None),
    ]:
        print(f"  - ventana {label}…", end=" ", flush=True)
        rows = run_native_query(headers, db_id, SQL_UNIQUE_VIEWERS_WINDOW.format(days=days))
        if rows is None:
            print("FALLÓ")
            continue
        print(f"{len(rows)} cards con views")
        for r in rows:
            cid = r.get("card_id")
            if cid is None:
                continue
            cid = int(cid)
            if cid in data_by_card:
                data_by_card[cid][key_u] = r.get("unique_viewers")
                if key_t:
                    data_by_card[cid][key_t] = r.get("total_views")

    print("  - ventana lifetime…", end=" ", flush=True)
    rows = run_native_query(headers, db_id, SQL_UNIQUE_VIEWERS_LIFETIME)
    if rows is None:
        print("FALLÓ")
    else:
        print(f"{len(rows)} cards con views")
        for r in rows:
            cid = r.get("card_id")
            if cid is None:
                continue
            cid = int(cid)
            if cid in data_by_card:
                data_by_card[cid]["ult"] = r.get("unique_viewers")

    if args.dry_run:
        with_data = sum(1 for d in data_by_card.values() if d)
        print(f"\n[dry-run] {with_data}/{len(card_ids)} cards con datos. NO se escribe CSV.")
        return 0

    write_csv(card_ids, data_by_card, source_label=f"audit_db_{db_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
