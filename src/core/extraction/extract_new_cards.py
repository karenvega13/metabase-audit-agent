"""
src/core/extraction/extract_new_cards.py
Extrae las cards nuevas detectadas por freshness_check y genera lotes SQL
para ser auditados por audit_agent.py.

Lee:   data/processed/diccionario/freshness_report.json (lista de IDs nuevos)
Escribe: data/raw/lotes/lote_62.sql, lote_63.sql, ...

Uso:
    python -m src.core.extraction.extract_new_cards
    python -m src.core.extraction.extract_new_cards --dry-run   # solo imprime conteo
    python -m src.core.extraction.extract_new_cards --batch-size 50  # default: 50
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_ROOT          = Path(__file__).resolve().parents[3]
FRESHNESS_FILE = _ROOT / "data" / "processed" / "diccionario" / "freshness_report.json"
LOTES_DIR      = _ROOT / "data" / "raw" / "lotes"
RESULTS_DIR    = _ROOT / "data" / "processed" / "resultados"

METABASE_URL   = "https://metabase.example.com"
METABASE_TOKEN = os.environ.get("METABASE_SESSION", "")

DELAY_BETWEEN_REQUESTS = 0.15  # segundos entre llamadas a la API
BATCH_SIZE_DEFAULT     = 50


def _headers() -> dict:
    if not METABASE_TOKEN:
        raise EnvironmentError("METABASE_SESSION no definida. Exporta la variable antes de correr.")
    return {"X-Metabase-Session": METABASE_TOKEN}


def fetch_card(card_id: int) -> dict | None:
    """
    Obtiene metadata + SQL de una card via GET /api/card/{id}.
    Retorna dict con: id, name, vistas, collection, sql, query_type
    o None si falla.
    """
    url = f"{METABASE_URL}/api/card/{card_id}"
    try:
        resp = requests.get(url, headers=_headers(), timeout=20)
        if resp.status_code == 404:
            return None
        if resp.status_code == 403:
            return None
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  WARN card #{card_id}: {e}")
        return None

    query_type = data.get("query_type", "") or ""
    archived   = data.get("archived", False)
    if archived:
        return None

    sql = None
    if query_type == "native":
        try:
            sql = data["dataset_query"]["native"]["query"]
        except (KeyError, TypeError):
            sql = None

    collection_name = "Sin colección"
    coll = data.get("collection")
    if isinstance(coll, dict):
        collection_name = coll.get("name", "Sin colección") or "Sin colección"

    vistas = data.get("view_count", 0) or 0

    return {
        "id":         card_id,
        "name":       data.get("name", f"Card #{card_id}"),
        "vistas":     vistas,
        "collection": collection_name,
        "sql":        sql,
        "query_type": query_type,
    }


def _next_lote_number() -> int:
    """Determina el siguiente número de lote disponible."""
    existing = list(LOTES_DIR.glob("lote_*.sql"))
    nums = []
    for f in existing:
        import re
        m = re.match(r"lote_(\d+)", f.stem)
        if m:
            nums.append(int(m.group(1)))
    return max(nums) + 1 if nums else 62


def write_lote(cards: list[dict], lote_num: int, total_lotes: int, lote_start: int) -> Path:
    """
    Escribe un archivo .sql con el formato esperado por audit_agent.py.
    """
    filename = LOTES_DIR / f"lote_{lote_num:02d}.sql"

    lines = [
        "-- " + "=" * 60,
        f"-- LOTE {lote_num:02d} DE {lote_start + total_lotes - 1:02d}",
        "-- Tipo:   NUEVAS (extraidas de freshness_check)",
        f"-- Cards:  {len(cards)}",
        "-- " + "=" * 60,
    ]

    for card in cards:
        name      = card["name"].replace("\n", " ")
        vistas    = f"{card['vistas']:,}"
        coll      = card["collection"]
        sql       = card["sql"] or ""
        qtype     = card["query_type"]

        lines.append(f"\n-- Card: {name} (id:{card['id']} | vistas:{vistas})")
        lines.append(f"-- Colección: {coll}")

        if sql:
            lines.append(sql.rstrip())
            if not sql.rstrip().endswith(";"):
                lines.append(";")
        else:
            lines.append(f"-- [QUERY BUILDER — tipo: {qtype}]")
            lines.append(";")

    content = "\n".join(lines) + "\n"
    filename.write_text(content, encoding="utf-8")
    return filename


def main():
    parser = argparse.ArgumentParser(description="Extrae cards nuevas de Metabase y genera lotes SQL")
    parser.add_argument("--dry-run",    action="store_true", help="Solo imprime conteo, no escribe archivos")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE_DEFAULT, help=f"Cards por lote (default: {BATCH_SIZE_DEFAULT})")
    parser.add_argument("--limit",      type=int, default=0, help="Limita a N cards (dev/test)")
    args = parser.parse_args()

    print("=" * 62)
    print("  Extracción de Cards Nuevas — Metabase → Lotes SQL")
    print("=" * 62)

    # 1. Leer freshness report
    if not FRESHNESS_FILE.exists():
        print(f"ERROR: {FRESHNESS_FILE} no existe. Ejecuta freshness_check.py primero.")
        sys.exit(1)

    rep      = json.loads(FRESHNESS_FILE.read_text(encoding="utf-8"))
    new_list = rep.get("new_cards", [])
    total_new = len(new_list)
    print(f"\n  Cards nuevas en freshness_report: {total_new}")

    if args.limit:
        new_list = new_list[:args.limit]
        print(f"  (limitado a {args.limit} por --limit)")

    if not new_list:
        print("  Nada que extraer.")
        return

    # 2. Verificar cuáles ya tienen lote resultado (por si se corre dos veces)
    existing_audited = set()
    for jf in RESULTS_DIR.glob("lote_*_resultados.json"):
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
            for card in data.get("cards", []):
                existing_audited.add(card.get("card_id"))
        except Exception:
            continue

    pending_ids = [c["id"] for c in new_list if c["id"] not in existing_audited]
    already_done = total_new - len(pending_ids)
    if already_done:
        print(f"  Ya auditadas (skipping): {already_done}")
    print(f"  A extraer: {len(pending_ids)}")

    if args.dry_run:
        batches = [pending_ids[i:i+args.batch_size] for i in range(0, len(pending_ids), args.batch_size)]
        lote_start = _next_lote_number()
        print(f"\n  DRY-RUN: se crearían {len(batches)} lotes a partir de lote_{lote_start:02d}.sql")
        for i, batch in enumerate(batches):
            print(f"    lote_{lote_start + i:02d}.sql — {len(batch)} cards")
        return

    if not pending_ids:
        print("  Todas las cards nuevas ya están auditadas.")
        return

    # 3. Fetch desde Metabase
    print(f"\n  Fetching {len(pending_ids)} cards desde Metabase API...")
    fetched_sql   = []
    fetched_nogql = []
    failed        = []

    for i, card_id in enumerate(pending_ids, 1):
        if i % 50 == 0 or i == 1:
            print(f"    [{i}/{len(pending_ids)}] fetching card #{card_id}...")
        card = fetch_card(card_id)
        if card is None:
            failed.append(card_id)
        elif card["sql"]:
            fetched_sql.append(card)
        else:
            fetched_nogql.append(card)  # query builder o sin SQL
        time.sleep(DELAY_BETWEEN_REQUESTS)

    print(f"\n  Resultado fetch:")
    print(f"    Con SQL nativo:  {len(fetched_sql)}")
    print(f"    Sin SQL (QB/native vacio): {len(fetched_nogql)}")
    print(f"    Fallidas (403/404/error):  {len(failed)}")

    # Incluimos también las query-builder (marcadas) para no perder el registro
    all_cards = fetched_sql + fetched_nogql
    if not all_cards:
        print("  Sin cards para escribir.")
        return

    # 4. Generar lotes
    lote_start  = _next_lote_number()
    batches     = [all_cards[i:i+args.batch_size] for i in range(0, len(all_cards), args.batch_size)]
    lotes_written = []

    print(f"\n  Escribiendo {len(batches)} lotes (desde lote_{lote_start:02d})...")
    for i, batch in enumerate(batches):
        lote_num = lote_start + i
        path = write_lote(batch, lote_num, len(batches), lote_start)
        lotes_written.append(path)
        sql_count = sum(1 for c in batch if c["sql"])
        print(f"    {path.name} — {len(batch)} cards ({sql_count} con SQL)")

    if failed:
        failed_file = LOTES_DIR / "failed_extraction.json"
        failed_file.write_text(json.dumps({"failed_ids": failed}, indent=2), encoding="utf-8")
        print(f"\n  IDs fallidas guardadas en: {failed_file.name}")

    print(f"\n  Lotes creados: {len(lotes_written)}")
    print(f"  Cards con SQL listas para auditar: {len(fetched_sql)}")
    print()
    print("  Siguiente paso:")
    nums = [f"{lote_start + i:02d}" for i in range(len(batches))]
    lote_args = " ".join(f"--lote {n}" for n in nums)
    print(f"    python src/core/audit_agent.py {lote_args}")
    print("=" * 62)


if __name__ == "__main__":
    main()
