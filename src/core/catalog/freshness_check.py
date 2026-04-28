"""
src/core/catalog/freshness_check.py — Detección de drift entre el diccionario y Metabase.

Compara el timestamp de la última generación del diccionario contra las cards
en Metabase (via API) para detectar:
  - Cards NUEVAS: creadas después de la última ejecución del pipeline
  - Cards MODIFICADAS: cuyo SQL/nombre cambió después de la última ejecución
  - Cards ELIMINADAS: que estaban en el catálogo pero ya no existen en Metabase

Genera data/processed/diccionario/freshness_report.json.

Uso:
    python -m src.core.catalog.freshness_check
    python -m src.core.catalog.freshness_check --summary   # Solo imprime resumen
    python -m src.core.catalog.freshness_check --slack     # Envía alerta a Slack si hay cambios
    python -m src.core.catalog.freshness_check --threshold 5  # Alerta si hay >5 cambios (default: 10)
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

MASTER_FILE    = _ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
REPORT_FILE    = _ROOT / "data" / "processed" / "diccionario" / "freshness_report.json"

METABASE_URL   = os.environ.get("METABASE_URL", "https://metabase.example.com")
METABASE_TOKEN = os.environ.get("METABASE_SESSION", "")
SLACK_WEBHOOK  = os.environ.get("SLACK_WEBHOOK_URL", "")


# ---------------------------------------------------------------------------
# Metabase API helpers
# ---------------------------------------------------------------------------

def _get_headers() -> dict:
    if not METABASE_TOKEN:
        raise EnvironmentError(
            "METABASE_SESSION no definida. "
            "Exporta: export METABASE_SESSION=<tu_token>"
        )
    return {"X-Metabase-Session": METABASE_TOKEN}


def fetch_all_cards_meta() -> list[dict]:
    """
    Obtiene metadata de todas las cards via GET /api/card.
    Retorna lista con: id, name, updated_at, archived.
    """
    url = f"{METABASE_URL}/api/card"
    resp = requests.get(url, headers=_get_headers(), timeout=60)
    resp.raise_for_status()
    cards = resp.json()
    return [
        {
            "id":         c["id"],
            "name":       c.get("name", ""),
            "updated_at": c.get("updated_at", ""),
            "archived":   c.get("archived", False),
        }
        for c in cards
        if isinstance(c, dict)
    ]


# ---------------------------------------------------------------------------
# Freshness logic
# ---------------------------------------------------------------------------

def _parse_dt(dt_str: str) -> datetime | None:
    if not dt_str:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(dt_str, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def run(summary_only: bool = False, send_slack: bool = False, threshold: int = 10) -> dict:
    if not MASTER_FILE.exists():
        print(f"ERROR: No se encontró el catálogo en {MASTER_FILE}")
        sys.exit(1)

    master = json.loads(MASTER_FILE.read_text(encoding="utf-8"))
    generated_at_str = master.get("generated_at", "")
    generated_at = _parse_dt(generated_at_str)

    if not generated_at:
        print(f"ERROR: No se pudo parsear generated_at='{generated_at_str}' del catálogo.")
        sys.exit(1)

    # IDs conocidos en el catálogo
    catalog_ids = {m["primary_card_id"] for m in master.get("metrics", [])}
    catalog_names = {m["primary_card_id"]: m.get("primary_card_name", "") for m in master.get("metrics", [])}

    print(f"Catálogo generado: {generated_at_str}")
    print(f"Cards en catálogo: {len(catalog_ids)}")
    print("Consultando Metabase API...")

    try:
        live_cards = fetch_all_cards_meta()
    except EnvironmentError as e:
        print(f"ERROR: {e}")
        sys.exit(1)
    except requests.HTTPError as e:
        print(f"ERROR HTTP al consultar Metabase: {e}")
        sys.exit(1)

    live_ids = {c["id"] for c in live_cards if not c["archived"]}
    live_map = {c["id"]: c for c in live_cards}

    new_cards      = []
    modified_cards = []
    deleted_cards  = []

    # Nuevas y modificadas
    for card in live_cards:
        cid = card["id"]
        if card["archived"]:
            continue
        updated = _parse_dt(card["updated_at"])
        if cid not in catalog_ids:
            new_cards.append({
                "id":         cid,
                "name":       card["name"],
                "updated_at": card["updated_at"],
            })
        elif updated and generated_at and updated > generated_at:
            modified_cards.append({
                "id":           cid,
                "name":         card["name"],
                "updated_at":   card["updated_at"],
                "catalog_name": catalog_names.get(cid, ""),
                "name_changed": card["name"] != catalog_names.get(cid, ""),
            })

    # Eliminadas (en catálogo pero no en Metabase o archivadas)
    for cid in catalog_ids:
        if cid not in live_ids:
            card_meta = live_map.get(cid, {})
            deleted_cards.append({
                "id":           cid,
                "catalog_name": catalog_names.get(cid, ""),
                "archived":     card_meta.get("archived", False) if card_meta else True,
            })

    total_changes = len(new_cards) + len(modified_cards) + len(deleted_cards)
    days_since = (datetime.now(timezone.utc) - generated_at).days if generated_at else None

    report = {
        "generated_at":   datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "catalog_timestamp": generated_at_str,
        "days_since_pipeline": days_since,
        "summary": {
            "total_changes":   total_changes,
            "new_cards":       len(new_cards),
            "modified_cards":  len(modified_cards),
            "deleted_cards":   len(deleted_cards),
            "alert_triggered": total_changes > threshold,
        },
        "new_cards":      new_cards,
        "modified_cards": modified_cards,
        "deleted_cards":  deleted_cards,
    }

    REPORT_FILE.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    # Imprimir resumen
    print(f"\n{'='*55}")
    print(f"  Freshness Check — Diccionario de Métricas")
    print(f"{'='*55}")
    print(f"  Días desde último pipeline: {days_since}")
    print(f"  Cards nuevas:      {len(new_cards)}")
    print(f"  Cards modificadas: {len(modified_cards)}")
    print(f"  Cards eliminadas:  {len(deleted_cards)}")
    print(f"  Total cambios:     {total_changes}")

    if total_changes > threshold:
        print(f"\n  ⚠ ALERTA: {total_changes} cambios detectados (threshold={threshold})")
        print("  Considera re-ejecutar: python src/catalog/pipeline.py --skip-structural")

    if not summary_only and total_changes > 0:
        if new_cards:
            print(f"\n  Nuevas ({len(new_cards)}):")
            for c in new_cards[:5]:
                print(f"    #{c['id']} — {c['name']}")
            if len(new_cards) > 5:
                print(f"    ... y {len(new_cards) - 5} más")
        if modified_cards:
            print(f"\n  Modificadas ({len(modified_cards)}):")
            for c in modified_cards[:5]:
                flag = " [nombre cambió]" if c.get("name_changed") else ""
                print(f"    #{c['id']} — {c['name']}{flag}")
            if len(modified_cards) > 5:
                print(f"    ... y {len(modified_cards) - 5} más")
        if deleted_cards:
            print(f"\n  Eliminadas/Archivadas ({len(deleted_cards)}):")
            for c in deleted_cards[:5]:
                print(f"    #{c['id']} — {c['catalog_name']}")
            if len(deleted_cards) > 5:
                print(f"    ... y {len(deleted_cards) - 5} más")

    print(f"\n  Reporte: {REPORT_FILE.relative_to(_ROOT)}")
    print(f"{'='*55}")

    # Alerta Slack
    if send_slack and total_changes > threshold and SLACK_WEBHOOK:
        _send_slack_alert(total_changes, len(new_cards), len(modified_cards), len(deleted_cards), days_since, threshold)

    return report


def _send_slack_alert(total: int, new: int, mod: int, deleted: int, days: int | None, threshold: int) -> None:
    msg = {
        "text": (
            f":warning: *Diccionario de Métricas — Alerta de Freshness*\n"
            f"Se detectaron *{total} cambios* desde la última ejecución del pipeline "
            f"(hace {days or '?'} días, threshold={threshold}).\n"
            f"• Cards nuevas: {new}\n"
            f"• Cards modificadas: {mod}\n"
            f"• Cards eliminadas/archivadas: {deleted}\n\n"
            f"Acción: `python src/catalog/pipeline.py --skip-structural`"
        )
    }
    try:
        resp = requests.post(SLACK_WEBHOOK, json=msg, timeout=10)
        if resp.status_code == 200:
            print("  Alerta Slack enviada.")
        else:
            print(f"  Slack respondió {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"  Error al enviar alerta Slack: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Freshness check del diccionario de métricas")
    parser.add_argument("--summary", action="store_true", help="Solo imprime resumen, sin detalles")
    parser.add_argument("--slack", action="store_true", help="Envía alerta a Slack si supera threshold")
    parser.add_argument("--threshold", type=int, default=10, help="Nro de cambios para activar alerta (default: 10)")
    args = parser.parse_args()
    run(summary_only=args.summary, send_slack=args.slack, threshold=args.threshold)
