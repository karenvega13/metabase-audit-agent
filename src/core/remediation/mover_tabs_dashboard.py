"""
mover_tabs_dashboard.py — Copia tabs y cards del Dashboard 16 al Dashboard 77.

Contexto:
    Dashboard 16 "Funnel UNIR Dashboard" → Dashboard 77 "Funnels Validado"
    El D77 ya tiene las tabs creadas (mismo nombre). Solo hay que copiar las cards.

Mecanismo (Metabase v0.54+):
    POST /api/dashboard/16/copy  con is_deep_copy=True
      → crea un dashboard temporal con NUEVOS dashcard IDs y copias de las cards
    PUT /api/dashboard/77 con dashcards existentes + dashcards del copy (tab remapeada)
      → "adopta" los dashcard IDs del copy en D77

Reglas:
    - Tab "[Deprecated] Test" → SALTAR siempre
    - Tab "Lead to 1TU" → SALTAR dashcard cuya card tenga el mismo nombre que
      "new_funnel:: First Transaction & Intents(Adquisitions)" (ya existe en D77 como #732)
    - parameter_mappings se limpian (los filtros de D16 no corresponden a D77)
    - El dashboard temporal se elimina al final

Uso:
    python src/remediation/mover_tabs_dashboard.py --dry-run
    python src/remediation/mover_tabs_dashboard.py --dry-run --tab "ExampleCorp Users"
    python src/remediation/mover_tabs_dashboard.py --tab "ExampleCorp Users"
    python src/remediation/mover_tabs_dashboard.py          # todas las tabs (sin Deprecated)
"""

import sys
import json
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
from src.utils.metabase_client import METABASE_URL, get_session_token, make_headers

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

LOG_FILE = _ROOT / "data" / "processed" / "resultados" / "mover_tabs_log.json"

DASH_SRC = 16
DASH_DST = 77
COPY_NAME = "__TEMP_MIGRATION_COPY__"

# Tab a saltar (por nombre, case-insensitive)
SKIP_TAB_NAMES = {"[deprecated] test"}

# Nombre de card a saltar en tab "Lead to 1TU" (ya existe en D77)
DUPLICATE_CARD_NAME = "new_funnel:: First Transaction & Intents(Adquisitions) "

# Mapeo de parameter_id de D16 → parameter_id de D77 (por slug coincidente)
# Los slugs sin match en D77 se eliminan de parameter_mappings
PARAM_ID_MAP = {
    "c4955e43": "week-param-id",  # week → Week
    "c8cd6b6b": "1316c234",       # start_date → Start_date
    "c37b6700": "edc6c6c0",       # end_date → End_date
    "30f91c41": "88c3f575",       # year → Year
    # verification_group (6612a545) → sin match, se limpia
    # etapa_funnel (201dc1d8)      → sin match, se limpia
}


def remap_parameter_mappings(mappings):
    """Sustituye parameter_ids de D16 por los equivalentes de D77.
    Descarta los que no tienen equivalente en D77."""
    result = []
    for pm in mappings:
        old_pid = pm.get("parameter_id")
        new_pid = PARAM_ID_MAP.get(old_pid)
        if new_pid:
            result.append({**pm, "parameter_id": new_pid})
        # Si no hay match, se descarta (el filtro no se conecta, pero la card funciona)
    return result


# ──────────────────────────────────────────────────────────────────────────
def get_dashboard(session, headers, dash_id):
    r = session.get(f"{METABASE_URL}/api/dashboard/{dash_id}", headers=headers, timeout=20)
    if r.status_code == 401:
        print("ERROR: 401 — sesion expirada. Renueva METABASE_SESSION en config/api_key.env")
        sys.exit(1)
    r.raise_for_status()
    return r.json()


def deep_copy_dashboard(session, headers, src_id, name):
    print(f"  Creando deep copy del Dashboard {src_id}...")
    r = session.post(f"{METABASE_URL}/api/dashboard/{src_id}/copy",
                     headers=headers, json={"is_deep_copy": True, "name": name}, timeout=30)
    if r.status_code != 200:
        print(f"  ERROR al crear deep copy: HTTP {r.status_code} — {r.text[:300]}")
        sys.exit(1)
    copy = r.json()
    print(f"  Deep copy creado: ID={copy['id']}")
    return copy["id"]


def delete_dashboard(session, headers, dash_id):
    r = session.delete(f"{METABASE_URL}/api/dashboard/{dash_id}", headers=headers, timeout=15)
    return r.status_code


def clean_dc(dc, dst_tab_id, remap_params=True):
    """Prepara un dashcard para ser adoptado en el dashboard destino."""
    raw_mappings = dc.get("parameter_mappings", [])
    if remap_params:
        mapped = remap_parameter_mappings(raw_mappings)
    else:
        mapped = []
    return {
        "id": dc["id"],
        "card_id": dc.get("card_id"),
        "row": dc["row"],
        "col": dc["col"],
        "size_x": dc["size_x"],
        "size_y": dc["size_y"],
        "dashboard_tab_id": dst_tab_id,
        "parameter_mappings": mapped,
        "visualization_settings": dc.get("visualization_settings", {}),
    }


def save_dashboard(session, headers, dash_id, dashcards, tabs):
    """Guarda el estado completo de un dashboard (dashcards + tabs)."""
    payload = {
        "dashcards": dashcards,
        "tabs": [{"id": t["id"], "name": t["name"], "position": t.get("position", i)}
                 for i, t in enumerate(tabs)],
    }
    r = session.put(f"{METABASE_URL}/api/dashboard/{dash_id}",
                    headers=headers, json=payload, timeout=30)
    return r


# ──────────────────────────────────────────────────────────────────────────
def run(tab_filter, dry_run):
    token = get_session_token()
    headers = make_headers(token)
    session = requests.Session()

    mode = "DRY-RUN" if dry_run else "REAL"
    print(f"\n{'=' * 65}")
    print(f"  mover_tabs_dashboard.py — Modo: {mode}")
    print(f"{'=' * 65}\n")

    # ── Obtener estado actual de D77 ─────────────────────────────────────
    print(f"Obteniendo Dashboard {DASH_DST} (destino)...")
    d_dst = get_dashboard(session, headers, DASH_DST)
    dst_tabs = d_dst.get("tabs", [])
    dst_dashcards = d_dst.get("dashcards", [])

    # Mapa nombre → tab_id en D77
    dst_tab_by_name = {t["name"].strip().lower(): t for t in dst_tabs}

    # Cards ya existentes en D77 por tab
    dst_cards_by_tab = {}
    for dc in dst_dashcards:
        tid = dc.get("dashboard_tab_id")
        dst_cards_by_tab.setdefault(tid, set()).add(dc.get("card_id"))

    # ── Deep copy de D16 ─────────────────────────────────────────────────
    if not dry_run:
        copy_id = deep_copy_dashboard(session, headers, DASH_SRC, COPY_NAME)
    else:
        copy_id = "DRY"
        print(f"  [DRY-RUN] Se crearía deep copy de Dashboard {DASH_SRC}")

    if not dry_run:
        # Obtener el copy para mapear sus tabs y dashcards
        d_copy = get_dashboard(session, headers, copy_id)
        copy_tabs = d_copy.get("tabs", [])
        copy_dashcards = d_copy.get("dashcards", [])
    else:
        # En dry-run, usar D16 directamente para mostrar qué se haría
        d_src = get_dashboard(session, headers, DASH_SRC)
        copy_tabs = d_src.get("tabs", [])
        copy_dashcards = d_src.get("dashcards", [])

    # Agrupar dashcards del copy por tab
    copy_by_tab = {}
    for dc in copy_dashcards:
        copy_by_tab.setdefault(dc.get("dashboard_tab_id"), []).append(dc)

    # ── Construir lista de dashcards a adoptar en D77 ────────────────────
    new_dashcards_for_d77 = []
    log_tabs = []
    total_to_copy = 0
    total_skipped = 0

    for tab in sorted(copy_tabs, key=lambda t: t.get("position", 0)):
        tab_name = tab["name"]

        # Saltar tabs excluidas
        if tab_name.strip().lower() in SKIP_TAB_NAMES:
            print(f"  [SKIP] Tab \"{tab_name}\" — excluida por regla")
            continue

        # Filtro --tab
        if tab_filter and tab_name.strip().lower() != tab_filter.strip().lower():
            continue

        # Buscar tab equivalente en D77
        dst_tab_info = dst_tab_by_name.get(tab_name.strip().lower())
        if dst_tab_info is None:
            print(f"  [WARN] Tab \"{tab_name}\" no encontrada en D77 — saltando")
            continue

        dst_tab_id = dst_tab_info["id"]
        dst_existing_cards = dst_cards_by_tab.get(dst_tab_id, set())

        dashcards_in_tab = copy_by_tab.get(tab["id"], [])
        print(f"\n  Tab \"{tab_name}\" (copy:{tab['id']} -> D77:{dst_tab_id})")

        tab_log = {"tab_name": tab_name, "copy_tab_id": tab["id"],
                   "dst_tab_id": dst_tab_id, "results": []}

        for dc in dashcards_in_tab:
            card_id = dc.get("card_id")
            card_name = (dc.get("card") or {}).get("name", "[texto/heading]")

            # Saltar duplicado en Lead to 1TU
            if (tab_name.strip().lower() == "lead to 1tu" and
                    card_name.strip() == DUPLICATE_CARD_NAME.strip()):
                print(f"    [SKIP] \"{card_name[:60]}\" — duplicado de #732 ya en D77")
                total_skipped += 1
                tab_log["results"].append({"card_id": card_id, "status": "skipped_duplicate",
                                           "name": card_name})
                continue

            label = f"#{card_id}" if card_id else "[texto]"
            if dry_run:
                print(f"    [DRY]  {label} \"{card_name[:60]}\"")
            else:
                print(f"    [+]    {label} \"{card_name[:60]}\"")

            new_dashcards_for_d77.append(clean_dc(dc, dst_tab_id))
            total_to_copy += 1
            tab_log["results"].append({"card_id": card_id, "status": "dry_run" if dry_run else "queued",
                                       "name": card_name})

        log_tabs.append(tab_log)

    # ── Aplicar en D77 (modo real) ────────────────────────────────────────
    if not dry_run and new_dashcards_for_d77:
        print(f"\n  Aplicando {len(new_dashcards_for_d77)} dashcards en Dashboard {DASH_DST}...")

        # ── Convertir Dashboard Questions a Regular Questions ─────────────
        # Cards copiadas de D16 pueden tener dashboard_id=copy_id (Dashboard Questions).
        # Metabase rechaza adoptar ese tipo de card en otro dashboard.
        # Solución: PUT /api/card/{id} {"dashboard_id": null} antes de adoptar.
        print(f"  Verificando dashboard_id de {len(new_dashcards_for_d77)} cards copiadas...")
        converted = 0
        for dc in new_dashcards_for_d77:
            card_id = dc.get("card_id")
            if card_id is None:
                continue
            rc = session.get(f"{METABASE_URL}/api/card/{card_id}", headers=headers, timeout=15)
            if rc.status_code != 200:
                continue
            card_data = rc.json()
            if card_data.get("dashboard_id") is not None:
                rp = session.put(f"{METABASE_URL}/api/card/{card_id}",
                                 headers=headers, json={"dashboard_id": None}, timeout=15)
                if rp.status_code == 200:
                    converted += 1
                else:
                    print(f"    WARN: no se pudo convertir card {card_id}: HTTP {rp.status_code}")
        if converted:
            print(f"  Convertidas {converted} Dashboard Questions → Regular Questions")

        # Estado actual de D77 (re-fetch para tener IDs frescos)
        d_dst_fresh = get_dashboard(session, headers, DASH_DST)
        existing_clean = [clean_dc(dc, dc["dashboard_tab_id"], remap_params=False)
                          for dc in d_dst_fresh.get("dashcards", [])]

        r = save_dashboard(session, headers, DASH_DST,
                           existing_clean + new_dashcards_for_d77,
                           d_dst_fresh.get("tabs", []))

        if r.status_code == 200:
            resp_dcs = r.json().get("dashcards", [])
            print(f"  OK — D77 ahora tiene {len(resp_dcs)} dashcards en total")
            for tab_log in log_tabs:
                for entry in tab_log["results"]:
                    if entry["status"] == "queued":
                        entry["status"] = "ok"
        else:
            print(f"  ERROR HTTP {r.status_code}: {r.text[:300]}")
            for tab_log in log_tabs:
                for entry in tab_log["results"]:
                    if entry["status"] == "queued":
                        entry["status"] = "error"

        # Limpiar el dashboard temporal
        print(f"\n  Eliminando dashboard temporal ID={copy_id}...")
        status = delete_dashboard(session, headers, copy_id)
        print(f"  DELETE /api/dashboard/{copy_id} → HTTP {status}")

    elif dry_run:
        print(f"\n  [DRY-RUN] Se adoptarian {total_to_copy} dashcards en D77")
        print(f"  [DRY-RUN] Se crearia y eliminaria el copy temporal de D16")

    # ── Resumen ──────────────────────────────────────────────────────────
    print(f"\n{'─' * 50}")
    action_word = "Se copiarian" if dry_run else "Copiados"
    print(f"  {action_word} : {total_to_copy}")
    print(f"  Saltados  : {total_skipped}")
    print(f"{'─' * 50}")

    if dry_run:
        print("\n  DRY-RUN OK — para ejecutar real quita el flag --dry-run")

    # ── Log ──────────────────────────────────────────────────────────────
    log_entry = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "tab_filter": tab_filter,
        "copy_id": copy_id,
        "copied": total_to_copy,
        "skipped": total_skipped,
        "tabs": log_tabs,
    }
    existing_log = []
    if LOG_FILE.exists():
        try:
            existing_log = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    existing_log.append(log_entry)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps(existing_log, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n  Log guardado en: {LOG_FILE.relative_to(_ROOT)}")


# ──────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Copia tabs/cards de Dashboard 16 a Dashboard 77")
    parser.add_argument("--dry-run", action="store_true",
                        help="Muestra que se copiaria sin ejecutar nada")
    parser.add_argument("--tab", metavar="NOMBRE",
                        help="Procesa solo esta tab (ej: 'ExampleCorp Users')")
    args = parser.parse_args()

    if not args.dry_run:
        print("\nATENCION: modo REAL activo. Se modificara el Dashboard 77.")
        confirm = input("Escribe 'SI' para continuar: ")
        if confirm.strip().upper() != "SI":
            print("Cancelado.")
            sys.exit(0)

    run(tab_filter=args.tab, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
