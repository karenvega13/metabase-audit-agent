"""
inspect_dashboards.py — Verifica el estado de los dashboards 16 y 77 antes de migrar.

Uso:
    python -m src.core.remediation.inspect_dashboards

Qué hace (solo lectura):
    - GET /api/dashboard/16 → tabs + cards del Funnel Unir Dashboard
    - GET /api/dashboard/77 → tabs existentes en Funnels Testing
    - GET /api/user/current → nivel de acceso del token
    Guarda reporte en data/processed/resultados/dashboard_inspect_report.json
"""

import sys
import json
import requests
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
from src.utils.metabase_client import METABASE_URL, get_session_token, make_headers

REPORT_FILE = _ROOT / "data" / "processed" / "resultados" / "dashboard_inspect_report.json"
DASH_16 = 16
DASH_77 = 77


def get_dashboard(session, headers, dash_id):
    r = session.get(f"{METABASE_URL}/api/dashboard/{dash_id}", headers=headers, timeout=20)
    if r.status_code == 401:
        print(f"  ❌ 401 — sesión expirada. Renueva METABASE_SESSION en config/api_key.env")
        sys.exit(1)
    if r.status_code == 403:
        print(f"  ❌ 403 — sin acceso al dashboard {dash_id}")
        return None
    if r.status_code == 404:
        print(f"  ❌ 404 — dashboard {dash_id} no existe")
        return None
    r.raise_for_status()
    return r.json()


def get_current_user(session, headers):
    r = session.get(f"{METABASE_URL}/api/user/current", headers=headers, timeout=10)
    if r.status_code != 200:
        return None
    return r.json()


def check_write_permission(session, headers, dash_id):
    """Intenta un GET y revisa el campo can_write en la respuesta del dashboard."""
    r = session.get(f"{METABASE_URL}/api/dashboard/{dash_id}", headers=headers, timeout=15)
    if r.status_code != 200:
        return False
    d = r.json()
    # Metabase expone can_write en la respuesta del dashboard
    return d.get("can_write", False)


def analyze_dashboard(d):
    """Extrae tabs y conteo de cards por tab."""
    tabs = d.get("tabs", [])
    dashcards = d.get("dashcards", [])

    # Agrupar dashcards por tab
    cards_by_tab = defaultdict(list)
    for dc in dashcards:
        tab_id = dc.get("dashboard_tab_id")
        cards_by_tab[tab_id].append(dc)

    tab_info = []
    for tab in tabs:
        tid = tab["id"]
        tab_cards = cards_by_tab.get(tid, [])
        real_cards = [dc for dc in tab_cards if dc.get("card_id") is not None]
        tab_info.append({
            "id": tid,
            "name": tab.get("name", "Sin nombre"),
            "position": tab.get("position", 0),
            "dashcards_total": len(tab_cards),
            "cards_with_card_id": len(real_cards),
        })

    # También dashcards sin tab (tab_id = None → sin tabs en el dashboard)
    no_tab = cards_by_tab.get(None, [])

    return {
        "id": d.get("id"),
        "name": d.get("name", "Sin nombre"),
        "description": d.get("description") or "",
        "archived": d.get("archived", False),
        "can_write": d.get("can_write", False),
        "collection": (d.get("collection") or {}).get("name", "Sin colección"),
        "tabs": tab_info,
        "tabs_count": len(tabs),
        "dashcards_total": len(dashcards),
        "dashcards_no_tab": len(no_tab),
        "parameters": [p.get("name") for p in d.get("parameters", [])],
    }


def print_dashboard_summary(info, protected_tab_ids=None):
    protected_tab_ids = protected_tab_ids or []
    print(f"\n  Dashboard {info['id']} — {info['name']}")
    print(f"  Colección : {info['collection']}")
    print(f"  Archivado : {info['archived']}")
    print(f"  can_write : {info['can_write']}")
    print(f"  Parámetros: {', '.join(info['parameters']) or '(ninguno)'}")
    print(f"  Tabs ({info['tabs_count']}):")
    for tab in sorted(info["tabs"], key=lambda t: t["position"]):
        protected = " ← PROTEGIDA" if tab["id"] in protected_tab_ids else ""
        print(f"    [{tab['id']}] \"{tab['name']}\"  —  "
              f"{tab['cards_with_card_id']} cards reales / "
              f"{tab['dashcards_total']} dashcards totales{protected}")
    if info["dashcards_no_tab"] > 0:
        print(f"    (Sin tab asignada: {info['dashcards_no_tab']} dashcards)")
    print(f"  TOTAL dashcards: {info['dashcards_total']}")


def main():
    print("=" * 60)
    print("  inspect_dashboards.py — Solo lectura")
    print("=" * 60)

    token = get_session_token()
    headers = make_headers(token)
    session = requests.Session()

    # ── Usuario actual ──────────────────────────────────────────
    print("\n▶ Verificando usuario actual...")
    user = get_current_user(session, headers)
    if user:
        print(f"  Usuario : {user.get('common_name') or user.get('email')}")
        print(f"  Email   : {user.get('email')}")
        print(f"  Is admin: {user.get('is_superuser', False)}")
    else:
        print("  ⚠️  No se pudo obtener info de usuario")

    # ── Dashboard 16 ────────────────────────────────────────────
    print(f"\n▶ Obteniendo Dashboard {DASH_16}...")
    d16_raw = get_dashboard(session, headers, DASH_16)
    if d16_raw is None:
        print("  ❌ No se pudo obtener el dashboard 16. Abortando.")
        sys.exit(1)
    d16 = analyze_dashboard(d16_raw)
    print_dashboard_summary(d16)

    # ── Dashboard 77 ────────────────────────────────────────────
    print(f"\n▶ Obteniendo Dashboard {DASH_77}...")
    d77_raw = get_dashboard(session, headers, DASH_77)
    if d77_raw is None:
        print("  ❌ No se pudo obtener el dashboard 77. Abortando.")
        sys.exit(1)
    d77 = analyze_dashboard(d77_raw)
    # Tabs existentes en 77 = protegidas (no tocar)
    protected = [t["id"] for t in d77["tabs"]]
    print_dashboard_summary(d77, protected_tab_ids=protected)

    # ── Análisis de colisiones de nombres ───────────────────────
    names_77 = {t["name"].strip().lower() for t in d77["tabs"]}
    collisions = [t for t in d16["tabs"] if t["name"].strip().lower() in names_77]
    new_tabs = [t for t in d16["tabs"] if t["name"].strip().lower() not in names_77]

    print(f"\n▶ Análisis de migración:")
    print(f"  Tabs del 16 que YA existen en 77 (se saltarán): {len(collisions)}")
    for t in collisions:
        print(f"    - [{t['id']}] \"{t['name']}\"")
    print(f"  Tabs del 16 a CREAR en 77: {len(new_tabs)}")
    for t in new_tabs:
        print(f"    + [{t['id']}] \"{t['name']}\"  ({t['cards_with_card_id']} cards)")

    total_cards_to_move = sum(t["cards_with_card_id"] for t in new_tabs)
    print(f"  Cards totales a mover: {total_cards_to_move}")

    if not d77["can_write"]:
        print(f"\n  ⚠️  can_write=False en dashboard 77 — no podremos crear tabs/cards")
    else:
        print(f"\n  ✅ can_write=True en dashboard 77 — listo para migrar")

    # ── Guardar reporte ─────────────────────────────────────────
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "user": {
            "email": (user or {}).get("email"),
            "is_superuser": (user or {}).get("is_superuser", False),
        },
        "dashboard_16": d16,
        "dashboard_77": d77,
        "migration_plan": {
            "tabs_to_create": new_tabs,
            "tabs_collision_skip": collisions,
            "total_cards_to_move": total_cards_to_move,
            "can_write_77": d77["can_write"],
        },
    }

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)

    print(f"\n  📋 Reporte guardado en: {REPORT_FILE.relative_to(_ROOT)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
