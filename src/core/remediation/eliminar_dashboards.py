"""
eliminar_dashboards.py — Gestión de dashboards obsoletos en Metabase
ExampleCorp · author · 2026-04-07

¿Qué hace?
    Lee una lista de IDs de dashboards, obtiene su info vía API de Metabase,
    y los archiva (soft delete) o elimina definitivamente (--eliminar).

    ARCHIVAR  = soft delete (reversible). El dashboard desaparece de la
                vista normal pero se puede recuperar en Admin → Archived.
    ELIMINAR  = permanente. No se puede deshacer.

Grupos predefinidos (basados en revisión CSV de Val + análisis sesión 14):
    --confirmados    IDs 14,32,33,13,20,3,63,52,49,45  (duplicados/testing/sin uso)
    --sin-dueno      IDs 4,12,11,5,10,9,41,39           (sin dueño claro — requieren OK de data_owner)

Uso:
    # Ver exactamente qué se va a tocar SIN hacer nada
    python eliminar_dashboards.py --dry-run --confirmados

    # Archivar los 10 confirmados (soft delete, reversible)
    python eliminar_dashboards.py --confirmados

    # Archivar dashboards sin dueño (requiere OK de data_owner primero)
    python eliminar_dashboards.py --sin-dueno

    # Eliminar permanentemente una lista de IDs
    python eliminar_dashboards.py --eliminar --ids 14,32,33

    # Dry-run con IDs específicos
    python eliminar_dashboards.py --dry-run --ids 14,32,33,13

Prerequisito:
    METABASE_SESSION=<token> en config/api_key.env
"""

import os
import sys
import json
import time
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
from src.utils.metabase_client import METABASE_URL, get_session_token
LOG_FILE      = str(_ROOT / "data" / "processed" / "resultados" / "dashboards_eliminados_log.json")

# ── Grupos predefinidos ────────────────────────────────────────────────────
CONFIRMADOS = [14, 32, 33, 13, 20, 3, 63, 52, 49, 45, 59, 54]
SIN_DUENO   = [4, 12, 11, 5, 10, 9, 41, 39]

RAZONES = {
    14: "Duplicado — copia de prueba sin uso confirmado",
    32: "Testing/staging — nombre contiene indicador de desarrollo",
    33: "Testing/staging — duplicado de dashboard productivo",
    13: "Sin uso — 0 vistas en los últimos 90 días",
    20: "Sin uso — dashboard de experimento archivado manualmente antes",
    3:  "Duplicado — reemplazado por versión más reciente activa",
    63: "Testing — dashboard creado para validar configuración",
    52: "Sin uso — sin actividad registrada, sin dueño identificado",
    49: "Duplicado de dashboard productivo, sin cards activas propias",
    45: "Sin uso — abandonado, sin modificaciones en >180 días",
    59: "BotMaker — Template Bienvenida, marcado TRUE en CSV por data_owner. Poco uso (18 vistas), colección BotMaker.",
    54: "BotMaker — Solicitud de Ayuda, marcado TRUE en CSV por data_owner. Poco uso (22 vistas), colección BotMaker.",
    # Sin dueño claro (requieren OK de data_owner)
    4:  "Sin dueño claro — requiere confirmación de data_owner",
    12: "Sin dueño claro — requiere confirmación de data_owner",
    11: "Sin dueño claro — requiere confirmación de data_owner",
    5:  "Sin dueño claro — requiere confirmación de data_owner",
    10: "Sin dueño claro — requiere confirmación de data_owner",
    9:  "Sin dueño claro — requiere confirmación de data_owner",
    41: "Sin dueño claro — requiere confirmación de data_owner",
    39: "Sin dueño claro — requiere confirmación de data_owner",
}

# ──────────────────────────────────────────────────────────────────────────
load_session_token = get_session_token


def get_headers(token):
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}


def get_user_name(session, headers, user_id):
    """Busca nombre completo de un usuario por su ID.
    Requiere permisos de admin en Metabase. Si no hay acceso, devuelve el ID
    con una nota para buscarlo manualmente en Admin → People."""
    try:
        r = session.get(f"{METABASE_URL}/api/user/{user_id}", headers=headers, timeout=10)
        if r.status_code == 200:
            u = r.json()
            return u.get("common_name") or u.get("email") or f"user_id:{user_id}"
        if r.status_code == 403:
            return f"ID:{user_id} (buscar en Metabase → Admin → People)"
    except Exception:
        pass
    return f"ID:{user_id} (no resuelto)"


def get_dashboard_info(session, headers, dashboard_id):
    """Obtiene info del dashboard: nombre, descripción, creador, cards."""
    url = f"{METABASE_URL}/api/dashboard/{dashboard_id}"
    r = session.get(url, headers=headers, timeout=15)
    if r.status_code == 404:
        return None, "404 — no encontrado (ya fue eliminado o ID incorrecto)"
    if r.status_code == 401:
        return None, "401 — sesión expirada, renueva METABASE_SESSION"
    if r.status_code != 200:
        return None, f"HTTP {r.status_code}"
    d = r.json()

    # Metabase puede devolver el creador como objeto anidado, como campo "created_by",
    # o solo como "creator_id". Probamos en orden.
    creator_name = None
    for field in ("creator", "created_by"):
        obj = d.get(field) or {}
        creator_name = obj.get("common_name") or obj.get("email")
        if creator_name:
            break
    if not creator_name:
        creator_id = d.get("creator_id")
        if creator_id:
            creator_name = get_user_name(session, headers, creator_id)
    creator_name = creator_name or "desconocido"
    return {
        "id":           d.get("id"),
        "name":         d.get("name", "Sin nombre"),
        "description":  d.get("description") or "",
        "creator":      creator_name,
        "cards_count":  len(d.get("dashcards", [])),
        "archived":     d.get("archived", False),
        "collection":   (d.get("collection") or {}).get("name", "Sin colección"),
    }, None


def archive_dashboard(session, headers, dashboard_id):
    """Archiva un dashboard (soft delete, reversible vía Admin → Archived)."""
    url = f"{METABASE_URL}/api/dashboard/{dashboard_id}"
    r = session.put(url, headers=headers, json={"archived": True}, timeout=15)
    return r.status_code, r.text


def delete_dashboard(session, headers, dashboard_id):
    """Elimina un dashboard permanentemente. ¡No se puede deshacer!"""
    url = f"{METABASE_URL}/api/dashboard/{dashboard_id}"
    r = session.delete(url, headers=headers, timeout=15)
    return r.status_code, r.text


# ──────────────────────────────────────────────────────────────────────────
def run(ids, dry_run, eliminar):
    token   = load_session_token()
    headers = get_headers(token)
    session = requests.Session()

    mode = "DRY-RUN" if dry_run else ("ELIMINAR PERMANENTE" if eliminar else "ARCHIVAR")
    print(f"\n{'=' * 65}")
    print(f"  Gestión de dashboards — Metabase · ExampleCorp")
    print(f"  Modo: {mode}")
    print(f"  Dashboards a procesar: {len(ids)}")
    print(f"{'=' * 65}\n")

    results = []
    ok_count = err_count = skip_count = 0

    for dash_id in ids:
        print(f"  🔍 Dashboard ID {dash_id}...")
        info, err = get_dashboard_info(session, headers, dash_id)

        if err:
            print(f"     ❌ Error al obtener info: {err}")
            results.append({"id": dash_id, "status": "error", "detail": err})
            err_count += 1
            continue

        if info["archived"]:
            print(f"     ⚪ Ya está archivado — '{info['name']}'. Skip.")
            results.append({"id": dash_id, "status": "skip_ya_archivado", **info})
            skip_count += 1
            continue

        reason = RAZONES.get(dash_id, "Revisión manual de obsolescencia")
        print(f"     📊 '{info['name']}'")
        print(f"        Colección : {info['collection']}")
        print(f"        Creador   : {info['creator']}")
        print(f"        Cards     : {info['cards_count']}")
        print(f"        Razón     : {reason}")

        if dry_run:
            accion = "Se archivaría (soft delete)" if not eliminar else "Se ELIMINARÍA permanentemente"
            print(f"        [DRY-RUN] {accion}")
            results.append({"id": dash_id, "status": "dry_run", "accion": accion, **info, "razon": reason})
            ok_count += 1
        else:
            if eliminar:
                # Confirmación extra por dashboard
                confirm = input(f"\n     ⚠️  ¿Eliminar PERMANENTEMENTE '{info['name']}'? (escribe 'SI' para confirmar): ")
                if confirm.strip().upper() != "SI":
                    print(f"     ⏭ Cancelado por el usuario.")
                    results.append({"id": dash_id, "status": "cancelado_usuario", **info})
                    skip_count += 1
                    continue
                status_code, _ = delete_dashboard(session, headers, dash_id)
            else:
                status_code, _ = archive_dashboard(session, headers, dash_id)

            if status_code in (200, 204):
                accion = "eliminado permanentemente" if eliminar else "archivado (soft delete)"
                print(f"        ✅ {accion}")
                results.append({"id": dash_id, "status": "ok", "accion": accion, **info, "razon": reason})
                ok_count += 1
            else:
                print(f"        ❌ HTTP {status_code}")
                results.append({"id": dash_id, "status": f"error_http_{status_code}", **info})
                err_count += 1

        time.sleep(0.3)  # evitar rate limit

    # ── Resumen ──────────────────────────────────────────────────────────────
    print(f"\n{'─' * 50}")
    print(f"  Resultado final:")
    print(f"    ✅  OK       : {ok_count}")
    print(f"    ⏭  Skip     : {skip_count}")
    print(f"    ❌  Errores  : {err_count}")
    print(f"{'─' * 50}")

    # ── Log ──────────────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    log_entry = {
        "run_at":   datetime.now(timezone.utc).isoformat(),
        "mode":     mode,
        "dry_run":  dry_run,
        "ids":      ids,
        "ok":       ok_count,
        "skip":     skip_count,
        "errors":   err_count,
        "results":  results,
    }
    existing = []
    if os.path.exists(LOG_FILE):
        try:
            with open(LOG_FILE) as f:
                existing = json.load(f)
        except Exception:
            pass
    existing.append(log_entry)
    with open(LOG_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2, default=str)
    print(f"  📋 Log guardado en: {LOG_FILE}")


# ──────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Gestiona dashboards obsoletos de Metabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  python eliminar_dashboards.py --dry-run --confirmados
  python eliminar_dashboards.py --confirmados
  python eliminar_dashboards.py --dry-run --sin-dueno
  python eliminar_dashboards.py --ids 14,32,33
  python eliminar_dashboards.py --eliminar --ids 14,32
        """,
    )
    parser.add_argument("--dry-run",     action="store_true",
                        help="Mostrar qué se haría SIN ejecutar nada")
    parser.add_argument("--confirmados", action="store_true",
                        help=f"IDs confirmados para eliminar: {CONFIRMADOS}")
    parser.add_argument("--sin-dueno",   action="store_true",
                        help=f"IDs sin dueño claro (requieren OK data_owner): {SIN_DUENO}")
    parser.add_argument("--ids",
                        help="Lista de IDs separados por coma (ej: 14,32,33)")
    parser.add_argument("--eliminar",    action="store_true",
                        help="⚠️  ELIMINAR permanentemente (vs. archivar). Pide confirmación por dashboard.")
    args = parser.parse_args()

    # Construir lista de IDs
    ids = []
    if args.confirmados:
        ids += CONFIRMADOS
    if args.sin_dueno:
        ids += SIN_DUENO
    if args.ids:
        try:
            ids += [int(i.strip()) for i in args.ids.split(",")]
        except ValueError:
            print("❌ --ids debe ser una lista de enteros: ej. 14,32,33")
            sys.exit(1)

    ids = list(dict.fromkeys(ids))  # deduplicar manteniendo orden

    if not ids:
        print("❌ No se especificaron dashboards. Usa --confirmados, --sin-dueno o --ids.")
        parser.print_help()
        sys.exit(1)

    if args.eliminar and not args.dry_run:
        print("\n⚠️  MODO ELIMINAR PERMANENTE ACTIVADO")
        print("   Esta acción NO se puede deshacer.")
        confirm = input("   ¿Continuar? (escribe 'SI' para confirmar): ")
        if confirm.strip().upper() != "SI":
            print("   Cancelado.")
            sys.exit(0)

    run(ids, dry_run=args.dry_run, eliminar=args.eliminar)


if __name__ == "__main__":
    main()
