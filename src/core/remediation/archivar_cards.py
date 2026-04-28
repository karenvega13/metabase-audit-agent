"""
Archivador Masivo de Cards — Metabase
ExampleCorp · author · 2026-03-28

¿Qué hace?
    Lee el CSV de obsolescencia (obsolescencia_cards.csv) y archiva en Metabase
    las cards que caigan en las categorías seleccionadas.

    ARCHIVAR = soft delete (reversible). La card desaparece de búsquedas pero
    se puede recuperar en Metabase → Admin → Archived.

    DELETE = permanente. No se puede deshacer.

Uso:
    # Ver qué se va a archivar SIN hacer nada
    python archivar_cards.py --dry-run

    # Archivar solo las que nunca fueron usadas y no están en dashboards
    python archivar_cards.py --categoria NUNCA_USADA --no-dashboards

    # Archivar inactivas >12 meses que no están en dashboards
    python archivar_cards.py --categoria INACTIVA_12M --no-dashboards
    python archivar_cards.py --categoria INACTIVA_18M+ --no-dashboards

    # Archivar una card específica por ID
    python archivar_cards.py --card 451

    # Archivar una lista de IDs (separados por coma)
    python archivar_cards.py --ids 451,520,437,199

    # ⚠️ Modo ELIMINAR permanente (pide confirmación extra)
    python archivar_cards.py --eliminar --ids 451,520,437

Prerequisito:
    METABASE_SESSION=<token> en config/api_key.env
    obsolescencia_cards.csv generado por analizar_obsolescencia.py
"""

import os
import sys
import csv
import time
import json
import argparse
import requests
from pathlib import Path
from datetime import datetime, timezone

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
from src.utils.metabase_client import METABASE_URL, get_session_token
CSV_FILE     = str(_ROOT / "data" / "raw" / "obsolescencia_cards.csv")
LOG_FILE     = str(_ROOT / "data" / "processed" / "resultados" / "archivado_log.json")

CATEGORIAS_VALIDAS = {"NUNCA_USADA", "INACTIVA_18M+", "INACTIVA_12M", "INACTIVA_6M"}

# ──────────────────────────────────────────────────────────────────────────────
load_session_token = get_session_token

def get_headers(token):
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}

def load_csv():
    if not os.path.exists(CSV_FILE):
        print(f"❌ No se encontró {CSV_FILE}")
        print("   Corre primero: python analizar_obsolescencia.py")
        sys.exit(1)
    rows = []
    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows

def archive_card(token, card_id, dry_run=True):
    """Archiva una card (soft delete). Retorna (success, msg)."""
    headers = get_headers(token)
    url     = f"{METABASE_URL}/api/card/{card_id}"
    if dry_run:
        return True, "DRY-RUN — no se envió request"
    try:
        resp = requests.put(url, headers=headers,
                            json={"archived": True}, timeout=30)
        if resp.status_code in (200, 204):
            return True, "Archivada OK"
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
    except Exception as e:
        return False, str(e)

def delete_card(token, card_id, dry_run=True):
    """Elimina permanentemente una card. IRREVERSIBLE."""
    headers = get_headers(token)
    url     = f"{METABASE_URL}/api/card/{card_id}"
    if dry_run:
        return True, "DRY-RUN — no se envió DELETE"
    try:
        resp = requests.delete(url, headers=headers, timeout=30)
        if resp.status_code in (200, 204):
            return True, "Eliminada OK"
        else:
            return False, f"HTTP {resp.status_code}: {resp.text[:100]}"
    except Exception as e:
        return False, str(e)

def save_log(log_entries):
    os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = LOG_FILE.replace(".json", f"_{ts}.json")
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(log_entries, f, indent=2, ensure_ascii=False)
    print(f"\n📋 Log guardado en: {log_path}")
    return log_path

# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Archiva/elimina cards obsoletas en Metabase")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--categoria", choices=list(CATEGORIAS_VALIDAS),
                       help="Archivar todas las cards de esta categoría de uso")
    group.add_argument("--ids", help="IDs de cards separados por coma (ej: 451,520,437)")
    group.add_argument("--card", type=int, help="ID de una card específica")

    parser.add_argument("--no-dashboards", action="store_true",
                        help="Excluir cards que están en dashboards")
    parser.add_argument("--accion", choices=["ARCHIVAR","ELIMINAR","REVISAR_ELIMINAR"],
                        help="Filtrar por acción recomendada")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simular sin hacer cambios reales")
    parser.add_argument("--eliminar", action="store_true",
                        help="⚠️ Eliminar PERMANENTEMENTE (sin esto, solo archiva)")

    args = parser.parse_args()

    if not any([args.categoria, args.ids, args.card]):
        parser.print_help()
        print("\n💡 Ejemplo: python archivar_cards.py --categoria NUNCA_USADA --no-dashboards --dry-run")
        sys.exit(0)

    # Confirmación extra para eliminación permanente
    if args.eliminar and not args.dry_run:
        print("\n⚠️⚠️⚠️  ATENCIÓN: Estás a punto de ELIMINAR cards PERMANENTEMENTE.")
        print("    Esto NO se puede deshacer. Asegúrate de tener la aprobación de data_owner.")
        confirm = input("\n   Escribe 'ELIMINAR' en mayúsculas para confirmar: ").strip()
        if confirm != "ELIMINAR":
            print("❌ Cancelado.")
            sys.exit(0)

    token = load_session_token()
    all_rows = load_csv()

    # Filtrar por criterios
    if args.card:
        target = [r for r in all_rows if r["card_id"] == str(args.card)]
    elif args.ids:
        id_set = set(i.strip() for i in args.ids.split(","))
        target = [r for r in all_rows if r["card_id"] in id_set]
    else:
        target = [r for r in all_rows if r["uso_categoria"] == args.categoria]

    if args.no_dashboards:
        antes = len(target)
        target = [r for r in target if int(r.get("en_dashboards") or 0) == 0]
        print(f"   → {antes - len(target)} cards excluidas por estar en dashboards")

    if args.accion:
        target = [r for r in target if r.get("accion") == args.accion]

    if not target:
        print("⚠️  No hay cards que coincidan con los criterios.")
        sys.exit(0)

    print(f"\n{'='*55}")
    mode = "ELIMINAR PERMANENTEMENTE" if args.eliminar else "ARCHIVAR"
    dr   = " [DRY-RUN]" if args.dry_run else ""
    print(f"  {mode}{dr} — {len(target)} cards")
    print(f"{'='*55}")

    # Mostrar preview
    for r in target[:15]:
        dash_info = f" (en {r['en_dashboards']} dash)" if int(r.get("en_dashboards",0)) > 0 else ""
        print(f"  [{r['card_id']:>5}] {r['nombre'][:45]:<45}  {r['uso_categoria']:<18}  {r['dias_inactiva'] or 'nunca'} días{dash_info}")
    if len(target) > 15:
        print(f"  ... y {len(target)-15} cards más")

    if not args.dry_run:
        print(f"\n{'='*55}")
        confirm = input(f"  ¿Confirmas {mode} de {len(target)} cards? (s/n): ").strip().lower()
        if confirm not in ("s", "si", "sí", "y", "yes"):
            print("❌ Cancelado.")
            sys.exit(0)

    # Ejecutar
    print(f"\n{'='*55}")
    ok_count  = 0
    err_count = 0
    log_entries = []

    for i, r in enumerate(target):
        cid = r["card_id"]
        if args.eliminar:
            success, msg = delete_card(token, cid, dry_run=args.dry_run)
        else:
            success, msg = archive_card(token, cid, dry_run=args.dry_run)

        status = "✅" if success else "❌"
        print(f"  {status} [{cid:>5}] {r['nombre'][:40]:<40}  {msg}")

        log_entries.append({
            "card_id":   cid,
            "nombre":    r["nombre"],
            "accion":    "DELETE" if args.eliminar else "ARCHIVE",
            "success":   success,
            "msg":       msg,
            "dry_run":   args.dry_run,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        if success:
            ok_count += 1
        else:
            err_count += 1

        # Rate limiting
        if not args.dry_run and i % 10 == 9:
            time.sleep(0.5)

    print(f"\n{'='*55}")
    print(f"  ✅ Exitosas: {ok_count}")
    if err_count:
        print(f"  ❌ Errores:  {err_count}")
    if args.dry_run:
        print(f"  🔵 Modo dry-run — ningún cambio real aplicado")
    print(f"{'='*55}")

    save_log(log_entries)

if __name__ == "__main__":
    main()
