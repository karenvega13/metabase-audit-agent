"""fix_tanda3_5.py — Fixes SQL accionables identificados en la auditoría Definición-vs-SQL (tandas 3-5).

Aplica 7 fixes individualizados (no patrones repetidos) a cards específicas:
  #131, #173, #174 — `'Payable,Paid'` como string único en filtro IN
  #169, #174       — typo `ORinjected_date` (sin espacio) que rompe filtro {{day}}
  #172, #174       — paréntesis desbalanceado en `[[AND col::date BETWEEN ... )]]`
  #165             — precedencia OR/AND sin paréntesis en cláusula WHERE
  #59              — denominador per ground truth data_owner 08/04/26 (agregar Hold + 4 Chargebacks)

#132 (typo `tinjected_date`) NO se incluye aquí — ya está en `fix_masivo.py --solo-s59` pendiente OK data_owner.

Uso:
  python -m src.core.remediation.fix_tanda3_5 --dry-run
  python -m src.core.remediation.fix_tanda3_5
"""

import argparse
import io
import json
import re
import time
import sys
from datetime import datetime
from pathlib import Path

import requests

from src.utils.metabase_client import METABASE_URL, get_session_token, make_headers

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() != "utf-8":
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parents[3]
BACKUP_DIR = ROOT / "data" / "processed" / "resultados" / "backups"
LOG_FILE = ROOT / "data" / "processed" / "resultados" / "fix_tanda3_5_log.json"


# ─────────────────────────────────────────────
# FIX FUNCTIONS
# ─────────────────────────────────────────────

def fix_payable_paid_string(sql):
    """`'Payable,Paid'` → `'Payable','Paid'`"""
    return re.sub(r"'(Payable),(Paid)'", r"'\1','\2'", sql, flags=re.IGNORECASE)


def fix_or_injected_typo(sql):
    """`ORinjected_date` (sin espacio) → `OR injected_date`"""
    return re.sub(r"\bORinjected_date\b", "OR injected_date", sql)


def fix_between_paren(sql):
    """`[[AND injected_date::date BETWEEN {{Start_date}} and {{End_date}})]]` → agregar `(` después de AND."""
    return re.sub(
        r"\[\[AND\s+(\w+(?:\.\w+)?)::date\s+BETWEEN\s+\{\{Start_date\}\}\s+and\s+\{\{End_date\}\}\)\]\]",
        r"[[AND (\1::date BETWEEN {{Start_date}} and {{End_date}})]]",
        sql, flags=re.IGNORECASE
    )


def fix_174_combined(sql):
    """Card #174 acumula 3 bugs."""
    sql = fix_payable_paid_string(sql)
    sql = fix_or_injected_typo(sql)
    sql = fix_between_paren(sql)
    return sql


def fix_165_or_paren(sql):
    """Card #165: envolver cláusula OR en paréntesis para fijar precedencia con AND siguiente.

    Patrón actual (sin paréntesis):
      "...".user_label" = 'new user authorized' or "...".user_label" = 'recurring user'
       AND "...".counter" = 1
    """
    # Match the specific 2-line OR clause + AND counter=1 follow-up
    pattern = re.compile(
        r'("public"\."transaction_details"\."user_label"\s*=\s*\'new user authorized\'\s+or\s+'
        r'"public"\."transaction_details"\."user_label"\s*=\s*\'recurring user\')'
        r'(\s*\n\s*AND\s+"public"\."transaction_details"\."counter"\s*=\s*1)',
        re.IGNORECASE
    )
    return pattern.sub(r'(\1)\2', sql)


def fix_59_denominator(sql):
    """Card #59: denominador `('Sent','Payable','Paid','Cancelled','Client_refund')` → agregar Hold + 4 Chargebacks (data_owner 08/04/26).

    Identifica el filtro del denominador del NULLIF (la segunda ocurrencia de FILTER), que tiene 5 statuses,
    y lo expande a 10 (los 5 originales + Hold + 4 Chargeback).
    """
    return re.sub(
        r"transaction_status\s+in\s*\(\s*'Sent'\s*,\s*'Payable'\s*,\s*'Paid'\s*,\s*'Cancelled'\s*,\s*'Client_refund'\s*\)",
        "transaction_status in ('Sent','Payable','Paid','Cancelled','Client_refund','Hold','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir')",
        sql, flags=re.IGNORECASE
    )


# ─────────────────────────────────────────────
# CARDS LIST
# ─────────────────────────────────────────────

FIXES = [
    (131, "Gross Profit margin — 'Payable,Paid' string único en FILTER", fix_payable_paid_string),
    (173, "Operating Profit margin first transaction — 'Payable,Paid' string único", fix_payable_paid_string),
    (174, "Operating Profit margin without first trx — TRIPLE: 'Payable,Paid' + ORinjected + paréntesis BETWEEN", fix_174_combined),
    (169, "% fx revenue first transaction — typo ORinjected_date rompe filtro {{day}}", fix_or_injected_typo),
    (172, "Gross Standing volume without first trx — paréntesis BETWEEN desbalanceado", fix_between_paren),
    (165, "profitability by bands and users in First Transaction — precedencia OR/AND sin paréntesis", fix_165_or_paren),
    (59,  "% Cancelled/authorized — denominador agregar Hold + 4 Chargebacks (data_owner 08/04/26)", fix_59_denominator),
]


# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def get_card(card_id, headers):
    resp = requests.get(f"{METABASE_URL}/api/card/{card_id}", headers=headers)
    resp.raise_for_status()
    return resp.json()


def update_card(card_id, payload, headers):
    resp = requests.put(f"{METABASE_URL}/api/card/{card_id}", headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()


def extract_sql(card_data):
    try:
        return card_data["dataset_query"]["native"]["query"]
    except (KeyError, TypeError):
        return None


def set_sql(card_data, new_sql):
    dq = card_data["dataset_query"].copy()
    dq["native"] = dq["native"].copy()
    dq["native"]["query"] = new_sql
    return {"dataset_query": dq}


def backup_card(card_id, card_data):
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    path = BACKUP_DIR / f"card_{card_id}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(card_data, indent=2, ensure_ascii=False), encoding="utf-8")
    return str(path)


def show_full_diff(orig, fixed):
    """Imprime TODAS las líneas que cambian (no solo la primera)."""
    orig_lines = orig.split('\n')
    fixed_lines = fixed.split('\n')
    n = max(len(orig_lines), len(fixed_lines))
    shown = 0
    for i in range(n):
        ol = orig_lines[i] if i < len(orig_lines) else ''
        fl = fixed_lines[i] if i < len(fixed_lines) else ''
        if ol != fl:
            shown += 1
            print(f"      L{i+1:3d} -  {ol.strip()[:140]}")
            print(f"      L{i+1:3d} +  {fl.strip()[:140]}")
    if shown == 0:
        print("      (sin diferencias visibles línea-a-línea — posible cambio multi-línea)")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(dry_run: bool, only_card: int | None = None):
    token = get_session_token()
    headers = make_headers(token)
    log = []

    mode = "DRY-RUN" if dry_run else "EJECUCIÓN REAL"
    print(f"\n{'='*70}")
    print(f"  fix_tanda3_5.py — {mode}")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*70}")

    if not dry_run:
        print("\n⚠️  ADVERTENCIA: Esto modificará 7 cards en Metabase de producción.")
        print("   Backups en data/processed/resultados/backups/")
        ans = input("¿Continuar? (escribe 'si' para confirmar): ")
        if ans.strip().lower() != 'si':
            print("Cancelado.")
            return 1

    for card_id, label, fix_fn in FIXES:
        if only_card and card_id != only_card:
            continue
        print(f"\n  [{card_id}] {label}")
        try:
            card = get_card(card_id, headers)
            sql_orig = extract_sql(card)
            if sql_orig is None:
                print("    ⚠️  No es card nativa. Skip.")
                log.append({"card_id": card_id, "status": "skip_no_sql"})
                continue

            sql_fixed = fix_fn(sql_orig)
            if sql_fixed == sql_orig:
                print("    ⚠️  Patrón no encontrado en el SQL actual. Skip (puede que ya esté arreglado o el SQL haya cambiado).")
                log.append({"card_id": card_id, "status": "skip_pattern_not_found"})
                continue

            print("    Diff:")
            show_full_diff(sql_orig, sql_fixed)

            if dry_run:
                print("    [DRY-RUN] No se aplicó el cambio.")
                log.append({"card_id": card_id, "status": "dry_run_ok"})
                continue

            backup_path = backup_card(card_id, card)
            print(f"    Backup: {backup_path}")
            payload = set_sql(card, sql_fixed)
            update_card(card_id, payload, headers)
            print("    ✅ Actualizado en Metabase.")
            log.append({"card_id": card_id, "status": "ok", "backup": backup_path})
            time.sleep(0.4)

        except requests.HTTPError as e:
            print(f"    ❌ HTTP {e.response.status_code}: {e.response.text[:200]}")
            log.append({"card_id": card_id, "status": "error", "msg": str(e)})
        except Exception as e:
            print(f"    ❌ Error: {e}")
            log.append({"card_id": card_id, "status": "error", "msg": str(e)})

    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOG_FILE.write_text(json.dumps({
        "ts": datetime.now().isoformat(),
        "mode": mode,
        "results": log,
    }, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"\n{'='*70}")
    counts = {"ok": 0, "dry_run_ok": 0, "skip_no_sql": 0, "skip_pattern_not_found": 0, "error": 0}
    for r in log:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    for k, v in counts.items():
        if v:
            print(f"  {k}: {v}")
    print(f"  Log: {LOG_FILE}")
    print(f"{'='*70}\n")
    return 0


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="No aplica cambios, solo muestra diff.")
    ap.add_argument("--card", type=int, default=None, help="Aplicar solo a esta card_id.")
    args = ap.parse_args()
    sys.exit(run(dry_run=args.dry_run, only_card=args.card))


if __name__ == "__main__":
    main()
