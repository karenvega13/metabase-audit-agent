"""
validate_coherence.py
Audita la coherencia de los ~1,863 hallazgos decididos contra las reglas de
negocio del ground truth (INICIO.md).

NO modifica ningún archivo. Solo lee y reporta.

Tipos de conflicto detectados:
  CE_PERO_EXEMPT    → marcado confirmed_error pero la card es exenta
                      (ATV, MAU/QAU/WAU/DAU, NPS → "solo Paid" es correcto)
  CE_PERO_COUNTER   → confirmed_error + descripción menciona counter IN (0,1)
                      (workaround intencional documentado)
  CE_PERO_MTO       → confirmed_error + descripción menciona LEFT JOIN + mto
                      (deuda técnica sistémica, no crítico)
  CE_PERO_FUNNEL    → confirmed_error + card es de funnel o menciona
                      transaction_details_funnel con Sent/Payment/Paymentlink
  FP_POSIBLE_CE     → false_positive de alta visibilidad (>500 vistas) con
                      descripción que menciona filtro incompleto en métrica
                      no exenta (requiere revisión manual, no se auto-corrige)

Uso:
    python validate_coherence.py             # resumen + reporte JSON
    python validate_coherence.py --verbose   # muestra cada conflicto en consola
    python validate_coherence.py --solo-ce   # solo conflictos CE_PERO_*
    python validate_coherence.py --stats     # solo estadísticas, sin detalle
"""

import sys
import io
import json
import argparse
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime

# Forzar UTF-8 en stdout (Windows cp1252 falla con caracteres especiales)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIG
# ============================================================

_ROOT       = Path(__file__).resolve().parent.parent.parent.parent
RESULTS_DIR = _ROOT / "data" / "processed" / "resultados"
REPORT_PATH = RESULTS_DIR / "coherence_report.json"

# Tipos de conflicto
CE_PERO_EXEMPT  = "CE_PERO_EXEMPT"
CE_PERO_COUNTER = "CE_PERO_COUNTER"
CE_PERO_MTO     = "CE_PERO_MTO"
CE_PERO_FUNNEL  = "CE_PERO_FUNNEL"
FP_POSIBLE_CE   = "FP_POSIBLE_CE"

CONFLICT_DESCRIPTIONS = {
    CE_PERO_EXEMPT:  "Marcado CE pero la card es exenta (ATV/MAU/QAU/WAU/DAU/NPS — solo Paid es correcto)",
    CE_PERO_COUNTER: "Marcado CE + descripción menciona counter IN (0,1) — workaround intencional",
    CE_PERO_MTO:     "Marcado CE + descripción menciona LEFT JOIN mto — deuda técnica sistémica",
    CE_PERO_FUNNEL:  "Marcado CE + card de funnel con Sent/Payment/Paymentlink — etapas válidas del journey",
    FP_POSIBLE_CE:   "Marcado FP pero alta visibilidad + descripción sugiere filtro incompleto (revisar)",
}

# ============================================================
# REGLAS DE CARDS EXENTAS (ground truth INICIO.md)
# ============================================================

# Patrones en el NOMBRE de la card que la hacen exenta
# (para estas métricas "solo Paid" es la definición correcta)
EXEMPT_NAME_PATTERNS = [
    r"\bATV\b",
    r"\bMAU\b",
    r"\bQAU\b",
    r"\bWAU\b",
    r"\bDAU\b",
    r"\bNPS\b",
]

# Patrones en el NOMBRE de la card que indican funnel
FUNNEL_NAME_PATTERNS = [
    r"\bfunnel\b",
    r"\bFunnel\b",
    r"transaction_details_funnel",
    r"\bopt.in\b",
    r"\bopt.out\b",
]

# La descripción debe mencionar EXPLÍCITAMENTE la tabla funnel para que el CE sea sospechoso.
# Si la descripción dice "transaction_details_funnel" → 'Payment' es válido → CE podría ser FP.
# Si la descripción dice solo "transaction_details" → 'Payment' NO existe → CE es correcto.
FUNNEL_DESC_KEYWORDS = [
    "transaction_details_funnel",
]

# Colecciones de funnel (case-insensitive)
FUNNEL_COLLECTIONS = [
    "funnel",
    "leads",
    "opt-in",
    "opt_in",
]

# ============================================================
# WHITELIST — decisiones confirmadas, suprimir falsas alarmas
# ============================================================
# Formato: card_id → razón por la que el conflicto es falsa alarma
# Agregar aquí cards revisadas manualmente en sesiones de QA.

CONFIRMED_CORRECT: dict[int, str] = {
    # ATV cards con CE correcto: usan statuses EXTRA incorrectos (Payable/Cancelled/Client_refund)
    # cuando ATV debería usar solo Paid. CE es correcto. CE_PERO_EXEMPT dispara por nombre "ATV".
    437:  "CE correcto: 'ATV PER WEEK - Duplicate' usa Paid+Payable+Client_refund+Cancelled. ATV debe usar solo Paid. (S56 20/04/2026)",
    521:  "CE correcto: 'Distribution of Authorized Txns & ATV - Dup' usa Paid+Payable+Cancelled+Client_refund. ATV debe solo Paid. (S56 20/04/2026)",
    # Cards FP con alta visibilidad: descripciones contienen 'transaction_status' pero modelo
    # confirmó que el filtro ES correcto ('Paid drop off' / 'Payable drop off' distribution).
    1421: "FP correcto: 'Paid drop off weekly distribution - Duplicate'. Verify_pending.py confirmó filtro correcto. (S56 20/04/2026)",
    1437: "FP correcto: 'Payable drop off distribution Monthly - Duplicate'. Verify_pending.py confirmó filtro correcto. (S56 20/04/2026)",
    # ATV cards FP: usan status='Paid' intencionalmente (ground truth ATV = counter_paid>=1 + Paid).
    # propagate_reviews.py sobreescribió FP→CE desde Excel. JSON corregido en S58.
    253:  "FP correcto: 'Distribution of Authorized Txns & ATV'. ATV usa solo Paid — ground truth. (S58 21/04/2026)",
    1136: "FP correcto: 'ATV per Week first auth trx'. ATV usa solo Paid — ground truth. (S58 21/04/2026)",
}

# ============================================================
# DETECCIÓN DE CONFLICTOS
# ============================================================

def norm(text: str) -> str:
    return text.lower().strip() if text else ""


def is_exempt_card(card_name: str) -> tuple[bool, str]:
    """Devuelve (True, razón) si la card es exenta por ground truth."""
    cn = norm(card_name)
    for pat in EXEMPT_NAME_PATTERNS:
        if re.search(pat, card_name, re.IGNORECASE):
            metric = pat.strip(r"\b").upper()
            return True, f"card es tipo '{metric}' → solo Paid es correcto por diseño de negocio"
    return False, ""


def is_funnel_card(card_name: str, coleccion: str = "") -> tuple[bool, str]:
    """Devuelve (True, razón) si la card es de tipo funnel."""
    for pat in FUNNEL_NAME_PATTERNS:
        if re.search(pat, card_name, re.IGNORECASE):
            return True, f"nombre contiene patrón de funnel: '{pat}'"
    col = norm(coleccion)
    for fc in FUNNEL_COLLECTIONS:
        if fc in col:
            return True, f"colección '{coleccion}' es de funnel"
    return False, ""


def desc_mentions_counter(desc: str) -> bool:
    d = norm(desc)
    return "counter in (0,1)" in d or "counter in(0,1)" in d or "counter in (0, 1)" in d


def desc_mentions_mto(desc: str) -> bool:
    d = norm(desc)
    return "left join" in d and "mto" in d


def desc_mentions_funnel_elements(desc: str) -> bool:
    d = norm(desc)
    return any(kw in d for kw in FUNNEL_DESC_KEYWORDS)


def desc_suggests_filter_error(desc: str) -> bool:
    """Descripción sugiere un error de filtro incompleto de status (posible CE real)."""
    d = norm(desc)
    keywords = [
        "filtro de status",
        "filtro incompleto",
        "no incluye",
        "falta",
        "omite",
        "missing",
        "incompleto",
        "transaction_status",
    ]
    return sum(1 for kw in keywords if kw in d) >= 2


def check_hallazgo_coherence(card_id: int, card_name: str, vistas: int,
                              coleccion: str, hallazgo: dict) -> list[dict]:
    """
    Verifica un hallazgo decidido contra el ground truth.
    Devuelve lista de conflictos encontrados (puede ser vacía).
    """
    status = hallazgo.get("human_review_status", "")
    desc   = hallazgo.get("descripcion", "")
    validated_by = hallazgo.get("validated_by", "")

    conflicts = []

    # Whitelist: decisión revisada manualmente y confirmada como correcta
    if card_id in CONFIRMED_CORRECT:
        return []

    if status == "confirmed_error":
        # Check 1: card exenta (ATV/MAU/DAU/NPS/etc.)
        exempt, reason = is_exempt_card(card_name)
        if exempt:
            conflicts.append({
                "tipo": CE_PERO_EXEMPT,
                "descripcion_conflicto": CONFLICT_DESCRIPTIONS[CE_PERO_EXEMPT],
                "razon": reason,
                "accion_sugerida": "Revisar: si el hallazgo dice 'solo Paid' o 'no incluye Hold', probablemente es FP.",
            })

        # Check 2: counter workaround
        if desc_mentions_counter(desc):
            conflicts.append({
                "tipo": CE_PERO_COUNTER,
                "descripcion_conflicto": CONFLICT_DESCRIPTIONS[CE_PERO_COUNTER],
                "razon": "counter IN (0,1) es workaround intencional (~17,597 registros). Ground truth: nunca reportar como error.",
                "accion_sugerida": "Cambiar a intentional.",
            })

        # Check 3: LEFT JOIN mto
        if desc_mentions_mto(desc):
            conflicts.append({
                "tipo": CE_PERO_MTO,
                "descripcion_conflicto": CONFLICT_DESCRIPTIONS[CE_PERO_MTO],
                "razon": "LEFT JOIN a tabla mto = deuda técnica sistémica documentada. Ground truth: no escalar como crítico.",
                "accion_sugerida": "Cambiar a intentional o false_positive.",
            })

        # Check 4: funnel
        is_fun, fun_reason = is_funnel_card(card_name, coleccion)
        if is_fun and desc_mentions_funnel_elements(desc):
            conflicts.append({
                "tipo": CE_PERO_FUNNEL,
                "descripcion_conflicto": CONFLICT_DESCRIPTIONS[CE_PERO_FUNNEL],
                "razon": f"Card de funnel ({fun_reason}) con hallazgo sobre Sent/Payment/Paymentlink — etapas válidas del journey.",
                "accion_sugerida": "Revisar SQL real. Si usa transaction_details_funnel, es FP/intentional.",
            })

    elif status == "false_positive":
        # Check 5: FP de alta visibilidad con descripción de error de filtro
        if vistas > 500 and desc_suggests_filter_error(desc):
            # Excluir si la card es exenta (FP correcto en ese caso)
            exempt, _ = is_exempt_card(card_name)
            is_fun, _ = is_funnel_card(card_name, coleccion)
            if not exempt and not is_fun:
                conflicts.append({
                    "tipo": FP_POSIBLE_CE,
                    "descripcion_conflicto": CONFLICT_DESCRIPTIONS[FP_POSIBLE_CE],
                    "razon": f"Card con {vistas:,} vistas marcada FP pero descripción sugiere filtro incompleto de status.",
                    "accion_sugerida": "Verificar SQL real en data/raw/lotes/ para confirmar si es FP o CE.",
                })

    return conflicts


# ============================================================
# LEER JSONs Y AUDITAR
# ============================================================

def audit_all_jsons(verbose: bool = False, solo_ce: bool = False) -> dict:
    """
    Lee todos los JSONs de resultados y audita coherencia.
    Devuelve el reporte completo.
    """
    json_files = sorted(RESULTS_DIR.glob("lote_*_resultados.json"))
    if not json_files:
        raise FileNotFoundError(f"No se encontraron JSONs en {RESULTS_DIR}")

    # Contadores globales
    total_hallazgos  = 0
    total_decididos  = 0
    total_conflictos = 0
    by_tipo          = defaultdict(int)
    conflictos_list  = []

    # Por card_id para evitar duplicados (si una card aparece en 2 lotes)
    seen_cards = set()

    for jf in json_files:
        with open(jf, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                print(f"  ⚠️  JSON inválido, saltando: {jf.name}")
                continue

        for card in data.get("cards", []):
            if not isinstance(card, dict):
                continue

            card_id   = card.get("card_id")
            card_name = card.get("card_name", "")
            vistas    = card.get("vistas", 0) or 0
            coleccion = card.get("coleccion", "") or ""

            if card_id in seen_cards:
                continue
            seen_cards.add(card_id)

            for h in card.get("hallazgos", []):
                if not isinstance(h, dict):
                    continue

                total_hallazgos += 1
                status = h.get("human_review_status", "")

                if status not in ("confirmed_error", "false_positive", "intentional"):
                    continue

                total_decididos += 1

                conflicts = check_hallazgo_coherence(
                    card_id, card_name, vistas, coleccion, h
                )

                for c in conflicts:
                    if solo_ce and c["tipo"] == FP_POSIBLE_CE:
                        continue

                    by_tipo[c["tipo"]] += 1
                    total_conflictos += 1

                    entry = {
                        "card_id":       card_id,
                        "card_name":     card_name,
                        "vistas":        vistas,
                        "lote":          jf.name,
                        "status_actual": status,
                        "descripcion":   h.get("descripcion", "")[:200],
                        "validated_by":  h.get("validated_by", ""),
                        "validated_date": h.get("validated_date", ""),
                        **c,
                    }
                    conflictos_list.append(entry)

                    if verbose:
                        print(f"\n  [{c['tipo']}] card #{card_id} ({vistas:,}v) — {card_name[:50]}")
                        print(f"    Status: {status}")
                        print(f"    Razon:  {c['razon']}")
                        print(f"    Desc:   {h.get('descripcion','')[:120]}...")
                        print(f"    Accion: {c['accion_sugerida']}")

    # Ordenar por prioridad: primero CE_PERO_EXEMPT, luego por vistas descendente
    tipo_order = {CE_PERO_EXEMPT: 0, CE_PERO_COUNTER: 1, CE_PERO_MTO: 2,
                  CE_PERO_FUNNEL: 3, FP_POSIBLE_CE: 4}
    conflictos_list.sort(key=lambda x: (tipo_order.get(x["tipo"], 9), -x.get("vistas", 0)))

    report = {
        "generado":         datetime.now().strftime("%Y-%m-%d %H:%M"),
        "total_hallazgos":  total_hallazgos,
        "total_decididos":  total_decididos,
        "total_conflictos": total_conflictos,
        "por_tipo": dict(by_tipo),
        "conflictos": conflictos_list,
    }
    return report


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Auditoría de coherencia de hallazgos decididos vs ground truth"
    )
    parser.add_argument("--verbose",  action="store_true", help="Muestra cada conflicto en consola")
    parser.add_argument("--solo-ce",  action="store_true", help="Solo muestra conflictos CE_PERO_*")
    parser.add_argument("--stats",    action="store_true", help="Solo estadísticas, sin guardar reporte")
    args = parser.parse_args()

    print("=" * 65)
    print("  Validate Coherence — Hallazgos vs Ground Truth (INICIO.md)")
    print("=" * 65)

    report = audit_all_jsons(verbose=args.verbose, solo_ce=args.solo_ce)

    # Resumen
    print(f"\n  Total hallazgos en JSONs:  {report['total_hallazgos']:,}")
    print(f"  Decididos (CE+FP+INT):     {report['total_decididos']:,}")
    print(f"  Conflictos detectados:     {report['total_conflictos']:,}")
    print()

    if report["por_tipo"]:
        print("  Por tipo:")
        for tipo, count in sorted(report["por_tipo"].items(),
                                   key=lambda x: -x[1]):
            print(f"    {tipo:<22} {count:>4}  — {CONFLICT_DESCRIPTIONS[tipo]}")
    else:
        print("  ✅ Sin conflictos detectados — clasificaciones coherentes con ground truth")

    # Cards únicas afectadas
    cards_afectadas = {c["card_id"] for c in report["conflictos"]}
    if cards_afectadas:
        print(f"\n  Cards únicas con conflicto: {len(cards_afectadas)}")
        # Agrupar por tipo para mostrar IDs
        by_tipo_ids = defaultdict(list)
        for c in report["conflictos"]:
            by_tipo_ids[c["tipo"]].append(c["card_id"])
        for tipo, ids in sorted(by_tipo_ids.items()):
            ids_str = ", ".join(str(i) for i in sorted(set(ids))[:20])
            suffix = f"... ({len(set(ids))} total)" if len(set(ids)) > 20 else ""
            print(f"\n    [{tipo}] cards: {ids_str}{suffix}")

    if not args.stats:
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        print(f"\n  💾 Reporte guardado en: {REPORT_PATH.name}")
        print(f"     ({report['total_conflictos']} conflictos para revisar)")
    else:
        print("\n  Modo --stats: no se guardó reporte.")

    print("=" * 65)

    # Indicación de siguiente paso
    if report["total_conflictos"] > 0:
        print("\n  Siguiente paso:")
        print("    1. Revisa data/processed/resultados/coherence_report.json")
        print("    2. Para conflictos confirmados: agrega CARD_DECISIONS en auto_classify.py")
        print("    3. Re-ejecuta auto_classify.py para corregir los JSONs")


if __name__ == "__main__":
    main()
