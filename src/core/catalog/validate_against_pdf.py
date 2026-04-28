"""
src/core/catalog/validate_against_pdf.py — Validación del diccionario contra el ground truth del PDF.

Compara las definiciones LLM de metrics_master.json contra pdf_metrics_canonical.json
y genera data/processed/resultados/validation_report.json con tres categorías:
  - OK       → definición alineada con el PDF
  - MISMATCH → definición existe pero tiene discrepancias detectables
  - MISSING  → métrica del PDF sin ninguna card correspondiente en el catálogo

Uso:
    python -m src.core.catalog.validate_against_pdf
    python -m src.core.catalog.validate_against_pdf --verbose
"""

import argparse
import io
import json
import re
import sys
from pathlib import Path

# Forzar UTF-8 en stdout para Windows (evita UnicodeEncodeError con caracteres especiales)
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

CANONICAL_FILE  = _ROOT / "data" / "raw" / "ground_truth" / "pdf_metrics_canonical.json"
MASTER_FILE     = _ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
REPORT_FILE     = _ROOT / "data" / "processed" / "resultados" / "validation_report.json"

# ---------------------------------------------------------------------------
# Overrides manuales: fuerzan la card canónica por nombre de métrica PDF.
# Usar cuando _find_best_match elige la card equivocada por coincidencia de nombre.
# Verificados en sesiones de auditoría:
#   MTU:       card #2804 es un funnel (12 CTEs), no conteo MTU. Canónica: #33 (21,728 vistas)
#   New User:  card #230 está deprecada. Canónica: #557 "# of New Users (Acq - Paid)"
#   Hold Rate: card #1122 "Tasa de éxito" tiene overlap de nombre falso ('tasa de').
#              Canónica: #805 "% hold of total by day" (S57 20/04/2026)
# ---------------------------------------------------------------------------
MANUAL_OVERRIDES: dict[str, int] = {
    "MTU":       33,
    "New User":  557,
    "Hold Rate": 805,
    # DAU: card #3633 "Usuarios Activos" (QA Etiquetado) tiene score mayor pero es card de QA,
    # no la canónica. Canónica establecida: #354 "DAU" (pendiente data_owner — excluye Hold). (S58)
    "DAU":       354,
    # SLA: card #1566 "Time Response Uniteller" tiene enrichment alucinado y sin tag #ops.
    # Canónica: #2997 "Tiempo de Procesamiento" con tags #ops/#pagos. (S58)
    "SLA First Response Time": 2997,
}

# ---------------------------------------------------------------------------
# Gaps documentados — métricas del PDF que no aplican a la validación normal.
# Se reportan por separado (no cuentan como MISMATCH) y quedan explícitas como
# brechas conocidas para el stakeholder.
#
# fuera_de_scope: la métrica existe en el PDF pero decisión de negocio la excluye
#                 del scope de la auditoría (ej. se mide en otra herramienta).
# sin_card_canonica: la métrica del PDF no tiene una card canónica en Metabase;
#                    se listan cards candidatas y se espera decisión futura.
# ---------------------------------------------------------------------------
DOCUMENTED_GAPS: dict[str, dict] = {
    "CSAT": {
        "status": "fuera_de_scope",
        "reason": "CSAT pertenece a customer support y se mide fuera de Metabase. Decisión S59 (22/04/2026): no incluir en la auditoría de métricas de datos.",
        "candidates": [],
    },
    "FX Revenue": {
        "status": "sin_card_canonica",
        "reason": "PDF define FX Revenue como 'counter_auth >= 2' (User Pricing, 1.1% take rate). Ninguna card existente cumple estrictamente la definición. Gap documentado para decisión futura (crear card nueva o elegir aproximada). S59 (22/04/2026).",
        "candidates": [
            {"card_id": 169,  "reason": "usa counter=1 (Lead Pricing), NO canónica"},
            {"card_id": 4,    "reason": "sin filtro counter (mezcla Lead+User), NO canónica estricta"},
            {"card_id": 3070, "reason": "tabla snapshot monthly_metrics — filtro no verificable desde SQL"},
            {"card_id": 3240, "reason": "counter>=2 pero calcula promedio por tx, no total"},
        ],
    },
}


# ---------------------------------------------------------------------------
# Matching helpers
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Normaliza texto para comparación: minúsculas, sin puntuación extra."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9áéíóúñü\s/]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _token_overlap(a: str, b: str) -> float:
    """Jaccard sobre tokens de dos strings normalizados."""
    tokens_a = set(_normalize(a).split())
    tokens_b = set(_normalize(b).split())
    if not tokens_a or not tokens_b:
        return 0.0
    return len(tokens_a & tokens_b) / len(tokens_a | tokens_b)


def _find_best_match(pdf_metric: dict, catalog: list[dict]) -> tuple[dict | None, float]:
    """
    Busca la card del catálogo que mejor coincide con la métrica del PDF.
    Estrategia: nombre exacto > alias exacto > token overlap.
    Retorna (mejor_card, score).
    """
    pdf_name = pdf_metric["name"]
    aliases = [pdf_name] + pdf_metric.get("aliases", [])

    best_card = None
    best_score = 0.0

    for card in catalog:
        card_name = card.get("primary_card_name", "")
        # Exacto
        for alias in aliases:
            if _normalize(alias) == _normalize(card_name):
                return card, 1.0
        # Token overlap
        for alias in aliases:
            score = _token_overlap(alias, card_name)
            if score > best_score:
                best_score = score
                best_card = card

    return best_card, best_score


# ---------------------------------------------------------------------------
# Discrepancy checkers
# ---------------------------------------------------------------------------

def _check_anchoring_fields(pdf_metric: dict, card: dict) -> list[str]:
    """
    Verifica que los campos ancla del PDF estén mencionados en las definiciones del catálogo.
    """
    issues = []
    anchors = pdf_metric.get("anchoring_fields", [])
    defn = (card.get("business_definition", "") + " " + card.get("technical_translation", "")).lower()
    sql  = card.get("core_logic", "").lower()
    # También revisar el nombre de la card (contiene hints como "(Paid)")
    card_name = card.get("primary_card_name", "").lower()
    full_text = defn + " " + sql + " " + card_name

    # Equivalencias español↔inglés para keywords frecuentes de anchors
    _ES_EN_EQUIV: dict[str, str] = {
        "paid":   "pagad",  # "Paid" ↔ "pagadas/pagado"
        "pagad":  "paid",
    }

    for anchor in anchors:
        anchor_keywords = _normalize(anchor).split()
        # Al menos la mitad de las keywords del anchor deben aparecer en el texto
        def _kw_found(kw: str, text: str) -> bool:
            if kw in text:
                return True
            equiv = _ES_EN_EQUIV.get(kw)
            return equiv is not None and equiv in text

        found = sum(1 for kw in anchor_keywords if _kw_found(kw, full_text))
        if found < max(1, len(anchor_keywords) // 2):
            issues.append(f"Campo ancla no mencionado: '{anchor}'")

    return issues


def _check_taxonomy_tags(pdf_metric: dict, card: dict) -> list[str]:
    """
    Verifica que las tags de taxonomía estén presentes si el PDF las especifica.
    """
    issues = []
    taxonomy = pdf_metric.get("taxonomy", "")
    card_tags = card.get("tags", [])

    taxonomy_tag_map = {
        "1tu_mtu":          ["#1tu", "#mtu"],
        # '#retencion' es la variante de tagger para cohort-retention (S57 20/04/2026)
        "nuevo_recurrente": ["#nuevo-usuario", "#recurrente", "#retencion"],
        "revenue":          ["#ventas", "#fees", "#fx"],
        # '#lead-pricing' es la tag específica para métricas de costo Lead Pricing (S57 20/04/2026)
        "marketing":        ["#usuarios", "#conversion", "#lead-pricing"],
        "ops":              ["#ops"],
        "fraude":           ["#fraude", "#chargebacks"],
    }

    expected_any = taxonomy_tag_map.get(taxonomy, [])
    if expected_any and not any(t in card_tags for t in expected_any):
        issues.append(f"Sin tag de taxonomía '{taxonomy}' (esperado alguno de: {expected_any})")

    return issues


def _check_eliminated(pdf_metric: dict, card: dict) -> list[str]:
    """Verifica que métricas eliminadas estén marcadas como Deprecadas."""
    issues = []
    if card.get("certification_status") != "Deprecada":
        issues.append(
            f"Esta métrica fue ELIMINADA pero el catálogo la tiene como '{card.get('certification_status')}'"
        )
    return issues


def _classify_match(pdf_metric: dict, card: dict, match_score: float) -> tuple[str, list[str]]:
    """
    Clasifica el resultado como OK, MISMATCH, y retorna lista de issues.
    """
    issues = []

    issues += _check_anchoring_fields(pdf_metric, card)
    issues += _check_taxonomy_tags(pdf_metric, card)

    # Verificar subtypes si existen
    for subtype in pdf_metric.get("subtypes", []):
        sub_anchors = subtype.get("anchoring_fields", [])
        sql = card.get("core_logic", "").lower()
        for anchor in sub_anchors:
            anchor_kws = _normalize(anchor).split()
            if not any(kw in sql for kw in anchor_kws):
                issues.append(
                    f"Subtipo '{subtype['name']}': campo ancla '{anchor}' no encontrado en SQL"
                )

    # Match score bajo → posible card incorrecta
    if match_score < 0.3:
        issues.append(f"Match débil con el catálogo (score={match_score:.2f}) — posible card incorrecta")

    status = "OK" if not issues else "MISMATCH"
    return status, issues


# ---------------------------------------------------------------------------
# Main validation
# ---------------------------------------------------------------------------

def run(verbose: bool = False) -> dict:
    if not CANONICAL_FILE.exists():
        print(f"ERROR: No se encontró el ground truth en {CANONICAL_FILE}")
        sys.exit(1)
    if not MASTER_FILE.exists():
        print(f"ERROR: No se encontró el catálogo en {MASTER_FILE}")
        sys.exit(1)

    canonical = json.loads(CANONICAL_FILE.read_text(encoding="utf-8"))
    master    = json.loads(MASTER_FILE.read_text(encoding="utf-8"))
    catalog   = master.get("metrics", [])

    print(f"Ground truth: {len(canonical['metrics'])} métricas del PDF")
    print(f"Catálogo:     {len(catalog)} cards en metrics_master.json")
    print()

    results_ok      = []
    results_mismatch = []
    results_missing  = []
    results_gaps     = []

    # Validar métricas eliminadas
    eliminated_names = {e["name"].lower() for e in canonical.get("eliminated_metrics", [])}

    for pdf_metric in canonical["metrics"]:
        pdf_name = pdf_metric["name"]

        # Si está marcada como gap documentado, reportar aparte y continuar
        if pdf_name in DOCUMENTED_GAPS:
            gap = DOCUMENTED_GAPS[pdf_name]
            results_gaps.append({
                "pdf_metric": pdf_name,
                "gap_status": gap["status"],
                "reason":     gap["reason"],
                "candidates": gap.get("candidates", []),
            })
            if verbose:
                print(f"  [GAP]       {pdf_name} ({gap['status']})")
            continue

        # Aplicar override manual si existe para esta métrica
        override_id = MANUAL_OVERRIDES.get(pdf_name)
        if override_id is not None:
            override_card = next(
                (c for c in catalog if c.get("primary_card_id") == override_id), None
            )
            if override_card is not None:
                best_card, score = override_card, 1.0
            else:
                best_card, score = _find_best_match(pdf_metric, catalog)
        else:
            best_card, score = _find_best_match(pdf_metric, catalog)

        if best_card is None or score < 0.1:
            results_missing.append({
                "pdf_metric": pdf_name,
                "aliases": pdf_metric.get("aliases", []),
                "taxonomy": pdf_metric.get("taxonomy", ""),
                "reason": "No se encontró ninguna card con nombre similar en el catálogo",
                "action": "Crear entrada manual o revisar si existe con nombre distinto"
            })
            if verbose:
                print(f"  [MISSING]   {pdf_name}")
            continue

        status, issues = _classify_match(pdf_metric, best_card, score)

        entry = {
            "pdf_metric":    pdf_name,
            "matched_card":  best_card.get("primary_card_name"),
            "card_id":       best_card.get("primary_card_id"),
            "match_score":   round(score, 3),
            "catalog_status": best_card.get("certification_status"),
            "card_url":      best_card.get("url", ""),
        }

        if status == "OK":
            results_ok.append(entry)
            if verbose:
                print(f"  [OK]        {pdf_name} -> #{best_card.get('primary_card_id')} (score={score:.2f})")
        else:
            entry["issues"] = issues
            results_mismatch.append(entry)
            if verbose:
                print(f"  [MISMATCH]  {pdf_name} -> #{best_card.get('primary_card_id')} (score={score:.2f})")
                for issue in issues:
                    print(f"              ! {issue}")

    # Métricas eliminadas — verificar que estén deprecadas en el catálogo
    eliminated_results = []
    for elim in canonical.get("eliminated_metrics", []):
        elim_name = elim["name"]
        best_card, score = _find_best_match({"name": elim_name, "aliases": []}, catalog)
        if best_card and score >= 0.25:
            issues = _check_eliminated(elim, best_card)
            eliminated_results.append({
                "metric_name":    elim_name,
                "reason":         elim.get("reason", ""),
                "eliminated_date": elim.get("eliminated_date", ""),
                "matched_card":   best_card.get("primary_card_name"),
                "card_id":        best_card.get("primary_card_id"),
                "catalog_status": best_card.get("certification_status"),
                "issues":         issues,
            })

    # Resumen
    report = {
        "generated_at": __import__("datetime").datetime.now().strftime("%Y-%m-%d %H:%M"),
        "summary": {
            "total_pdf_metrics": len(canonical["metrics"]),
            "ok": len(results_ok),
            "mismatch": len(results_mismatch),
            "missing": len(results_missing),
            "documented_gaps": len(results_gaps),
            "eliminated_checked": len(eliminated_results),
        },
        "ok":              results_ok,
        "mismatch":        results_mismatch,
        "missing":         results_missing,
        "documented_gaps": results_gaps,
        "eliminated":      eliminated_results,
    }

    REPORT_FILE.parent.mkdir(parents=True, exist_ok=True)
    REPORT_FILE.write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    print(f"\n{'='*55}")
    print(f"  Resultado de validación")
    print(f"{'='*55}")
    print(f"  OK:        {len(results_ok)}")
    print(f"  MISMATCH:  {len(results_mismatch)}")
    print(f"  MISSING:   {len(results_missing)}")
    print(f"  GAPS documentados: {len(results_gaps)}")
    print(f"  Eliminadas verificadas: {len(eliminated_results)}")
    print(f"\n  Reporte guardado en: {REPORT_FILE.relative_to(_ROOT)}")
    print(f"{'='*55}")

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validar diccionario contra PDF de training")
    parser.add_argument("--verbose", "-v", action="store_true", help="Mostrar detalle de cada métrica")
    args = parser.parse_args()
    run(verbose=args.verbose)
