"""
src/core/catalog/pipeline.py — Diccionario de Métricas Vivo: orquestador principal.

Uso:
    python -m src.core.catalog.pipeline [opciones]

    --skip-enrichment   Omite las llamadas al LLM (usa cache existente o vacío)
    --skip-structural   Omite el análisis de similitud estructural (solo usa exact dedup)
    --limit N           Procesa solo N cards (modo desarrollo/test)
    --output-dir PATH   Directorio de salida (default: data/processed/diccionario/)

Módulos ejecutados en orden:
    0. Carga del ground truth canónico (data/raw/ground_truth/pdf_metrics_canonical.json)
    1. Carga de datos (lotes + audit results + duplicados + obsolescencia)
    2. Análisis estructural (tablas, JOINs, core logic)
    3. Deduplicación (exacta → carga duplicados_sql.json, estructural → Union-Find)
    4. Enriquecimiento semántico (LLM, con cache persistente y contexto canónico)
    5. Clasificación + Tagging
    6. Generación de outputs (JSON, CSV, Wiki, Conflictos, Mantenimiento)
    7. Validación contra ground truth del PDF
    8. Snapshot de popularidad Metabase + ranking 7d/30d (best-effort)
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

from src.core.catalog.loaders import load_all_cards, load_audit_results, load_duplicates, load_obsolescence
from src.core.catalog.deduplication import enrich_cards_with_structure, build_structural_groups
from src.core.catalog.enrichment import load_cache, save_cache, enrich_all
from src.core.catalog.tagger import generate_tags, classify_certification
from src.core.catalog.writers import (
    write_master_json, write_master_csv, write_wiki,
    write_conflicts_report, write_maintenance_report,
)


# ---------------------------------------------------------------------------
# Deduplication map builder
# ---------------------------------------------------------------------------

def _build_dedup_map(cards: dict[int, dict], existing_dup: dict, skip_structural: bool) -> dict[int, dict]:
    """
    Construye un mapa card_id → dedup_info con las siguientes claves:
      dedup_type:        "DUPLICADO EXACTO" | "VARIACION" | "UNICA"
      is_primary:        True si es la card representativa del grupo
      parent_card_id:    card_id de la primaria (solo si is_primary=False)
      duplicate_card_ids: list de card_ids exactamente iguales (solo si is_primary=True)
      variation_card_ids: list de card_ids variantes (solo si is_primary=True y METRICA MADRE)
      vistas_total_group: suma de vistas de todo el grupo
    """
    dedup: dict[int, dict] = {}

    # --- Paso 1: Duplicados exactos desde duplicados_sql.json ---
    for group in existing_dup.get("grupos_exactos", []):
        group_cards = group.get("cards", [])
        if len(group_cards) < 2:
            continue
        primary = max(group_cards, key=lambda c: c.get("vistas", 0))
        primary_id = primary["id"]
        dup_ids = [c["id"] for c in group_cards if c["id"] != primary_id]
        total_views = group.get("total_vistas", sum(c.get("vistas", 0) for c in group_cards))

        dedup[primary_id] = {
            "dedup_type":          "DUPLICADO EXACTO",
            "is_primary":          True,
            "parent_card_id":      None,
            "duplicate_card_ids":  dup_ids,
            "variation_card_ids":  [],
            "vistas_total_group":  total_views,
        }
        for dup_id in dup_ids:
            dedup[dup_id] = {
                "dedup_type":          "DUPLICADO EXACTO",
                "is_primary":          False,
                "parent_card_id":      primary_id,
                "duplicate_card_ids":  [],
                "variation_card_ids":  [],
                "vistas_total_group":  total_views,
            }

    # --- Paso 2: Grupos fuzzy existentes ---
    for group in existing_dup.get("grupos_fuzzy", []):
        group_cards = group.get("cards", [])
        if len(group_cards) < 2:
            continue
        primary = max(group_cards, key=lambda c: c.get("vistas", 0))
        primary_id = primary["id"]

        # No sobreescribir si ya está clasificado como exact duplicate
        if primary_id in dedup:
            continue

        var_ids = [c["id"] for c in group_cards if c["id"] != primary_id]
        total_views = sum(c.get("vistas", 0) for c in group_cards)

        dedup[primary_id] = {
            "dedup_type":          "METRICA MADRE",
            "is_primary":          True,
            "parent_card_id":      None,
            "duplicate_card_ids":  [],
            "variation_card_ids":  var_ids,
            "vistas_total_group":  total_views,
        }
        for vid in var_ids:
            if vid not in dedup:
                dedup[vid] = {
                    "dedup_type":          "VARIACION",
                    "is_primary":          False,
                    "parent_card_id":      primary_id,
                    "duplicate_card_ids":  [],
                    "variation_card_ids":  [],
                    "vistas_total_group":  total_views,
                }

    # --- Paso 3: Similitud estructural (nuevo) ---
    if not skip_structural:
        struct_groups = build_structural_groups(cards)
        for group in struct_groups:
            # Saltar si todos ya son exact duplicates entre sí
            all_exact = all(
                dedup.get(cid, {}).get("dedup_type") == "DUPLICADO EXACTO"
                for cid in group
            )
            if all_exact:
                continue

            primary_id = max(group, key=lambda cid: cards[cid].get("vistas", 0))
            var_ids = [cid for cid in group if cid != primary_id]
            total_views = sum(cards[cid].get("vistas", 0) for cid in group)

            if primary_id not in dedup:
                dedup[primary_id] = {
                    "dedup_type":          "METRICA MADRE",
                    "is_primary":          True,
                    "parent_card_id":      None,
                    "duplicate_card_ids":  [],
                    "variation_card_ids":  var_ids,
                    "vistas_total_group":  total_views,
                }
            elif dedup[primary_id]["dedup_type"] == "METRICA MADRE":
                # Agregar variantes nuevas al grupo existente
                existing_vars = set(dedup[primary_id]["variation_card_ids"])
                dedup[primary_id]["variation_card_ids"] = sorted(existing_vars | set(var_ids))

            for vid in var_ids:
                if vid not in dedup:
                    dedup[vid] = {
                        "dedup_type":         "VARIACION",
                        "is_primary":         False,
                        "parent_card_id":     primary_id,
                        "duplicate_card_ids": [],
                        "variation_card_ids": [],
                        "vistas_total_group": total_views,
                    }

    return dedup


# ---------------------------------------------------------------------------
# Conflict detection
# ---------------------------------------------------------------------------

def _detect_conflicts(metrics: list[dict]) -> list[dict]:
    """
    Encuentra métricas con nombres similares (primeras 4 palabras) pero
    tablas de origen distintas → posible lógica SQL contradictoria.
    """
    def name_key(name: str) -> str:
        return " ".join(name.lower().split()[:4])

    by_name: dict[str, list[dict]] = defaultdict(list)
    for m in metrics:
        # Solo incluir métricas con tablas detectadas (excluye CTEs mal parseados)
        if m.get("tables_referenced"):
            by_name[name_key(m.get("primary_card_name", ""))].append(m)

    conflicts = []
    for key, group in by_name.items():
        if len(group) < 2:
            continue
        table_sets = [frozenset(m.get("tables_referenced") or []) for m in group]
        if len(set(table_sets)) > 1:
            conflicts.append(
                {
                    "conflict_type":   "nombre_similar_tablas_distintas",
                    "name_group":      key,
                    "num_metrics":     len(group),
                    "metrics": [
                        {
                            "card_id":      m["primary_card_id"],
                            "name":         m["primary_card_name"],
                            "tables":       sorted(m.get("tables_referenced") or []),
                            "score_salud":  m.get("score_salud"),
                            "status":       m.get("certification_status"),
                            "url":          m.get("url", ""),
                        }
                        for m in group
                    ],
                }
            )
    return conflicts


# ---------------------------------------------------------------------------
# Metric entry builder
# ---------------------------------------------------------------------------

def _build_metric_entry(
    card: dict,
    audit: dict,
    obs: dict,
    dedup_info: dict,
    enrichment: dict,
    canonical_names: set[str] | None = None,
) -> dict:
    tables = sorted(card.get("tables") or frozenset())
    joins  = sorted(card.get("joins") or frozenset())
    cid    = card["card_id"]

    card_name_lower = card.get("card_name", "").lower()
    pdf_validated = (
        any(alias in card_name_lower for alias in (canonical_names or set()))
        or card_name_lower in (canonical_names or set())
    )

    return {
        # Identidad
        "primary_card_id":    cid,
        "primary_card_name":  card.get("card_name", ""),
        "collection":         card.get("collection", "Sin Colección"),
        "lote":               card.get("lote", ""),
        "url":                f"https://metabase.example.com/card/{cid}",
        # Tráfico
        "vistas_primary":     card.get("vistas", 0),
        "vistas_total_group": dedup_info.get("vistas_total_group", card.get("vistas", 0)),
        # Dedup
        "dedup_type":           dedup_info.get("dedup_type", "UNICA"),
        "is_primary":           dedup_info.get("is_primary", True),
        "parent_card_id":       dedup_info.get("parent_card_id"),
        "duplicate_card_ids":   dedup_info.get("duplicate_card_ids", []),
        "variation_card_ids":   dedup_info.get("variation_card_ids", []),
        # SQL análisis
        "sql_fingerprint":    card.get("sql_fingerprint", ""),
        "tables_referenced":  tables,
        "joins":              joins,
        "core_logic":         card.get("core_logic", ""),
        "where_summary":      card.get("where_summary", ""),
        # Auditoría
        "score_salud":            audit.get("score_salud", 100),
        "hallazgos":              audit.get("hallazgos", []),
        "human_review_status":    audit.get("human_review_status"),
        "human_review_summary":   audit.get("human_review_summary", {}),
        "human_review_updated":   audit.get("human_review_updated"),
        # Semántico
        "business_definition":    enrichment.get("business_definition", ""),
        "technical_translation":  enrichment.get("technical_translation", ""),
        # Clasificación
        "certification_status":   classify_certification(card, audit, obs),
        "tags":                   generate_tags({**card, "tables": frozenset(tables)}),
        # Obsolescencia
        "uso_categoria":  obs.get("uso_categoria", ""),
        "last_used_at":   obs.get("last_used_at", ""),
        "created_at":     obs.get("created_at", ""),
        "accion":         obs.get("accion", ""),
        # Validación contra ground truth del PDF
        "pdf_validated":  pdf_validated,
    }


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def _load_canonical_names() -> set[str]:
    """
    Carga el ground truth del PDF y retorna el conjunto de nombres canónicos
    (nombre + aliases) para marcar pdf_validated en el catálogo.
    """
    canonical_file = _ROOT / "data" / "raw" / "ground_truth" / "pdf_metrics_canonical.json"
    if not canonical_file.exists():
        return set()
    try:
        data = json.loads(canonical_file.read_text(encoding="utf-8"))
    except Exception:
        return set()

    names: set[str] = set()
    for m in data.get("metrics", []):
        names.add(m["name"].lower())
        for alias in m.get("aliases", []):
            names.add(alias.lower())
    return names


def run(args: argparse.Namespace) -> None:
    sep = "=" * 62
    print(sep)
    print("  Diccionario de Métricas Vivo — Pipeline")
    print(sep)

    # ── 0. Ground truth canónico ─────────────────────────────────────────
    print("\n[0/6] Cargando ground truth del PDF de training...")
    canonical_names = _load_canonical_names()
    if canonical_names:
        print(f"  Métricas canónicas cargadas: {len(canonical_names)} nombres/aliases")
    else:
        print("  AVISO: data/raw/ground_truth/pdf_metrics_canonical.json no encontrado — pdf_validated=false en todas las cards")

    # ── 1. Carga de datos ────────────────────────────────────────────────
    print("\n[1/7] Cargando datos...")
    cards         = load_all_cards()
    audit_results = load_audit_results()
    dup_data      = load_duplicates()
    obs_data      = load_obsolescence()

    if args.limit:
        card_ids = sorted(cards)[:args.limit]
        cards = {cid: cards[cid] for cid in card_ids}

    print(f"  Cards cargadas:          {len(cards)}")
    print(f"  Con resultado de audit:  {sum(1 for cid in cards if cid in audit_results)}")
    print(f"  Con dato de obsolesc.:   {sum(1 for cid in cards if cid in obs_data)}")
    print(f"  Grupos duplicados exact: {len(dup_data.get('grupos_exactos', []))}")

    # ── 2. Análisis estructural ──────────────────────────────────────────
    print("\n[2/7] Análisis estructural (tablas, JOINs, core logic)...")
    enrich_cards_with_structure(cards)

    # ── 3. Deduplicación ─────────────────────────────────────────────────
    print("\n[3/7] Deduplicación...")
    if args.skip_structural:
        print("  Similitud estructural: OMITIDA (--skip-structural)")
    else:
        print("  Calculando similitud estructural... (puede tardar ~30s)")

    dedup_map = _build_dedup_map(cards, dup_data, args.skip_structural)

    exact_primaries   = sum(1 for v in dedup_map.values() if v["is_primary"] and v["dedup_type"] == "DUPLICADO EXACTO")
    exact_dupes       = sum(1 for v in dedup_map.values() if not v["is_primary"] and v["dedup_type"] == "DUPLICADO EXACTO")
    madre_count       = sum(1 for v in dedup_map.values() if v["is_primary"] and v["dedup_type"] == "METRICA MADRE")
    variacion_count   = sum(1 for v in dedup_map.values() if v["dedup_type"] == "VARIACION")
    unicas            = len(cards) - len(dedup_map)

    print(f"  Grupos exactos (primarias):   {exact_primaries}")
    print(f"  Duplicados a eliminar:         {exact_dupes}")
    print(f"  Métricas madre (estructural):  {madre_count}")
    print(f"  Variaciones:                   {variacion_count}")
    print(f"  Cards únicas (sin dedup):      {unicas}")

    # ── 4. Enriquecimiento semántico ─────────────────────────────────────
    cache = load_cache()

    if args.skip_enrichment:
        print("\n[4/7] Enriquecimiento semántico — OMITIDO (--skip-enrichment)")
        enrichments: dict[int, dict] = {cid: {"business_definition": "", "technical_translation": ""} for cid in cards}
    else:
        print("\n[4/7] Enriquecimiento semántico (LLM)...")
        # Solo enriquecemos cards únicas o primarias (no duplicados redundantes)
        to_enrich = [
            cards[cid]
            for cid in cards
            if dedup_map.get(cid, {}).get("is_primary", True)
            and dedup_map.get(cid, {}).get("dedup_type", "UNICA") != "DUPLICADO EXACTO"
            or cid not in dedup_map
        ]
        # Simplificar: incluir exactamente las no-redundantes
        # Duplicados exactos no-primarios se excluyen (copias redundantes, heredan definición)
        # Variaciones se incluyen con contexto de su métrica madre para definición propia
        to_enrich = []
        for cid in cards:
            dinfo = dedup_map.get(cid, {})
            dedup_type = dinfo.get("dedup_type", "UNICA")
            is_primary = dinfo.get("is_primary", True)
            if dedup_type == "DUPLICADO EXACTO" and not is_primary:
                continue
            card_data = dict(cards[cid])
            if dedup_type == "VARIACION":
                parent_id = dinfo.get("parent_card_id")
                card_data["parent_card_name"] = (
                    cards[parent_id].get("card_name", "") if parent_id and parent_id in cards else ""
                )
                card_data["is_variation"] = True
            to_enrich.append(card_data)

        variation_count_enrich = sum(1 for c in to_enrich if c.get("is_variation"))
        print(f"  Métricas a enriquecer: {len(to_enrich)} ({variation_count_enrich} variaciones con definición propia)")
        enrichments = enrich_all(to_enrich, cache, verbose=True)
        # Duplicados exactos no-primarios heredan definición de su primaria
        for cid in cards:
            if cid not in enrichments:
                parent_id = dedup_map.get(cid, {}).get("parent_card_id")
                enrichments[cid] = enrichments.get(parent_id, {"business_definition": "", "technical_translation": ""})

    # ── 5. Construcción del catálogo ─────────────────────────────────────
    print("\n[5/7] Construyendo catálogo de métricas...")
    metrics = []
    for cid, card in cards.items():
        audit   = audit_results.get(cid, {})
        obs     = obs_data.get(cid, {})
        dinfo   = dedup_map.get(cid, {})
        enrich  = enrichments.get(cid, {"business_definition": "", "technical_translation": ""})
        metrics.append(_build_metric_entry(card, audit, obs, dinfo, enrich, canonical_names))

    # Ordenar: primero por vistas desc, duplicados al final
    def sort_key(m):
        order = {"UNICA": 0, "METRICA MADRE": 1, "DUPLICADO EXACTO": 2, "VARIACION": 3}
        return (order.get(m.get("dedup_type", "UNICA"), 9), -(m.get("vistas_primary") or 0))

    metrics.sort(key=sort_key)

    conflicts = _detect_conflicts(metrics)
    print(f"  Total métricas en catálogo:         {len(metrics)}")
    print(f"  Conflictos de veracidad detectados: {len(conflicts)}")

    cert    = sum(1 for m in metrics if m.get("certification_status") == "Certificada por Agente")
    review  = sum(1 for m in metrics if m.get("certification_status") == "En Revisión")
    dep     = sum(1 for m in metrics if m.get("certification_status") == "Deprecada")
    print(f"  Certificadas: {cert}  |  En Revisión: {review}  |  Deprecadas: {dep}")

    # ── 6. Generación de outputs ─────────────────────────────────────────
    print("\n[6/7] Generando outputs...")
    p = write_master_json(metrics);       print(f"  OK  {p.relative_to(_ROOT)}")
    p = write_master_csv(metrics);        print(f"  OK  {p.relative_to(_ROOT)}")
    p = write_wiki(metrics);              print(f"  OK  {p.relative_to(_ROOT)}/")
    p = write_conflicts_report(conflicts);print(f"  OK  {p.relative_to(_ROOT)}")
    p = write_maintenance_report(metrics);print(f"  OK  {p.relative_to(_ROOT)}")

    # ── 7. Validación contra ground truth del PDF ────────────────────────
    print("\n[7/8] Validando definiciones contra PDF de training...")
    validated_count = sum(1 for m in metrics if m.get("pdf_validated"))
    print(f"  Cards con match en ground truth PDF: {validated_count} / {len(metrics)}")
    if canonical_names:
        try:
            from src.core.catalog.validate_against_pdf import run as run_validation
            run_validation(verbose=False)
        except Exception as e:
            print(f"  AVISO: validación completa no ejecutada ({e})")
    else:
        print("  Validación completa omitida (ground truth no disponible)")

    # ── 8. Snapshot + ranking de popularidad ─────────────────────────────
    print("\n[8/8] Snapshot de popularidad (Metabase /api/card)...")
    try:
        from src.core.extraction.snapshot_popularity import run as snapshot_run
        from src.core.catalog.compute_popularity import run as popularity_run
        snapshot_run(metrics)
        popularity_run()
    except SystemExit:
        print("  AVISO: METABASE_SESSION no configurada — snapshot omitido; diccionario usará datos previos")
    except Exception as e:
        print(f"  AVISO: popularidad no actualizada ({e}) — diccionario usará datos previos")

    print(f"\n{sep}")
    print("  Pipeline completado. Outputs en data/processed/diccionario/")
    print(sep)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Diccionario de Métricas Vivo — Metabase",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  # Ejecución completa (con LLM y similitud estructural)
  python src/catalog/pipeline.py

  # Sin LLM (modo rápido / sin costo de API)
  python src/catalog/pipeline.py --skip-enrichment

  # Solo dedup exacto, sin similitud estructural
  python src/catalog/pipeline.py --skip-structural --skip-enrichment

  # Test con 50 cards
  python src/catalog/pipeline.py --limit 50 --skip-enrichment
        """,
    )
    parser.add_argument(
        "--skip-enrichment",
        action="store_true",
        help="Omite las llamadas al LLM (usa cache existente o cadena vacía)",
    )
    parser.add_argument(
        "--skip-structural",
        action="store_true",
        help="Omite el análisis de similitud estructural (solo usa exact dedup)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Procesa solo N cards (útil para testing)",
    )
    args = parser.parse_args()
    run(args)
