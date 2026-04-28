"""
src/core/catalog/writers.py — Generación de outputs: JSON maestro, CSV, Wiki Markdown,
Reporte de Conflictos y Reporte de Mantenimiento.
"""

import csv
import json
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
OUTPUT_DIR = _ROOT / "data" / "processed" / "diccionario"

_NOW = datetime.now().strftime("%Y-%m-%d %H:%M")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_folder(name: str) -> str:
    """Convierte un nombre en un nombre de carpeta URL-safe."""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name)
    name = re.sub(r"\s+", "_", name)          # espacios → guión bajo
    name = re.sub(r"_+", "_", name)           # colapsar múltiples _
    return name[:60].strip("_. ") or "sin_coleccion"


def _flatten_list(val) -> str:
    if isinstance(val, (list, set, frozenset)):
        return "|".join(str(x) for x in sorted(val))
    return str(val) if val is not None else ""


# ---------------------------------------------------------------------------
# JSON maestro
# ---------------------------------------------------------------------------

_MASTER_CSV_FIELDS = [
    "primary_card_id", "primary_card_name", "collection", "lote",
    "vistas_primary", "vistas_total_group",
    "dedup_type", "duplicate_card_ids", "variation_card_ids",
    "certification_status", "tags",
    "score_salud", "human_review_status",
    "tables_referenced", "joins",
    "business_definition", "technical_translation",
    "uso_categoria", "last_used_at", "created_at", "accion",
    "sql_fingerprint", "url", "pdf_validated",
]


def write_master_json(metrics: list[dict]) -> Path:
    """Escribe metrics_master.json con toda la metadata técnica y de negocio."""
    out = OUTPUT_DIR / "metrics_master.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": _NOW,
        "total_metrics": len(metrics),
        "certified": sum(1 for m in metrics if m.get("certification_status") == "Certificada por Agente"),
        "in_review": sum(1 for m in metrics if m.get("certification_status") == "En Revisión"),
        "deprecated": sum(1 for m in metrics if m.get("certification_status") == "Deprecada"),
        "exact_duplicates": sum(1 for m in metrics if m.get("dedup_type") == "DUPLICADO EXACTO"),
        "variations": sum(1 for m in metrics if m.get("dedup_type") == "VARIACION"),
        "metrics": metrics,
    }
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=list), encoding="utf-8")
    return out


# ---------------------------------------------------------------------------
# CSV maestro
# ---------------------------------------------------------------------------

def write_master_csv(metrics: list[dict]) -> Path:
    """Escribe metrics_master.csv con campos aplanados."""
    out = OUTPUT_DIR / "metrics_master.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    with open(out, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=_MASTER_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for m in metrics:
            row = {k: _flatten_list(m.get(k, "")) for k in _MASTER_CSV_FIELDS}
            writer.writerow(row)
    return out


# ---------------------------------------------------------------------------
# Wiki Markdown
# ---------------------------------------------------------------------------

def write_wiki(metrics: list[dict]) -> Path:
    """
    Genera estructura de carpetas wiki/:
      wiki/README.md                    — índice global
      wiki/{coleccion}/README.md        — tabla comparativa por colección
    """
    wiki_dir = OUTPUT_DIR / "wiki"
    wiki_dir.mkdir(parents=True, exist_ok=True)

    by_col: dict[str, list[dict]] = defaultdict(list)
    for m in metrics:
        by_col[m.get("collection", "Sin Colección")].append(m)

    # Resumen global
    cert_total = sum(1 for m in metrics if m.get("certification_status") == "Certificada por Agente")
    rev_total  = sum(1 for m in metrics if m.get("certification_status") == "En Revisión")
    dep_total  = sum(1 for m in metrics if m.get("certification_status") == "Deprecada")
    pdf_validated_total = sum(1 for m in metrics if m.get("pdf_validated"))

    global_lines = [
        "---\ntitle: \"Diccionario de Métricas Vivo\"\n---\n\n",
        "# Diccionario de Métricas Vivo — Metabase\n\n",
        f"*Generado: {_NOW}*\n\n",
        f"| Indicador | Valor |\n",
        f"|-----------|-------|\n",
        f"| Total métricas | **{len(metrics)}** |\n",
        f"| Certificadas por Agente | {cert_total} |\n",
        f"| En Revisión | {rev_total} |\n",
        f"| Deprecadas | {dep_total} |\n",
        f"| Validadas contra PDF | {pdf_validated_total} |\n",
        f"| Colecciones | {len(by_col)} |\n\n",
        "## Colecciones\n\n",
        "| Colección | Métricas | Certificadas | Deprecadas |\n",
        "|-----------|----------|--------------|------------|\n",
    ]
    for col, col_metrics in sorted(by_col.items()):
        cert = sum(1 for m in col_metrics if m.get("certification_status") == "Certificada por Agente")
        dep  = sum(1 for m in col_metrics if m.get("certification_status") == "Deprecada")
        folder = _safe_folder(col)
        global_lines.append(
            f"| [{col}](./{folder}/README.md) | {len(col_metrics)} | {cert} | {dep} |\n"
        )

    (wiki_dir / "README.md").write_text("".join(global_lines), encoding="utf-8")

    # Por colección
    for col, col_metrics in by_col.items():
        col_dir = wiki_dir / _safe_folder(col)
        col_dir.mkdir(exist_ok=True)

        # Calcular tags únicos de la colección para frontmatter
        col_tags = sorted({t for m in col_metrics for t in (m.get("tags") or [])})
        cert_count = sum(1 for m in col_metrics if m.get("certification_status") == "Certificada por Agente")
        dep_count  = sum(1 for m in col_metrics if m.get("certification_status") == "Deprecada")

        frontmatter = (
            "---\n"
            f"title: \"{col}\"\n"
            f"tags:\n"
            + "".join(f"  - {t.lstrip('#')}\n" for t in col_tags[:10])
            + "---\n\n"
        )

        col_lines = [
            frontmatter,
            f"# {col}\n\n",
            f"*{len(col_metrics)} métricas — generado {_NOW}*\n\n",
            f"> **Certificadas:** {cert_count} &nbsp;|&nbsp; **Deprecadas:** {dep_count}\n\n",
            "| Métrica | Definición de Negocio | Tablas Origen | Estado | Tags |\n",
            "|---------|----------------------|---------------|--------|------|\n",
        ]
        for m in sorted(col_metrics, key=lambda x: -(x.get("vistas_primary") or 0)):
            name   = m.get("primary_card_name", "")[:60]
            cid    = m.get("primary_card_id", "")
            defn   = (m.get("business_definition") or "—")[:120].replace("|", "\\|")
            tables = ", ".join(sorted(m.get("tables_referenced") or []))[:60]
            status = m.get("certification_status", "?")
            tags   = " ".join(m.get("tags") or [])
            col_lines.append(
                f"| {name} (#{cid}) | {defn} | {tables} | {status} | {tags} |\n"
            )

        # Sección de variaciones
        variations = [m for m in col_metrics if m.get("dedup_type") == "VARIACION"]
        if variations:
            col_lines += [
                "\n## Variaciones detectadas\n\n",
                "Estas cards comparten >80% de estructura con su Métrica Madre.\n\n",
                "| Variación (ID) | Métrica Madre (ID) | Filtro diferenciador |\n",
                "|----------------|-------------------|----------------------|\n",
            ]
            for v in variations:
                col_lines.append(
                    f"| {v.get('primary_card_name','')[:50]} (#{v.get('primary_card_id')}) "
                    f"| #{v.get('parent_card_id', '?')} "
                    f"| {(v.get('where_summary') or '')[:80]} |\n"
                )

        (col_dir / "README.md").write_text("".join(col_lines), encoding="utf-8")

    return wiki_dir


# ---------------------------------------------------------------------------
# Reporte de Conflictos de Veracidad
# ---------------------------------------------------------------------------

def write_conflicts_report(conflicts: list[dict]) -> Path:
    """Escribe conflicts_report.json con métricas de nombre similar pero lógica contradictoria."""
    out = OUTPUT_DIR / "conflicts_report.json"
    out.write_text(
        json.dumps(
            {
                "generated_at": _NOW,
                "total_conflict_groups": len(conflicts),
                "description": (
                    "Grupos de métricas con nombres similares (primeras 4 palabras coinciden) "
                    "pero que referencian conjuntos de tablas distintos, lo que puede indicar "
                    "lógicas SQL contradictorias."
                ),
                "conflicts": conflicts,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return out


# ---------------------------------------------------------------------------
# Reporte de Mantenimiento
# ---------------------------------------------------------------------------

def write_maintenance_report(metrics: list[dict]) -> Path:
    """Escribe maintenance_report.md con secciones de Modificar, Descartar y Duplicados."""
    to_review    = [m for m in metrics if m.get("certification_status") == "En Revisión"]
    to_deprecate = [m for m in metrics if m.get("certification_status") == "Deprecada"]
    to_delete    = [
        m for m in metrics
        if m.get("dedup_type") == "DUPLICADO EXACTO" and not m.get("is_primary", True)
    ]

    lines = [
        "# Reporte de Mantenimiento del Catálogo\n\n",
        f"*Generado: {_NOW}*\n\n",
        "## Resumen\n\n",
        f"| Categoría | Cards |\n",
        f"|-----------|-------|\n",
        f"| Para Modificar (En Revisión) | {len(to_review)} |\n",
        f"| Para Descartar (Deprecadas) | {len(to_deprecate)} |\n",
        f"| Duplicados exactos a eliminar | {len(to_delete)} |\n\n",
        "---\n\n",
        "## Cards para Modificar\n\n",
        "Cards con hallazgos sin resolver o pendientes de revisión humana.\n\n",
        "| ID | Nombre | Score | Hallazgos | human_review_status | URL |\n",
        "|----|--------|-------|-----------|--------------------|---------|\n",
    ]
    for m in sorted(to_review, key=lambda x: x.get("score_salud", 100)):
        hcount = len(m.get("hallazgos") or [])
        altas  = sum(1 for h in (m.get("hallazgos") or []) if h.get("severidad") == "alta")
        lines.append(
            f"| {m['primary_card_id']} "
            f"| {(m.get('primary_card_name') or '')[:55]} "
            f"| {m.get('score_salud','?')} "
            f"| {hcount} ({altas} alta) "
            f"| {m.get('human_review_status') or 'null'} "
            f"| {m.get('url','')} |\n"
        )

    lines += [
        "\n---\n\n",
        "## Cards para Descartar\n\n",
        "Cards marcadas como Deprecadas por obsolescencia, inactividad o error confirmado.\n\n",
        "| ID | Nombre | Razón | Último Uso | Acción Sugerida |\n",
        "|----|--------|-------|------------|----------------|\n",
    ]
    for m in to_deprecate:
        reason   = m.get("uso_categoria") or m.get("dedup_type") or "?"
        last     = m.get("last_used_at") or "—"
        accion   = m.get("accion") or "ARCHIVAR"
        lines.append(
            f"| {m['primary_card_id']} "
            f"| {(m.get('primary_card_name') or '')[:55]} "
            f"| {reason} "
            f"| {last} "
            f"| {accion} |\n"
        )

    if to_delete:
        lines += [
            "\n---\n\n",
            "## Duplicados Exactos — Candidatos a Eliminar\n\n",
            "La card primaria (mayor vistas) se conserva. Las siguientes son redundantes.\n\n",
            "| ID Duplicado | Nombre | Card Primaria | Vistas |\n",
            "|--------------|--------|---------------|--------|\n",
        ]
        for m in to_delete:
            lines.append(
                f"| {m['primary_card_id']} "
                f"| {(m.get('primary_card_name') or '')[:55]} "
                f"| #{m.get('parent_card_id','?')} "
                f"| {m.get('vistas_primary',0)} |\n"
            )

    out = OUTPUT_DIR / "maintenance_report.md"
    out.write_text("".join(lines), encoding="utf-8")
    return out
