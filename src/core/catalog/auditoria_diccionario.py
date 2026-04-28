"""
src/core/catalog/auditoria_diccionario.py — Auditoría de valor del diccionario de métricas.

Aplica una rúbrica determinística (Mantener / Revisar / Eliminar) sobre cada
card de metrics_master.json usando señales ya computadas:
  - accion + uso_categoria (obsolescencia_cards.csv)
  - pdf_validated (validate_against_pdf.py)
  - dedup_type + is_primary (deduplication.py)
  - vistas_primary, last_used_at, dias_inactiva
  - creator + en_dashboards (obsolescencia_cards.csv)
  - unique_viewers_* (opcional — fetch_unique_viewers.py, puede estar NULL si OSS)

Produce:
  - data/processed/diccionario/auditoria/auditoria_detalle.csv
  - data/processed/diccionario/auditoria/auditoria_detalle.json
  - data/processed/diccionario/auditoria/auditoria_resumen.json
  - docs/auditoria_diccionario.md  (reporte narrativo, tono planeación interna)

Uso: python -m src.core.catalog.auditoria_diccionario
"""
from __future__ import annotations

import csv
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(_ROOT))

from src.core.catalog.build_glosario import assign_domain  # noqa: E402

METRICS_PATH = _ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
OBSOLESCENCIA_CSV = _ROOT / "data" / "raw" / "obsolescencia_cards.csv"
UNIQ_VIEWERS_CSV = _ROOT / "data" / "raw" / "metabase_unique_viewers.csv"

OUT_DIR = _ROOT / "data" / "processed" / "diccionario" / "auditoria"
OUT_CSV = OUT_DIR / "auditoria_detalle.csv"
OUT_JSON = OUT_DIR / "auditoria_detalle.json"
OUT_RESUMEN = OUT_DIR / "auditoria_resumen.json"
OUT_ARCHIVE_LIST = OUT_DIR / "to_archive_inmediato.json"
OUT_MD = _ROOT / "docs" / "auditoria_diccionario.md"

CLASIF_ORDER = ["Mantener", "Revisar", "Eliminar"]

ACCION_TO_CLASIF = {
    "MANTENER": "Mantener",
    "APLICAR_FIX": "Mantener",
    "REVISAR_ELIMINAR": "Revisar",
    "REVISAR_DASHBOARD": "Revisar",
    "REVISAR_CON_DUENO": "Revisar",
    "ARCHIVAR_TRAS_FIX": "Revisar",
    "ARCHIVAR": "Eliminar",
    "ELIMINAR": "Eliminar",
}

NAMING_PATTERNS = [
    re.compile(r"\bv\d+\b", re.IGNORECASE),
    re.compile(r"\b(?:duplicate|duplicado|copy|copia)\b", re.IGNORECASE),
    re.compile(r"\b(?:nuevo|new|old|antiguo|deprecated|viejo)\b", re.IGNORECASE),
    re.compile(r"\b(?:test|prueba|wip|draft|borrador)\b", re.IGNORECASE),
    re.compile(r"\((?:\d+|copy|old|new)\)", re.IGNORECASE),
]


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def load_metrics() -> list[dict]:
    if not METRICS_PATH.exists():
        print(f"ERROR: no existe {METRICS_PATH}")
        sys.exit(1)
    return json.loads(METRICS_PATH.read_text(encoding="utf-8")).get("metrics", [])


def load_obsolescencia() -> dict[int, dict]:
    d: dict[int, dict] = {}
    if not OBSOLESCENCIA_CSV.exists():
        return d
    with OBSOLESCENCIA_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                cid = int(row["card_id"])
            except (KeyError, TypeError, ValueError):
                continue
            d[cid] = row
    return d


def load_unique_viewers() -> dict[int, dict]:
    d: dict[int, dict] = {}
    if not UNIQ_VIEWERS_CSV.exists():
        return d

    def _int_or_none(v: str | None) -> int | None:
        if v in (None, "", "null"):
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    with UNIQ_VIEWERS_CSV.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                cid = int(row["card_id"])
            except (KeyError, TypeError, ValueError):
                continue
            d[cid] = {
                "u30": _int_or_none(row.get("unique_viewers_30d")),
                "u90": _int_or_none(row.get("unique_viewers_90d")),
                "ult": _int_or_none(row.get("unique_viewers_lifetime")),
                "t30": _int_or_none(row.get("total_views_30d")),
                "source": row.get("_source"),
            }
    return d


# ---------------------------------------------------------------------------
# Classification
# ---------------------------------------------------------------------------

def classify(metric: dict, obs_row: dict | None, uv_row: dict | None) -> tuple[str, list[str], str]:
    """Apply rubric. Returns (clasificacion, flags, justificacion)."""
    flags: list[str] = []
    justif: list[str] = []
    clasif: str | None = None

    pdf_validated = bool(metric.get("pdf_validated"))
    dedup_type = metric.get("dedup_type") or "UNICA"
    is_primary = bool(metric.get("is_primary", True))
    accion_csv = metric.get("accion") or ((obs_row or {}).get("accion") or "")
    vistas = int(metric.get("vistas_primary") or 0)
    uv_available = bool(uv_row and uv_row.get("source") not in (None, "", "unavailable"))
    u90 = uv_row.get("u90") if uv_row else None

    dias_inactiva: int | None = None
    en_dashboards = 0
    if obs_row:
        try:
            dias_inactiva = int(obs_row.get("dias_inactiva") or 0) or None
        except (TypeError, ValueError):
            dias_inactiva = None
        try:
            en_dashboards = int(obs_row.get("en_dashboards") or 0)
        except (TypeError, ValueError):
            en_dashboards = 0

    # --- Rule 1: PDF canonical (takes precedence, nunca eliminar) ---
    if pdf_validated:
        flags.append("canonica_pdf")
        justif.append("Canónica del PDF de training (nunca eliminar)")
        clasif = "Mantener"

    # --- Rule 2: Hard redundancy — duplicado exacto no-primary ---
    if dedup_type == "DUPLICADO EXACTO" and not is_primary and not pdf_validated:
        flags.append("duplicado_exacto_no_primary")
        parent = metric.get("parent_card_id")
        justif.append(
            f"Duplicado exacto de #{parent}" if parent else "Duplicado exacto de otra card"
        )
        clasif = "Eliminar"

    # --- Rule 3: Baseline per `accion` ---
    if clasif is None:
        mapped = ACCION_TO_CLASIF.get(accion_csv)
        if mapped:
            flags.append(f"accion_{accion_csv}")
            justif.append(f"Obsolescencia acción={accion_csv}")
            clasif = mapped

    # Default fallback if nothing matched
    if clasif is None:
        flags.append("sin_clasificacion_previa")
        justif.append("Sin señal previa — revisión manual")
        clasif = "Revisar"

    # --- Rule 4: Ghost metric (only if unique_viewers available + not canonical) ---
    if uv_available and u90 is not None and u90 <= 1 and not pdf_validated:
        flags.append("ghost_metric")
        justif.append(f"Solo {u90} viewer único en 90d (ghost)")
        clasif = "Eliminar"

    # --- Rule 5: Zombie ---
    is_zombie = (
        vistas <= 5
        and en_dashboards == 0
        and (dias_inactiva is not None and dias_inactiva > 180)
        and not pdf_validated
    )
    if uv_available and u90 is not None:
        is_zombie = is_zombie and u90 <= 1

    if is_zombie:
        flags.append("zombie")
        justif.append(
            f"Zombie: {vistas}v · 0 dashboards · {dias_inactiva}d inactiva"
        )
        if clasif == "Revisar":
            clasif = "Eliminar"
        elif clasif == "Mantener" and not pdf_validated:
            clasif = "Revisar"

    # --- Rule 6: Deprecada (certification_status override para señalizar Revisar) ---
    if metric.get("certification_status") == "Deprecada" and clasif == "Mantener" and not pdf_validated:
        flags.append("deprecada_por_agente")
        justif.append("Marcada Deprecada por el pipeline del diccionario")
        clasif = "Revisar"

    return clasif, flags, " · ".join(justif) if justif else "Sin señales"


# ---------------------------------------------------------------------------
# Bonus pattern detection
# ---------------------------------------------------------------------------

def detect_naming_inconsistency(row: dict) -> list[str]:
    """Detecta sufijos/prefijos de naming inconsistente en el nombre."""
    name = row.get("primary_card_name") or ""
    hits = []
    for pat in NAMING_PATTERNS:
        if pat.search(name):
            hits.append(pat.pattern)
    return hits


def group_similar_names(rows: list[dict]) -> list[list[dict]]:
    """Agrupa cards cuyos nombres colapsan al normalizarlos (strip v1/v2/copy/etc)."""
    def _normalize(name: str) -> str:
        n = name.lower()
        for pat in NAMING_PATTERNS:
            n = pat.sub("", n)
        n = re.sub(r"\s+", " ", n).strip(" -_()[]")
        return n

    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        key = _normalize(r.get("primary_card_name") or "")
        if not key or len(key) < 6:
            continue
        buckets[key].append(r)

    return [b for b in buckets.values() if len(b) > 1]


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def build_audit_rows(metrics: list[dict], obs: dict[int, dict], uv: dict[int, dict]) -> list[dict]:
    rows: list[dict] = []
    for m in metrics:
        cid = m.get("primary_card_id")
        if cid is None:
            continue
        obs_row = obs.get(int(cid))
        uv_row = uv.get(int(cid))
        clasif, flags, justif = classify(m, obs_row, uv_row)

        naming_hits = detect_naming_inconsistency(m)
        if naming_hits:
            flags.append("naming_inconsistente")

        en_dash = 0
        creator = ""
        dias = None
        if obs_row:
            try:
                en_dash = int(obs_row.get("en_dashboards") or 0)
            except (TypeError, ValueError):
                en_dash = 0
            creator = obs_row.get("creator") or ""
            try:
                dias = int(obs_row.get("dias_inactiva") or 0) or None
            except (TypeError, ValueError):
                dias = None

        dominio = assign_domain(m.get("tags") or [])

        rows.append({
            "primary_card_id": cid,
            "primary_card_name": m.get("primary_card_name") or "",
            "dominio": dominio,
            "collection": m.get("collection") or "",
            "creator": creator,
            "vistas_primary": int(m.get("vistas_primary") or 0),
            "unique_viewers_30d": (uv_row or {}).get("u30"),
            "unique_viewers_90d": (uv_row or {}).get("u90"),
            "unique_viewers_lifetime": (uv_row or {}).get("ult"),
            "last_used_at": m.get("last_used_at") or "",
            "dias_inactiva": dias,
            "en_dashboards": en_dash,
            "dedup_type": m.get("dedup_type") or "UNICA",
            "is_primary": bool(m.get("is_primary", True)),
            "parent_card_id": m.get("parent_card_id"),
            "duplicate_card_ids_count": len(m.get("duplicate_card_ids") or []),
            "certification_status": m.get("certification_status") or "",
            "pdf_validated": bool(m.get("pdf_validated")),
            "uso_categoria": m.get("uso_categoria") or "",
            "accion_csv": m.get("accion") or (obs_row or {}).get("accion") or "",
            "clasificacion": clasif,
            "justificacion": justif,
            "flags": ";".join(flags),
            "tags": ";".join(m.get("tags") or []),
            "url": m.get("url") or "",
        })
    return rows


def build_resumen(rows: list[dict], uv_available: bool) -> dict:
    counts = Counter(r["clasificacion"] for r in rows)
    total = len(rows)
    by_domain = defaultdict(lambda: defaultdict(int))
    for r in rows:
        by_domain[r["dominio"]][r["clasificacion"]] += 1

    flags_counter: Counter = Counter()
    for r in rows:
        if r["flags"]:
            for fl in r["flags"].split(";"):
                flags_counter[fl] += 1

    # Bonus patterns
    never_adopted = [r for r in rows if r["vistas_primary"] == 0 and r["en_dashboards"] == 0 and not r["pdf_validated"]]
    analytical_noise = [
        r for r in rows
        if r["uso_categoria"] == "INACTIVA_12M"
        and r["accion_csv"] in ("ELIMINAR", "ARCHIVAR")
        and r["en_dashboards"] == 0
    ]

    # Creator concentration — aggregate, no shaming
    creator_counts: Counter = Counter()
    creator_low_value: dict[str, int] = defaultdict(int)
    for r in rows:
        if r["creator"]:
            creator_counts[r["creator"]] += 1
            if r["clasificacion"] == "Eliminar":
                creator_low_value[r["creator"]] += 1
    creator_depend = [
        {"creator": c, "total_cards": n, "cards_eliminar": creator_low_value.get(c, 0)}
        for c, n in creator_counts.most_common()
        if n >= 30
    ]

    return {
        "total_cards": total,
        "uv_available": uv_available,
        "counts": dict(counts),
        "pct": {k: round(100 * v / total, 1) for k, v in counts.items()} if total else {},
        "by_domain": {d: dict(v) for d, v in by_domain.items()},
        "flags": dict(flags_counter.most_common()),
        "bonus": {
            "never_adopted_count": len(never_adopted),
            "analytical_noise_count": len(analytical_noise),
            "creators_con_mas_de_30_cards": creator_depend,
        },
    }


def write_csv(rows: list[dict]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if not rows:
        OUT_CSV.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_json(rows: list[dict]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(rows, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def write_resumen(resumen: dict) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_RESUMEN.write_text(
        json.dumps(resumen, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def write_archive_list(rows: list[dict]) -> list[int]:
    """Lista de IDs ya accionables para archivar_cards.py."""
    ids = [
        r["primary_card_id"]
        for r in rows
        if r["clasificacion"] == "Eliminar"
        and not r["pdf_validated"]
    ]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_ARCHIVE_LIST.write_text(
        json.dumps({
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "total": len(ids),
            "card_ids": ids,
            "uso": "python -m src.core.remediation.archivar_cards --ids-file data/processed/diccionario/auditoria/to_archive_inmediato.json --dry-run",
        }, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return ids


# ---------------------------------------------------------------------------
# MD narrative
# ---------------------------------------------------------------------------

def _pct(n: int, total: int) -> str:
    return f"{100 * n / total:.1f}%" if total else "0%"


def build_md(rows: list[dict], resumen: dict, metrics: list[dict], archive_ids: list[int]) -> str:
    total = resumen["total_cards"]
    counts = resumen["counts"]
    uv_ok = resumen["uv_available"]
    gen_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    L: list[str] = []
    L.append("# Auditoría del Diccionario de Métricas")
    L.append(
        f"**Generado:** {gen_at} (S61) · **Audiencia:** author (planeación interna) · "
        f"**Fuente:** `data/processed/diccionario/metrics_master.json`"
    )
    L.append("")
    if not uv_ok:
        L.append("> ⚠ **Limitación de data**: el endpoint `view_log` de Metabase no está disponible")
        L.append("> en esta instancia (OSS sin Pro/Enterprise). Los criterios de **\"usuarios únicos\"**")
        L.append("> y **\"ghost metric\"** no pudieron computarse; el análisis se apoya en vistas")
        L.append("> agregadas, `dias_inactiva` y membresía en dashboards. Ver §6.2.")
        L.append("")

    # 1. Resumen ejecutivo
    L.append("## 1. Resumen ejecutivo")
    L.append("")
    L.append(f"De **{total:,}** cards auditadas:")
    L.append("")
    L.append("| Clasificación | Count | % |")
    L.append("|---|---:|---:|")
    for c in CLASIF_ORDER:
        n = counts.get(c, 0)
        icon = {"Mantener": "✅", "Revisar": "⚠", "Eliminar": "❌"}[c]
        L.append(f"| {icon} **{c}** | {n:,} | {_pct(n, total)} |")
    L.append("")

    # Top problemas
    flags = resumen["flags"]
    top_issues = []
    if flags.get("duplicado_exacto_no_primary"):
        top_issues.append(f"**{flags['duplicado_exacto_no_primary']:,} duplicados exactos no-primary** — acción directa: archivar")
    if resumen["bonus"]["never_adopted_count"]:
        top_issues.append(f"**{resumen['bonus']['never_adopted_count']:,} cards nunca adoptadas** (0 vistas · 0 dashboards)")
    if resumen["bonus"]["analytical_noise_count"]:
        top_issues.append(f"**{resumen['bonus']['analytical_noise_count']:,} cards de ruido analítico** (INACTIVA_12M + ELIMINAR + 0 dashboards)")
    naming_n = flags.get("naming_inconsistente", 0)
    if naming_n:
        top_issues.append(f"**{naming_n:,} cards con naming inconsistente** (v1/v2/copy/duplicate/nuevo)")
    depr = flags.get("deprecada_por_agente", 0)
    if depr:
        top_issues.append(f"**{depr:,} cards Deprecadas por el agente** pero aún no archivadas")

    if top_issues:
        L.append("**Top problemas detectados:**")
        L.append("")
        for i, issue in enumerate(top_issues, 1):
            L.append(f"{i}. {issue}")
        L.append("")

    # Impacto
    mantener_n = counts.get("Mantener", 0)
    L.append(
        f"**Impacto estimado:** ejecutar todas las acciones `Eliminar` reduce el diccionario "
        f"de **{total:,} → {mantener_n + counts.get('Revisar', 0):,}** cards "
        f"(-{_pct(counts.get('Eliminar', 0), total)})."
    )
    L.append("")

    # 2. Tabla de auditoría
    L.append("## 2. Tabla de auditoría")
    L.append("")
    L.append("### 2.1 Por dominio × clasificación")
    L.append("")
    L.append("| Dominio | Mantener | Revisar | Eliminar | Total |")
    L.append("|---|---:|---:|---:|---:|")
    by_dom = resumen["by_domain"]
    for dom in ["Negocio", "Producto", "Ops", "General"]:
        d = by_dom.get(dom, {})
        tot = sum(d.values())
        L.append(
            f"| {dom} | {d.get('Mantener', 0):,} | {d.get('Revisar', 0):,} "
            f"| {d.get('Eliminar', 0):,} | {tot:,} |"
        )
    L.append("")

    L.append("### 2.2 Por flag disparado")
    L.append("")
    L.append("| Flag | Count | Efecto |")
    L.append("|---|---:|---|")
    flag_desc = {
        "canonica_pdf": "Mantener forzado",
        "duplicado_exacto_no_primary": "Eliminar",
        "ghost_metric": "Eliminar (≤1 viewer 90d)",
        "zombie": "Downgrade (poco uso + antiguo + sin dashboard)",
        "deprecada_por_agente": "Revisar",
        "naming_inconsistente": "Bandera de redefinición",
        "sin_clasificacion_previa": "Revisar (default)",
    }
    # incluir accion_* flags también
    for fl, n in sorted(flags.items(), key=lambda kv: -kv[1]):
        if fl.startswith("accion_"):
            flag_desc.setdefault(fl, f"Baseline: {fl.replace('accion_','')}")
    for fl, n in sorted(flags.items(), key=lambda kv: -kv[1])[:15]:
        L.append(f"| `{fl}` | {n:,} | {flag_desc.get(fl, '—')} |")
    L.append("")
    L.append(
        f"**Detalle completo:** `data/processed/diccionario/auditoria/auditoria_detalle.csv` "
        f"({total:,} filas)."
    )
    L.append("")

    # 3. Recomendaciones
    L.append("## 3. Recomendaciones")
    L.append("")

    # 3.1 Eliminar inmediato
    L.append("### 3.1 Eliminar inmediato")
    L.append("")
    L.append(f"**{len(archive_ids):,} cards** listas para archivar (no canónicas, clasificadas `Eliminar`).")
    L.append("")
    L.append("**Comando sugerido (dry-run primero, pedir OK data_owner):**")
    L.append("")
    L.append("```bash")
    L.append("python -m src.core.remediation.archivar_cards \\")
    L.append("    --ids-file data/processed/diccionario/auditoria/to_archive_inmediato.json \\")
    L.append("    --dry-run")
    L.append("```")
    L.append("")
    L.append("**Top 10 ejemplos** (por vistas descendentes):")
    top_elim = sorted(
        [r for r in rows if r["clasificacion"] == "Eliminar" and not r["pdf_validated"]],
        key=lambda r: -r["vistas_primary"]
    )[:10]
    L.append("")
    L.append("| Card | Nombre | Vistas | Razón |")
    L.append("|---:|---|---:|---|")
    for r in top_elim:
        name = (r["primary_card_name"] or "").replace("|", "\\|")[:60]
        razon = (r["justificacion"] or "").replace("|", "\\|")[:80]
        L.append(f"| #{r['primary_card_id']} | {name} | {r['vistas_primary']:,} | {razon} |")
    L.append("")

    # 3.2 Consolidar duplicados
    L.append("### 3.2 Consolidar duplicados")
    L.append("")
    dup_primaries = defaultdict(list)
    for m in metrics:
        n_dups = len(m.get("duplicate_card_ids") or [])
        if m.get("is_primary") and n_dups > 0:
            dup_primaries[m.get("primary_card_id")].append({
                "name": m.get("primary_card_name"),
                "n_dups": n_dups,
                "vistas": int(m.get("vistas_primary") or 0),
            })
    L.append(f"**Top 20 grupos de duplicados** (del total de {len(dup_primaries):,} primaries con copias):")
    L.append("")
    L.append("| Primary | Nombre | # duplicados | Vistas primary |")
    L.append("|---:|---|---:|---:|")
    pid_sorted = sorted(dup_primaries.items(), key=lambda kv: -kv[1][0]["n_dups"])[:20]
    for pid, recs in pid_sorted:
        r = recs[0]
        name = (r["name"] or "").replace("|", "\\|")[:60]
        L.append(f"| #{pid} | {name} | {r['n_dups']} | {r['vistas']:,} |")
    L.append("")

    # 3.3 Redefinir (naming)
    L.append("### 3.3 Redefinir (naming inconsistente)")
    L.append("")
    similar_groups = group_similar_names(
        [m for m in metrics if m.get("is_primary") and m.get("certification_status") != "Deprecada"]
    )
    similar_groups.sort(key=len, reverse=True)
    L.append(f"**{len(similar_groups)} grupos** de cards con nombres casi-idénticos tras remover sufijos (v1/v2/copy/duplicate/nuevo).")
    L.append("")
    if similar_groups:
        L.append("**Top 10 grupos con más variantes:**")
        L.append("")
        for g in similar_groups[:10]:
            names = sorted({m.get("primary_card_name") for m in g})
            top_view = max(g, key=lambda x: int(x.get("vistas_primary") or 0))
            L.append(
                f"- **{len(g)} primaries · {len(names)} nombres distintos** · "
                f"canónica sugerida: #{top_view.get('primary_card_id')} "
                f"({int(top_view.get('vistas_primary') or 0):,}v) — ejemplos:"
            )
            for n in list(names)[:3]:
                L.append(f"  - `{n}`")
            if len(names) > 3:
                L.append(f"  - _… y {len(names) - 3} nombres más_")
    L.append("")

    # 3.4 Métricas faltantes
    L.append("### 3.4 Métricas faltantes / gaps")
    L.append("")
    L.append("Gaps ya documentados por `validate_against_pdf.py`:")
    L.append("")
    L.append("- **CSAT** — `fuera_de_scope`: pertenece a customer support, no está en Metabase.")
    L.append("  No requiere acción; registrado como gap documentado.")
    L.append("- **FX Revenue** — `sin_card_canonica`: PDF pide `counter_auth >= 2`; ninguna")
    L.append("  de las 4 candidatas (#169 · #4 · #3070 · #3240) cumple estrictamente.")
    L.append("  **Acción sugerida**: elegir una canónica con data_owner o crear nueva card.")
    L.append("")

    # 4. Nueva propuesta
    L.append("## 4. Nueva propuesta de diccionario")
    L.append("")
    post_clean = mantener_n
    L.append(
        f"Post-limpieza (subset `Mantener`): **{post_clean:,} cards** "
        f"(reducción de {_pct(total - post_clean, total)})."
    )
    L.append("")

    # 4.1 — Exclusiones aplicadas al diccionario online
    L.append("### 4.1 Exclusiones aplicadas a `docs/diccionario.html`")
    L.append("")
    L.append(
        "La web app del diccionario (`build_diccionario_app.py`) ahora lee "
        "`auditoria_detalle.json` y filtra 4 categorías antes de renderizar. "
        "Cada regla tiene su justificación."
    )
    L.append("")
    L.append("| # | Categoría | Cards | Regla | Por qué se oculta |")
    L.append("|---|---|---:|---|---|")
    L.append(
        f"| 1 | **Duplicados exactos no-primary** | {flags.get('duplicado_exacto_no_primary', 0):,} "
        "| `dedup_type=DUPLICADO EXACTO` AND `is_primary=false` "
        "| Mismo SQL-fingerprint que una primary; exponer ambas en el diccionario genera ambigüedad sin aportar info. La primary es suficiente. |"
    )
    L.append(
        f"| 2 | **Nunca adoptadas** | {resumen['bonus']['never_adopted_count']:,} "
        "| `vistas_primary=0` AND `en_dashboards=0` AND no canónica PDF "
        "| Nadie las abrió ni las usó en un dashboard. Mostrarlas como \"recomendadas\" induce a consumir métricas no validadas por la organización. |"
    )
    L.append(
        f"| 3 | **Ruido analítico** | {resumen['bonus']['analytical_noise_count']:,} "
        "| `uso_categoria=INACTIVA_12M` AND `accion IN (ELIMINAR, ARCHIVAR)` AND `en_dashboards=0` "
        "| Triple señal de obsolescencia: inactiva 12+ meses, ya marcada para eliminar, sin presencia en dashboards. Exponerlas contradice su estado real. |"
    )
    L.append(
        f"| 4 | **Grupos de naming inconsistente** | {flags.get('naming_inconsistente', 0):,} cards en {len(group_similar_names([m for m in metrics if m.get('is_primary') and m.get('certification_status') != 'Deprecada']))} grupos "
        "| Nombres que colapsan al remover sufijos `v1/v2/copy/duplicate/new/test` "
        "| Para cada grupo se mantiene **sólo la canónica** (top vistas, PDF-validated si existe). Mostrar todas las variantes obliga al consumidor a elegir sin data. |"
    )
    L.append("")
    L.append("**Reglas de protección:**")
    L.append("")
    L.append("- Las **canónicas del PDF Training** (`pdf_validated=true`) nunca se ocultan, incluso si califican para alguna regla de exclusión.")
    L.append("- La regla 1 ya estaba implícita vía `is_active()` (hereda `is_primary`); las reglas 2, 3 y 4 son **nuevas** en `build_diccionario_app.py` y se aplican solo si `auditoria_detalle.json` existe — si no existe, el script degrada al comportamiento anterior.")
    L.append("")
    L.append("**Impacto medido** (ejecución 2026-04-24):")
    L.append("")
    L.append("| | Antes (S60) | Después (S61) | Δ |")
    L.append("|---|---:|---:|---:|")
    L.append("| Métricas distintas en `docs/diccionario.html` | 1,071 | 1,043 | -28 |")
    L.append("| Cards activas cubiertas | 1,627 | 1,383 | -244 |")
    L.append("")
    L.append(
        "La reducción de **métricas** es modesta (-28) porque la mayoría de las "
        "4 categorías ya estaban filtradas aguas arriba por `is_active()` (que excluye "
        "non-primary, Deprecadas, y `accion∈{ARCHIVAR,ELIMINAR,REVISAR_ELIMINAR}`). Las "
        "reglas nuevas capturan principalmente cards con `accion=MANTENER` pero sin uso "
        "real (las 228 \"nunca adoptadas\" que se colaban antes) y las variantes de "
        "naming que antes sobrevivían como entradas duplicadas en el UI."
    )
    L.append("")
    L.append("")
    L.append("**Distribución por dominio post-limpieza:**")
    L.append("")
    L.append("| Dominio | Mantener | % del nuevo diccionario |")
    L.append("|---|---:|---:|")
    for dom in ["Negocio", "Producto", "Ops", "General"]:
        n = by_dom.get(dom, {}).get("Mantener", 0)
        L.append(f"| {dom} | {n:,} | {_pct(n, post_clean)} |")
    L.append("")
    L.append("**Próximo paso sugerido:**")
    L.append("")
    L.append("1. Ejecutar `archivar_cards.py` sobre `to_archive_inmediato.json` (tras OK data_owner)")
    L.append("2. Resolver los **grupos de duplicados** (§3.2) con la primary sugerida")
    L.append("3. Cerrar FX Revenue con data_owner (§3.4)")
    L.append("4. Re-correr `pipeline.py` → regenera `metrics_master.json` limpio")
    L.append(f"5. Re-correr `build_diccionario_app.py` → `docs/diccionario.html` queda con ~{post_clean:,} entradas")
    L.append("")

    # 5. Patrones bonus
    L.append("## 5. Patrones bonus")
    L.append("")
    bn = resumen["bonus"]
    L.append(f"- **Nunca adoptada** (0 vistas · 0 dashboards): **{bn['never_adopted_count']:,}** cards")
    L.append(f"- **Ruido analítico** (INACTIVA_12M + ELIMINAR + 0 dashboards): **{bn['analytical_noise_count']:,}** cards")
    L.append(f"- **Naming inconsistente**: **{flags.get('naming_inconsistente', 0):,}** cards con sufijos v1/v2/copy/duplicate/nuevo")
    L.append(f"- **Concentración por creador** (≥30 cards): **{len(bn['creators_con_mas_de_30_cards'])}** creators")
    L.append("")

    # 6. Apéndice
    L.append("## 6. Apéndice técnico")
    L.append("")
    L.append("### 6.1 Rúbrica aplicada (orden de overrides)")
    L.append("")
    L.append("1. **Canónica PDF** (`pdf_validated=true`) → `Mantener` (nunca eliminar)")
    L.append("2. **Duplicado exacto no-primary** (`dedup_type=DUPLICADO EXACTO` + `is_primary=false`) → `Eliminar`")
    L.append("3. **Baseline por `accion`** de `obsolescencia_cards.csv`:")
    L.append("   - `MANTENER`/`APLICAR_FIX` → `Mantener`")
    L.append("   - `REVISAR_*`/`ARCHIVAR_TRAS_FIX` → `Revisar`")
    L.append("   - `ARCHIVAR`/`ELIMINAR` → `Eliminar`")
    L.append("4. **Ghost metric** (si `unique_viewers_90d` disponible): `u90 ≤ 1` AND no canónica → `Eliminar`")
    L.append("5. **Zombie**: `vistas ≤ 5` AND `en_dashboards=0` AND `dias_inactiva > 180` AND no canónica → downgrade")
    L.append("6. **Deprecada por agente** (sin override previo) → `Revisar`")
    L.append("")

    L.append("### 6.2 Estado del endpoint Metabase")
    L.append("")
    if uv_ok:
        L.append("- `GET /api/database`: OK, Audit DB encontrada.")
        L.append("- SQL `SELECT COUNT(DISTINCT user_id) FROM view_log WHERE model='card'`: OK.")
        L.append("- Ventanas extraídas: 30d / 90d / lifetime.")
    else:
        L.append("- `GET /api/database`: OK, 3 DBs expuestas — ninguna con flag `is_audit=True`.")
        L.append("- Metabase OSS no expone `view_log` por defecto (feature Pro/Enterprise).")
        L.append("- Para habilitar: contratar \"Usage analytics\" o exportar `view_log` desde la app DB.")
        L.append(f"- CSV con nulls: `{UNIQ_VIEWERS_CSV.relative_to(_ROOT)}` (4,007 filas, `_source=unavailable`).")
    L.append("")

    L.append("### 6.3 Creators con ≥30 cards (coordinación de limpieza)")
    L.append("")
    L.append("_Lista para author: con quién coordinar batch de limpieza por creador. No para dashboards públicos._")
    L.append("")
    if bn["creators_con_mas_de_30_cards"]:
        L.append("| Creator | Total cards | Cards Eliminar |")
        L.append("|---|---:|---:|")
        for c in bn["creators_con_mas_de_30_cards"]:
            L.append(f"| {c['creator']} | {c['total_cards']:,} | {c['cards_eliminar']:,} |")
    else:
        L.append("_No se detectaron creators con ≥30 cards._")
    L.append("")

    L.append("### 6.4 Limitaciones conocidas")
    L.append("")
    if not uv_ok:
        L.append("- **Usuarios únicos no disponibles** (ver §6.2). Reglas 4 (ghost) no se aplicó.")
    L.append("- La rúbrica privilegia `accion` del CSV de obsolescencia; si ese CSV está desactualizado")
    L.append("  respecto a la realidad actual, el output también.")
    L.append("- La detección de naming inconsistente es por regex; puede haber falsos positivos")
    L.append("  (p.ej. \"vista mensual\" matchea `\\bv\\d+\\b` falsamente — se mitiga por el `\\b`).")
    L.append("")

    return "\n".join(L) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 60)
    print("  auditoria_diccionario — rúbrica de valor + entregables")
    print("=" * 60)

    metrics = load_metrics()
    obs = load_obsolescencia()
    uv = load_unique_viewers()
    uv_available = any((row or {}).get("u90") is not None for row in uv.values())

    print(f"Cards cargadas:   {len(metrics):,}")
    print(f"Obsolescencia:    {len(obs):,} filas")
    print(f"Unique viewers:   {'disponible' if uv_available else 'NO DISPONIBLE (fallback)'}")
    print()

    rows = build_audit_rows(metrics, obs, uv)
    resumen = build_resumen(rows, uv_available)

    print("Distribución:")
    for c in CLASIF_ORDER:
        n = resumen["counts"].get(c, 0)
        print(f"  {c:10s}: {n:>6,}  ({_pct(n, resumen['total_cards'])})")
    print()

    archive_ids = write_archive_list(rows)
    write_csv(rows)
    write_json(rows)
    write_resumen(resumen)

    md = build_md(rows, resumen, metrics, archive_ids)
    OUT_MD.write_text(md, encoding="utf-8")

    print(f"Archive list:     {OUT_ARCHIVE_LIST.relative_to(_ROOT)} ({len(archive_ids):,} ids)")
    print(f"Detalle CSV:      {OUT_CSV.relative_to(_ROOT)} ({len(rows):,} filas)")
    print(f"Detalle JSON:     {OUT_JSON.relative_to(_ROOT)}")
    print(f"Resumen JSON:     {OUT_RESUMEN.relative_to(_ROOT)}")
    print(f"Reporte MD:       {OUT_MD.relative_to(_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
