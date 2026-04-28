"""Genera docs/diccionario.html — web app single-file para consulta rapida de metricas.

Entregable D4 del backlog (docs/mejoras.md): diccionario interactivo con
buscador en tiempo real, filtros de dominio y panel de detalle, pensado
para consumo de Product Managers y equipos no-tecnicos.

Output: un unico archivo HTML con CSS + JS + datos embebidos. No requiere
servidor ni build. Abrir con doble-click en cualquier navegador.

Invocar: python -m src.core.catalog.build_diccionario_app
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

from src.core.catalog.build_glosario import (
    assign_domain,
    build_tldr,
    is_active,
)
from src.core.catalog.auditoria_diccionario import NAMING_PATTERNS

ROOT = Path(__file__).resolve().parents[3]
METRICS_PATH = ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
AUDIT_PATH = ROOT / "data" / "processed" / "diccionario" / "auditoria" / "auditoria_detalle.json"
POPULARITY_PATH = ROOT / "data" / "processed" / "diccionario" / "popularity_ranked.json"
OUTPUT_PATH = ROOT / "docs" / "diccionario.html"


def _load_popularity() -> dict | None:
    """Carga popularity_ranked.json si existe. None si no se ha corrido snapshot_popularity."""
    if not POPULARITY_PATH.exists():
        return None
    try:
        return json.loads(POPULARITY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return None


def _popularity_lookup(popularity: dict | None) -> dict[int, dict]:
    """Mapea card_id → {d7, d30, lu} para inyectar en cada entry (para sort/filtros)."""
    out: dict[int, dict] = {}
    if not popularity:
        return out
    for win_key, field in (("window_7d", "d7"), ("window_30d", "d30")):
        win = popularity.get(win_key) or {}
        for r in win.get("top", []) or []:
            cid = r.get("card_id")
            if cid is None:
                continue
            out.setdefault(int(cid), {})[field] = int(r.get("delta") or 0)
            if r.get("last_used_at"):
                out[int(cid)].setdefault("lu", r["last_used_at"])
    # Complementar last_used desde top_lifetime (degraded mode)
    for r in popularity.get("top_lifetime") or []:
        cid = r.get("card_id")
        if cid is None:
            continue
        entry = out.setdefault(int(cid), {})
        if r.get("last_used_at") and "lu" not in entry:
            entry["lu"] = r["last_used_at"]
    return out


def _load_audit_index() -> dict[int, dict]:
    """Carga auditoria_detalle.json → dict keyed by primary_card_id. Vacío si no existe."""
    if not AUDIT_PATH.exists():
        return {}
    rows = json.loads(AUDIT_PATH.read_text(encoding="utf-8"))
    return {int(r["primary_card_id"]): r for r in rows if r.get("primary_card_id") is not None}


def _excluded_by_audit(rec: dict, audit_row: dict | None) -> str | None:
    """Devuelve razón de exclusión (para telemetría) o None si el rec debe mostrarse.

    Reglas (en orden; canónicas PDF siempre pasan):
      1. duplicado exacto no-primary    → "duplicado_exacto"
      2. nunca adoptada (0v + 0 dash)   → "nunca_adoptada"
      3. ruido analítico (INACTIVA_12M + ELIMINAR|ARCHIVAR + 0 dashboards) → "ruido_analitico"
    """
    if rec.get("pdf_validated"):
        return None

    # Rule 1: duplicado exacto no-primary (estructural, no depende de audit)
    if rec.get("dedup_type") == "DUPLICADO EXACTO" and not rec.get("is_primary", True):
        return "duplicado_exacto"

    if not audit_row:
        return None

    en_dash = 0
    try:
        en_dash = int(audit_row.get("en_dashboards") or 0)
    except (TypeError, ValueError):
        en_dash = 0
    vistas = int(rec.get("vistas_primary") or 0)
    accion = audit_row.get("accion_csv") or ""
    uso = audit_row.get("uso_categoria") or ""

    # Rule 2: nunca adoptada (0v + 0 dashboards)
    if vistas == 0 and en_dash == 0:
        return "nunca_adoptada"

    # Rule 3: ruido analítico
    if uso == "INACTIVA_12M" and accion in ("ELIMINAR", "ARCHIVAR") and en_dash == 0:
        return "ruido_analitico"

    return None


def _normalize_for_naming_group(name: str) -> str:
    """Colapsa v1/v2/copy/duplicate/new/test/etc para agrupar variantes de un mismo concepto."""
    n = (name or "").lower()
    for pat in NAMING_PATTERNS:
        n = pat.sub("", n)
    return re.sub(r"\s+", " ", n).strip(" -_()[]")


def collect_entries(metrics: list[dict]) -> tuple[list[dict], dict[str, int]]:
    """Retorna (entries, exclusion_counts) — aplica filtros de auditoría."""
    audit_idx = _load_audit_index()
    exclusion_counts: dict[str, int] = defaultdict(int)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for rec in metrics:
        if not is_active(rec):
            exclusion_counts["is_active=False"] += 1
            continue
        reason = _excluded_by_audit(rec, audit_idx.get(int(rec.get("primary_card_id") or 0)))
        if reason:
            exclusion_counts[reason] += 1
            continue
        name = (rec.get("primary_card_name") or "").strip()
        if not name:
            continue
        grouped[name.lower()].append(rec)

    # Stage 1: colapsar por nombre exacto (existing pattern)
    intermediate: list[dict] = []
    for recs in grouped.values():
        recs.sort(key=lambda r: r.get("vistas_primary") or 0, reverse=True)
        rep = recs[0]
        merged_tags = []
        seen = set()
        for r in recs:
            for t in (r.get("tags") or []):
                if t not in seen:
                    seen.add(t)
                    merged_tags.append(t)
        audit = rep.get("definition_audit") or {}
        audit_reason = ""
        if audit.get("status") == "pending_review":
            audit_reason = (audit.get("reason") or "").strip()
        intermediate.append({
            "n": (rep.get("primary_card_name") or "").strip(),
            "d": assign_domain(merged_tags),
            "t": build_tldr(rep.get("business_definition")),
            "b": (rep.get("business_definition") or "").strip(),
            "tt": (rep.get("technical_translation") or "").strip(),
            "c": rep.get("primary_card_id"),
            "u": rep.get("url") or "",
            "v": rep.get("vistas_primary") or 0,
            "col": rep.get("collection") or "",
            "tags": merged_tags,
            "copies": len(recs) - 1,
            "cs": rep.get("certification_status") or "",
            "pv": bool(rep.get("pdf_validated")),
            "ar": audit_reason,
        })

    # Stage 2: colapsar grupos de naming inconsistente (v1/v2/copy/duplicate/etc)
    # Solo aplica a buckets de normalized name con ≥2 entradas Y nombre normalizado ≥6 chars.
    # Keep canónica (top-views) por grupo; descarta variantes.
    entries: list[dict] = []
    by_norm: dict[str, list[dict]] = defaultdict(list)
    passthrough: list[dict] = []
    for e in intermediate:
        key = _normalize_for_naming_group(e["n"])
        if not key or len(key) < 6:
            passthrough.append(e)
        else:
            by_norm[key].append(e)

    for group in by_norm.values():
        if len(group) == 1:
            entries.append(group[0])
            continue
        # Protege canónicas PDF: si hay al menos una PDF-validated, esa es la canónica
        group.sort(key=lambda e: (-int(e.get("pv", False)), -(e.get("v") or 0)))
        keeper = group[0]
        entries.append(keeper)
        dropped = len(group) - 1
        exclusion_counts["naming_group_variant"] += dropped

    entries.extend(passthrough)
    entries.sort(key=lambda e: e["n"].lower())
    return entries, dict(exclusion_counts)


HTML_TEMPLATE = """<!doctype html>
<html lang="es" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Diccionario de Metricas &middot; ExampleCorp</title>
<style>
  :root {{
    --color-bg: #FAFBFC;
    --color-surface: #FFFFFF;
    --color-surface-alt: #F4F6F8;
    --color-surface-soft: #F8FAFC;
    --color-border: #E5E8EC;
    --color-border-strong: #CDD3DA;
    --color-text: #0F172A;
    --color-text-secondary: #475569;
    --color-text-muted: #94A3B8;
    --color-accent: #4F46E5;
    --color-accent-soft: #EEF2FF;
    --color-accent-strong: #4338CA;
    --color-accent-ring: rgba(79, 70, 229, 0.18);

    --color-domain-negocio: #10B981;
    --color-domain-producto: #3B82F6;
    --color-domain-ops: #F59E0B;
    --color-domain-general: #94A3B8;

    --color-domain-negocio-soft: #ECFDF5;
    --color-domain-producto-soft: #EFF6FF;
    --color-domain-ops-soft: #FFFBEB;
    --color-domain-general-soft: #F1F5F9;

    --color-domain-negocio-text: #047857;
    --color-domain-producto-text: #1D4ED8;
    --color-domain-ops-text: #B45309;
    --color-domain-general-text: #475569;

    --color-cert-certificada: #059669;
    --color-cert-revision: #D97706;
    --color-cert-deprecada: #DC2626;

    --color-success-soft: #ECFDF5;
    --color-success-text: #047857;
    --color-danger-soft: #FEF2F2;
    --color-danger-text: #B91C1C;

    --shadow-xs: 0 1px 2px rgba(15, 23, 42, 0.05);
    --shadow-sm: 0 1px 3px rgba(15, 23, 42, 0.08), 0 1px 2px rgba(15, 23, 42, 0.04);
    --shadow-md: 0 4px 12px rgba(15, 23, 42, 0.08), 0 2px 4px rgba(15, 23, 42, 0.04);
    --shadow-lg: 0 12px 32px rgba(15, 23, 42, 0.12), 0 4px 8px rgba(15, 23, 42, 0.04);
    --shadow-drawer: -16px 0 48px rgba(15, 23, 42, 0.16);

    --radius-sm: 6px;
    --radius-md: 10px;
    --radius-lg: 14px;
    --radius-xl: 20px;
    --radius-pill: 999px;

    --ease-out: cubic-bezier(0.16, 1, 0.3, 1);
    --ease-in-out: cubic-bezier(0.65, 0, 0.35, 1);
    --dur-fast: 120ms;
    --dur-base: 200ms;
    --dur-slow: 320ms;

    --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI Variable", "Inter", "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    --font-mono: "SF Mono", "JetBrains Mono", ui-monospace, Menlo, Consolas, monospace;

    --color-header-bg: rgba(255, 255, 255, 0.85);
    --color-glass-bg: rgba(255, 255, 255, 0.7);
    --color-hero-grad-start: #FFFFFF;
    --color-hero-grad-end: #F8FAFC;
    --color-hero-radial-1: rgba(79, 70, 229, 0.10);
    --color-hero-radial-2: rgba(124, 58, 237, 0.08);
    --color-hero-grid-dot: rgba(15, 23, 42, 0.04);
    --color-chip-n-bg: rgba(15, 23, 42, 0.06);
    --color-chip-n-bg-active: rgba(79, 70, 229, 0.12);
    --color-toast-bg: #0F172A;
    --color-toast-fg: #FFFFFF;
    --color-toast-icon: #34D399;
    --color-mark-shadow: rgba(79, 70, 229, 0.28);
    --color-on-accent: #FFFFFF;

    color-scheme: light;
  }}

  [data-theme="dark"] {{
    --color-bg: #0B1020;
    --color-surface: #11151F;
    --color-surface-alt: #1A2030;
    --color-surface-soft: #161B27;
    --color-border: #232A3B;
    --color-border-strong: #2F3850;
    --color-text: #E2E8F0;
    --color-text-secondary: #94A3B8;
    --color-text-muted: #64748B;
    --color-accent: #818CF8;
    --color-accent-soft: rgba(129, 140, 248, 0.14);
    --color-accent-strong: #A5B4FC;
    --color-accent-ring: rgba(129, 140, 248, 0.32);

    --color-domain-negocio-soft: rgba(16, 185, 129, 0.16);
    --color-domain-producto-soft: rgba(59, 130, 246, 0.18);
    --color-domain-ops-soft: rgba(245, 158, 11, 0.16);
    --color-domain-general-soft: rgba(148, 163, 184, 0.16);

    --color-domain-negocio-text: #34D399;
    --color-domain-producto-text: #60A5FA;
    --color-domain-ops-text: #FBBF24;
    --color-domain-general-text: #CBD5E1;

    --color-success-soft: rgba(16, 185, 129, 0.16);
    --color-success-text: #34D399;
    --color-danger-soft: rgba(220, 38, 38, 0.18);
    --color-danger-text: #F87171;

    --shadow-xs: 0 1px 2px rgba(0, 0, 0, 0.40);
    --shadow-sm: 0 1px 3px rgba(0, 0, 0, 0.40), 0 1px 2px rgba(0, 0, 0, 0.30);
    --shadow-md: 0 4px 16px rgba(0, 0, 0, 0.45), 0 2px 4px rgba(0, 0, 0, 0.30);
    --shadow-lg: 0 12px 36px rgba(0, 0, 0, 0.55), 0 4px 8px rgba(0, 0, 0, 0.35);
    --shadow-drawer: -16px 0 48px rgba(0, 0, 0, 0.60);

    --color-header-bg: rgba(11, 16, 32, 0.78);
    --color-glass-bg: rgba(26, 32, 48, 0.65);
    --color-hero-grad-start: #141A28;
    --color-hero-grad-end: #0E1320;
    --color-hero-radial-1: rgba(129, 140, 248, 0.18);
    --color-hero-radial-2: rgba(167, 139, 250, 0.14);
    --color-hero-grid-dot: rgba(255, 255, 255, 0.05);
    --color-chip-n-bg: rgba(255, 255, 255, 0.08);
    --color-chip-n-bg-active: rgba(129, 140, 248, 0.22);
    --color-toast-bg: #1A2030;
    --color-toast-fg: #E2E8F0;
    --color-toast-icon: #34D399;
    --color-mark-shadow: rgba(129, 140, 248, 0.32);
    --color-on-accent: #0B1020;

    color-scheme: dark;
  }}

  * {{ box-sizing: border-box; }}
  [hidden] {{ display: none !important; }}
  html, body {{ margin: 0; padding: 0; }}
  body {{
    font-family: var(--font-sans);
    font-size: 15px;
    line-height: 1.55;
    color: var(--color-text);
    background: var(--color-bg);
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
  }}
  ::selection {{ background: var(--color-accent-soft); color: var(--color-accent-strong); }}

  /* ==================== Header ==================== */
  .app-header {{
    position: sticky;
    top: 0;
    z-index: 30;
    background: var(--color-header-bg);
    backdrop-filter: saturate(180%) blur(12px);
    -webkit-backdrop-filter: saturate(180%) blur(12px);
    border-bottom: 1px solid var(--color-border);
    transition: background var(--dur-base) var(--ease-out), border-color var(--dur-base) var(--ease-out);
  }}
  .app-header-inner {{
    max-width: 1320px;
    margin: 0 auto;
    padding: 0 32px;
    height: 64px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
  }}
  .brand {{
    display: flex;
    align-items: center;
    gap: 12px;
    min-width: 0;
  }}
  .brand-mark {{
    width: 32px;
    height: 32px;
    border-radius: 9px;
    background: linear-gradient(135deg, var(--color-accent) 0%, #7C3AED 100%);
    display: flex;
    align-items: center;
    justify-content: center;
    color: #fff;
    font-weight: 700;
    font-size: 14px;
    letter-spacing: 0.5px;
    box-shadow: 0 4px 12px var(--color-mark-shadow);
    flex-shrink: 0;
  }}
  .brand-text {{ display: flex; flex-direction: column; min-width: 0; }}
  .brand-title {{
    font-size: 15px;
    font-weight: 600;
    color: var(--color-text);
    letter-spacing: -0.01em;
    line-height: 1.2;
  }}
  .brand-sub {{
    font-size: 12px;
    color: var(--color-text-muted);
    line-height: 1.2;
  }}
  .header-right {{
    display: flex;
    align-items: center;
    gap: 12px;
  }}
  .count-pill {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 12px;
    background: var(--color-surface-alt);
    border-radius: var(--radius-pill);
    font-size: 12.5px;
    font-weight: 500;
    color: var(--color-text-secondary);
    font-variant-numeric: tabular-nums;
    transition: background var(--dur-base) var(--ease-out), color var(--dur-base) var(--ease-out);
  }}
  .count-pill .count-dot {{
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--color-domain-negocio);
    box-shadow: 0 0 0 3px rgba(16, 185, 129, 0.18);
  }}
  .kbd-hint {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 5px 10px;
    background: transparent;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    color: var(--color-text-secondary);
    font-size: 12px;
    cursor: pointer;
    transition: background var(--dur-fast) var(--ease-out), border-color var(--dur-fast) var(--ease-out);
  }}
  .kbd-hint:hover {{ background: var(--color-surface-alt); border-color: var(--color-border-strong); }}
  .theme-toggle {{
    width: 36px;
    height: 36px;
    border: 1px solid var(--color-border);
    background: transparent;
    border-radius: var(--radius-md);
    color: var(--color-text-secondary);
    cursor: pointer;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    transition: background var(--dur-fast) var(--ease-out), border-color var(--dur-fast) var(--ease-out), color var(--dur-fast) var(--ease-out);
  }}
  .theme-toggle:hover {{
    background: var(--color-surface-alt);
    border-color: var(--color-border-strong);
    color: var(--color-text);
  }}
  .theme-toggle:focus-visible {{
    outline: none;
    box-shadow: 0 0 0 3px var(--color-accent-ring);
  }}
  .theme-toggle .icon-sun {{ display: none; }}
  .theme-toggle .icon-moon {{ display: inline-flex; }}
  [data-theme="dark"] .theme-toggle .icon-sun {{ display: inline-flex; }}
  [data-theme="dark"] .theme-toggle .icon-moon {{ display: none; }}
  .kbd {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 18px;
    padding: 1px 5px;
    background: var(--color-surface);
    border: 1px solid var(--color-border-strong);
    border-bottom-width: 2px;
    border-radius: 4px;
    font-family: var(--font-mono);
    font-size: 11px;
    color: var(--color-text-secondary);
  }}

  /* ==================== Layout ==================== */
  .page {{
    max-width: 1320px;
    margin: 0 auto;
    padding: 28px 32px 120px;
  }}

  /* ==================== Hero ==================== */
  .hero {{
    position: relative;
    margin-bottom: 24px;
    padding: 24px 28px 26px;
    background:
      radial-gradient(circle at 0% 0%, var(--color-hero-radial-1) 0%, transparent 45%),
      radial-gradient(circle at 100% 0%, var(--color-hero-radial-2) 0%, transparent 50%),
      linear-gradient(180deg, var(--color-hero-grad-start) 0%, var(--color-hero-grad-end) 100%);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-xl);
    box-shadow: var(--shadow-sm);
    overflow: hidden;
  }}
  .hero::before {{
    content: "";
    position: absolute;
    inset: 0;
    background-image: radial-gradient(circle at 1px 1px, var(--color-hero-grid-dot) 1px, transparent 0);
    background-size: 24px 24px;
    mask-image: linear-gradient(180deg, rgba(0,0,0,0.5) 0%, transparent 60%);
    pointer-events: none;
  }}
  .hero-top {{
    position: relative;
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
    min-height: 36px;
  }}
  .hero-title-block {{
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
    min-width: 0;
  }}
  .hero-eyebrow {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 10px 4px 8px;
    background: var(--color-glass-bg);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-pill);
    font-size: 11.5px;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    color: var(--color-accent-strong);
    backdrop-filter: blur(4px);
  }}
  .hero-eyebrow svg {{ color: var(--color-accent); }}
  .hero-title {{
    margin: 0;
    font-size: 22px;
    font-weight: 600;
    letter-spacing: -0.02em;
    color: var(--color-text);
    line-height: 1.2;
  }}
  .hero-meta-inline {{
    margin: 0;
    padding: 3px 9px;
    background: var(--color-surface-alt);
    border-radius: var(--radius-pill);
    font-size: 12px;
    font-weight: 500;
    color: var(--color-text-muted);
    font-variant-numeric: tabular-nums;
    white-space: nowrap;
  }}
  .hero-meta-inline:empty {{ display: none; }}
  .hero-toggle {{
    display: inline-flex;
    position: relative;
    background: var(--color-surface-alt);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    padding: 3px;
    gap: 0;
  }}
  .hero-toggle button {{
    position: relative;
    z-index: 1;
    padding: 6px 14px;
    font: inherit;
    font-size: 12.5px;
    font-weight: 500;
    color: var(--color-text-secondary);
    background: transparent;
    border: none;
    border-radius: 7px;
    cursor: pointer;
    transition: color var(--dur-base) var(--ease-out);
  }}
  .hero-toggle button.active {{ color: var(--color-text); }}
  .hero-toggle .toggle-indicator {{
    position: absolute;
    z-index: 0;
    top: 3px;
    bottom: 3px;
    background: var(--color-surface);
    border-radius: 7px;
    box-shadow: var(--shadow-xs);
    transition: left var(--dur-base) var(--ease-out), width var(--dur-base) var(--ease-out);
    pointer-events: none;
  }}
  .hero-banner {{
    position: relative;
    margin-top: 14px;
    padding: 11px 14px;
    background: var(--color-glass-bg);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    display: flex;
    align-items: flex-start;
    gap: 10px;
    font-size: 12.5px;
    color: var(--color-text-secondary);
    line-height: 1.5;
    backdrop-filter: blur(4px);
  }}
  .hero-banner svg {{ color: var(--color-accent); flex-shrink: 0; margin-top: 1px; }}
  .hero-grid {{
    position: relative;
    margin-top: 16px;
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 12px;
  }}

  /* Hero metric cards (visually distinct from regular rows) */
  .trend-card {{
    position: relative;
    padding: 16px 16px 14px;
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    box-shadow: var(--shadow-xs);
    cursor: pointer;
    overflow: hidden;
    display: flex;
    flex-direction: column;
    min-height: 168px;
    transition: transform var(--dur-base) var(--ease-out), box-shadow var(--dur-base) var(--ease-out), border-color var(--dur-base) var(--ease-out);
  }}
  .trend-card::before {{
    content: "";
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 3px;
    border-radius: var(--radius-lg) var(--radius-lg) 0 0;
  }}
  .trend-card.dom-Negocio::before  {{ background: var(--color-domain-negocio); }}
  .trend-card.dom-Producto::before {{ background: var(--color-domain-producto); }}
  .trend-card.dom-Ops::before      {{ background: var(--color-domain-ops); }}
  .trend-card.dom-General::before  {{ background: var(--color-domain-general); }}
  .trend-card:hover {{
    transform: translateY(-3px);
    box-shadow: var(--shadow-md);
    border-color: var(--color-border-strong);
  }}
  .trend-card:focus-visible {{
    outline: none;
    box-shadow: 0 0 0 3px var(--color-accent-ring), var(--shadow-sm);
    border-color: var(--color-accent);
  }}
  .trend-card.is-top {{
    border-color: var(--color-accent);
    box-shadow: 0 0 0 1px var(--color-accent), var(--shadow-sm);
  }}
  .trend-card.is-top:hover {{
    box-shadow: 0 0 0 1px var(--color-accent), var(--shadow-md);
  }}
  .trend-rank {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    min-width: 26px;
    height: 22px;
    padding: 0 7px;
    background: var(--color-surface-alt);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-pill);
    font-family: var(--font-mono);
    font-size: 11px;
    font-weight: 600;
    color: var(--color-text-secondary);
    letter-spacing: 0.02em;
    margin-bottom: 10px;
    align-self: flex-start;
  }}
  .trend-card.is-top .trend-rank {{
    background: var(--color-accent);
    color: var(--color-on-accent);
    border-color: var(--color-accent);
  }}
  .trend-name {{
    font-size: 15px;
    font-weight: 600;
    color: var(--color-text);
    letter-spacing: -0.01em;
    line-height: 1.3;
    margin-bottom: 6px;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }}
  .trend-tldr {{
    font-size: 12.5px;
    color: var(--color-text-secondary);
    line-height: 1.45;
    flex: 1;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    margin-bottom: 12px;
  }}
  .trend-spark {{
    display: flex;
    align-items: flex-end;
    gap: 2px;
    height: 18px;
    margin-bottom: 10px;
    opacity: 0.85;
  }}
  .trend-spark span {{
    flex: 1;
    background: linear-gradient(180deg, var(--color-accent) 0%, rgba(79, 70, 229, 0.4) 100%);
    border-radius: 1px;
    min-height: 2px;
    transition: opacity var(--dur-base) var(--ease-out);
  }}
  .trend-card.dom-Negocio  .trend-spark span {{ background: linear-gradient(180deg, var(--color-domain-negocio) 0%, rgba(16, 185, 129, 0.4) 100%); }}
  .trend-card.dom-Producto .trend-spark span {{ background: linear-gradient(180deg, var(--color-domain-producto) 0%, rgba(59, 130, 246, 0.4) 100%); }}
  .trend-card.dom-Ops      .trend-spark span {{ background: linear-gradient(180deg, var(--color-domain-ops) 0%, rgba(245, 158, 11, 0.4) 100%); }}
  .trend-card.dom-General  .trend-spark span {{ background: linear-gradient(180deg, var(--color-domain-general) 0%, rgba(148, 163, 184, 0.4) 100%); }}
  .trend-foot {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    margin-top: auto;
  }}
  .trend-relative {{
    font-size: 11.5px;
    color: var(--color-text-muted);
    font-variant-numeric: tabular-nums;
  }}

  /* Delta pills */
  .delta {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 10px;
    border-radius: var(--radius-pill);
    font-size: 11.5px;
    font-weight: 600;
    font-variant-numeric: tabular-nums;
    letter-spacing: 0.01em;
  }}
  .delta.up   {{ background: var(--color-success-soft); color: var(--color-success-text); }}
  .delta.flat {{ background: var(--color-surface-alt); color: var(--color-text-muted); }}
  .delta.new  {{ background: var(--color-accent-soft); color: var(--color-accent-strong); }}
  .delta.down {{ background: var(--color-danger-soft); color: var(--color-danger-text); }}

  /* ==================== Filter card ==================== */
  .filter-card {{
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    box-shadow: var(--shadow-xs);
    padding: 18px 20px;
    margin-bottom: 18px;
  }}
  .filter-row {{
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
  }}
  .filter-row + .filter-row {{ margin-top: 14px; }}

  .search-wrap {{
    position: relative;
    flex: 1 1 320px;
    min-width: 240px;
  }}
  .search-wrap > svg.search-icon {{
    position: absolute;
    left: 14px;
    top: 50%;
    transform: translateY(-50%);
    color: var(--color-text-muted);
    pointer-events: none;
  }}
  .search-wrap .search-shortcut {{
    position: absolute;
    right: 12px;
    top: 50%;
    transform: translateY(-50%);
    pointer-events: none;
    transition: opacity var(--dur-fast) var(--ease-out);
  }}
  .search-wrap input {{ padding-right: 44px; }}
  .search-wrap input:focus + .search-shortcut {{ opacity: 0; }}
  input[type="search"] {{
    width: 100%;
    padding: 11px 16px 11px 42px;
    font: inherit;
    font-size: 14.5px;
    color: var(--color-text);
    background: var(--color-surface);
    border: 1px solid var(--color-border-strong);
    border-radius: var(--radius-md);
    outline: none;
    transition: border-color var(--dur-fast) var(--ease-out), box-shadow var(--dur-fast) var(--ease-out);
  }}
  input[type="search"]::placeholder {{ color: var(--color-text-muted); }}
  input[type="search"]:focus {{
    border-color: var(--color-accent);
    box-shadow: 0 0 0 3px var(--color-accent-ring);
  }}
  input[type="search"]::-webkit-search-cancel-button {{ -webkit-appearance: none; }}

  /* Sort segmented control */
  .sort-group {{
    display: inline-flex;
    position: relative;
    background: var(--color-surface-alt);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    padding: 3px;
    flex-shrink: 0;
  }}
  .sort-group button {{
    position: relative;
    z-index: 1;
    padding: 7px 14px;
    font: inherit;
    font-size: 13px;
    font-weight: 500;
    color: var(--color-text-secondary);
    background: transparent;
    border: none;
    border-radius: 7px;
    cursor: pointer;
    transition: color var(--dur-base) var(--ease-out);
    white-space: nowrap;
  }}
  .sort-group button.active {{ color: var(--color-text); }}
  .sort-group .sort-indicator {{
    position: absolute;
    z-index: 0;
    top: 3px;
    bottom: 3px;
    background: var(--color-surface);
    border-radius: 7px;
    box-shadow: var(--shadow-xs);
    transition: left var(--dur-base) var(--ease-out), width var(--dur-base) var(--ease-out);
    pointer-events: none;
  }}

  /* Filter chips */
  .chips {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    flex-wrap: wrap;
  }}
  .chips-label {{
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: var(--color-text-muted);
    font-weight: 600;
    margin-right: 4px;
  }}
  .chip {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 6px 12px;
    font-size: 13px;
    font-weight: 500;
    color: var(--color-text-secondary);
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-pill);
    cursor: pointer;
    transition: all var(--dur-fast) var(--ease-out);
    line-height: 1.2;
  }}
  .chip:hover {{
    background: var(--color-surface-alt);
    border-color: var(--color-border-strong);
  }}
  .chip:focus-visible {{
    outline: none;
    box-shadow: 0 0 0 3px var(--color-accent-ring);
  }}
  .chip.active {{
    background: var(--color-accent-soft);
    color: var(--color-accent-strong);
    border-color: var(--color-accent);
    transform: scale(1);
    animation: chip-pop var(--dur-base) var(--ease-out);
  }}
  @keyframes chip-pop {{
    0%   {{ transform: scale(0.96); }}
    100% {{ transform: scale(1); }}
  }}
  .chip .n {{
    font-variant-numeric: tabular-nums;
    font-weight: 500;
    font-size: 11.5px;
    padding: 1px 6px;
    background: var(--color-chip-n-bg);
    border-radius: var(--radius-pill);
    color: inherit;
    opacity: 0.85;
  }}
  .chip.active .n {{ background: var(--color-chip-n-bg-active); }}
  .chip-dot {{
    width: 7px;
    height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  .chip-dot.cert-cert {{ background: var(--color-cert-certificada); }}
  .chip-dot.cert-rev  {{ background: var(--color-cert-revision); }}
  .chip-dot.cert-dep  {{ background: var(--color-cert-deprecada); }}
  .chip-dot.dom-Negocio  {{ background: var(--color-domain-negocio); }}
  .chip-dot.dom-Producto {{ background: var(--color-domain-producto); }}
  .chip-dot.dom-Ops      {{ background: var(--color-domain-ops); }}
  .chip-dot.dom-General  {{ background: var(--color-domain-general); }}

  .chip.tags-toggle {{ font-weight: 500; }}
  .chip.tags-toggle.has-active {{
    background: var(--color-accent-soft);
    color: var(--color-accent-strong);
    border-color: var(--color-accent);
  }}
  .chip.clear-btn {{
    color: var(--color-text-muted);
    border-color: transparent;
    background: transparent;
    opacity: 0;
    pointer-events: none;
    transition: opacity var(--dur-base) var(--ease-out);
  }}
  .chip.clear-btn.show {{ opacity: 1; pointer-events: auto; }}
  .chip.clear-btn:hover {{ color: var(--color-danger-text); background: var(--color-danger-soft); }}

  .tag-panel {{
    width: 100%;
    margin-top: 0;
    max-height: 0;
    overflow: hidden;
    opacity: 0;
    transition: max-height var(--dur-base) var(--ease-out), opacity var(--dur-base) var(--ease-out), margin-top var(--dur-base) var(--ease-out);
  }}
  .tag-panel.open {{
    max-height: 320px;
    opacity: 1;
    margin-top: 14px;
  }}
  .tag-panel-inner {{
    padding: 14px;
    background: var(--color-surface-alt);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
  }}
  .tag-chip {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 4px 10px;
    font-size: 12px;
    font-family: var(--font-mono);
    color: var(--color-text-secondary);
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-pill);
    cursor: pointer;
    transition: all var(--dur-fast) var(--ease-out);
    line-height: 1.2;
  }}
  .tag-chip:hover {{ background: var(--color-surface-alt); border-color: var(--color-border-strong); }}
  .tag-chip.active {{
    background: var(--color-accent);
    color: var(--color-on-accent);
    border-color: var(--color-accent);
  }}
  .tag-chip .n {{
    font-variant-numeric: tabular-nums;
    opacity: 0.7;
    font-size: 11px;
  }}

  /* ==================== Table ==================== */
  .table-card {{
    background: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    box-shadow: var(--shadow-xs);
    overflow: hidden;
  }}
  table.metrics {{
    width: 100%;
    border-collapse: separate;
    border-spacing: 0;
  }}
  table.metrics thead th {{
    text-align: left;
    font-size: 11.5px;
    font-weight: 600;
    color: var(--color-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    padding: 13px 18px;
    background: var(--color-surface-alt);
    border-bottom: 1px solid var(--color-border);
    position: sticky;
    top: 64px;
    z-index: 5;
  }}
  table.metrics tbody tr {{
    cursor: pointer;
    border-top: 1px solid var(--color-border);
    transition: background var(--dur-fast) var(--ease-out);
    position: relative;
  }}
  table.metrics tbody tr:first-child {{ border-top: none; }}
  table.metrics tbody tr::before {{
    content: "";
    position: absolute;
    left: 0;
    top: 0;
    bottom: 0;
    width: 3px;
    background: transparent;
    transition: width var(--dur-fast) var(--ease-out);
  }}
  table.metrics tbody tr.dom-Negocio::before  {{ background: var(--color-domain-negocio); }}
  table.metrics tbody tr.dom-Producto::before {{ background: var(--color-domain-producto); }}
  table.metrics tbody tr.dom-Ops::before      {{ background: var(--color-domain-ops); }}
  table.metrics tbody tr.dom-General::before  {{ background: var(--color-domain-general); }}
  table.metrics tbody tr:hover {{ background: var(--color-surface-soft); }}
  table.metrics tbody tr:hover::before {{ width: 5px; }}
  table.metrics tbody tr.selected {{ background: var(--color-accent-soft); }}
  table.metrics tbody tr.selected::before {{ width: 5px; }}
  table.metrics tbody tr:focus-visible {{
    outline: none;
    box-shadow: inset 0 0 0 2px var(--color-accent);
  }}

  table.metrics tbody td {{
    padding: 16px 18px;
    vertical-align: top;
  }}
  td.col-name {{ width: 36%; }}
  td.col-name .name-row {{
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 14.5px;
    font-weight: 600;
    color: var(--color-text);
    letter-spacing: -0.005em;
  }}
  td.col-name .name-row .cert-dot {{
    width: 7px;
    height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  td.col-name .copies {{
    display: block;
    font-weight: 400;
    font-size: 11.5px;
    color: var(--color-text-muted);
    margin-top: 4px;
  }}
  td.col-domain {{ width: 130px; }}
  td.col-tldr {{
    color: var(--color-text-secondary);
    font-size: 13.5px;
    line-height: 1.5;
  }}
  td.col-tldr p {{
    margin: 0;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
  }}
  td.col-card {{
    width: 110px;
    text-align: right;
    font-family: var(--font-mono);
    font-size: 13px;
    color: var(--color-text-muted);
  }}
  td.col-card a {{
    display: inline-flex;
    align-items: center;
    gap: 4px;
    color: var(--color-text-muted);
    text-decoration: none;
    padding: 3px 8px;
    border-radius: var(--radius-sm);
    transition: background var(--dur-fast) var(--ease-out), color var(--dur-fast) var(--ease-out);
  }}
  td.col-card a:hover {{
    color: var(--color-accent-strong);
    background: var(--color-accent-soft);
  }}
  td.col-card a svg {{ opacity: 0; transition: opacity var(--dur-fast) var(--ease-out); }}
  table.metrics tbody tr:hover td.col-card a svg {{ opacity: 0.8; }}

  /* Domain & cert badges */
  .domain-badge {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 3px 10px 3px 8px;
    border-radius: var(--radius-pill);
    font-size: 12px;
    font-weight: 500;
    line-height: 1.4;
  }}
  .domain-badge .dot {{
    width: 6px;
    height: 6px;
    border-radius: 50%;
    flex-shrink: 0;
  }}
  .domain-badge.dom-Negocio  {{ background: var(--color-domain-negocio-soft);  color: var(--color-domain-negocio-text); }}
  .domain-badge.dom-Producto {{ background: var(--color-domain-producto-soft); color: var(--color-domain-producto-text); }}
  .domain-badge.dom-Ops      {{ background: var(--color-domain-ops-soft);      color: var(--color-domain-ops-text); }}
  .domain-badge.dom-General  {{ background: var(--color-domain-general-soft);  color: var(--color-domain-general-text); }}
  .domain-badge.dom-Negocio  .dot {{ background: var(--color-domain-negocio); }}
  .domain-badge.dom-Producto .dot {{ background: var(--color-domain-producto); }}
  .domain-badge.dom-Ops      .dot {{ background: var(--color-domain-ops); }}
  .domain-badge.dom-General  .dot {{ background: var(--color-domain-general); }}

  .cert-dot.cert-cert {{ background: var(--color-cert-certificada); box-shadow: 0 0 0 3px rgba(5, 150, 105, 0.18); }}
  .cert-dot.cert-rev  {{ background: var(--color-cert-revision); box-shadow: 0 0 0 3px rgba(217, 119, 6, 0.18); }}
  .cert-dot.cert-dep  {{ background: var(--color-cert-deprecada); box-shadow: 0 0 0 3px rgba(220, 38, 38, 0.18); }}

  .show-more-row td {{
    text-align: center !important;
    font-size: 13px;
    color: var(--color-text-muted) !important;
    padding: 18px !important;
    cursor: default !important;
    background: var(--color-surface-soft);
  }}

  /* ==================== Empty state ==================== */
  .empty {{
    padding: 56px 24px;
    text-align: center;
    background: var(--color-surface);
    border: 1px dashed var(--color-border-strong);
    border-radius: var(--radius-lg);
    animation: fade-in var(--dur-slow) var(--ease-out);
  }}
  @keyframes fade-in {{ from {{ opacity: 0; transform: translateY(4px); }} to {{ opacity: 1; transform: translateY(0); }} }}
  .empty-icon {{
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 48px;
    height: 48px;
    background: var(--color-surface-alt);
    border-radius: var(--radius-md);
    color: var(--color-text-muted);
    margin-bottom: 14px;
  }}
  .empty-title {{
    font-size: 16px;
    font-weight: 600;
    color: var(--color-text);
    margin: 0 0 4px;
  }}
  .empty-sub {{
    font-size: 13.5px;
    color: var(--color-text-muted);
    margin: 0 0 16px;
  }}
  .empty-action {{
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 14px;
    background: var(--color-accent);
    color: var(--color-on-accent);
    border: none;
    border-radius: var(--radius-md);
    font: inherit;
    font-size: 13px;
    font-weight: 500;
    cursor: pointer;
    transition: background var(--dur-fast) var(--ease-out);
  }}
  .empty-action:hover {{ background: var(--color-accent-strong); }}

  /* ==================== Drawer ==================== */
  .drawer-backdrop {{
    position: fixed;
    inset: 0;
    background: rgba(15, 23, 42, 0.42);
    backdrop-filter: blur(2px);
    -webkit-backdrop-filter: blur(2px);
    opacity: 0;
    pointer-events: none;
    transition: opacity var(--dur-base) var(--ease-out);
    z-index: 40;
  }}
  .drawer-backdrop.open {{
    opacity: 1;
    pointer-events: auto;
  }}
  .drawer {{
    position: fixed;
    top: 0;
    right: 0;
    bottom: 0;
    width: min(540px, 100%);
    background: var(--color-surface);
    box-shadow: var(--shadow-drawer);
    transform: translateX(100%);
    transition: transform var(--dur-slow) var(--ease-out);
    z-index: 41;
    overflow-y: auto;
    display: flex;
    flex-direction: column;
  }}
  .drawer.open {{ transform: translateX(0); }}
  .drawer-header {{
    position: sticky;
    top: 0;
    z-index: 2;
    padding: 22px 28px 20px;
    background: var(--color-surface);
    border-bottom: 1px solid var(--color-border);
  }}
  .drawer-close {{
    position: absolute;
    top: 18px;
    right: 18px;
    width: 36px;
    height: 36px;
    border: none;
    background: transparent;
    cursor: pointer;
    border-radius: var(--radius-md);
    color: var(--color-text-muted);
    display: flex;
    align-items: center;
    justify-content: center;
    transition: background var(--dur-fast) var(--ease-out), color var(--dur-fast) var(--ease-out);
  }}
  .drawer-close:hover {{ background: var(--color-surface-alt); color: var(--color-text); }}
  .drawer-header h2 {{
    margin: 0 56px 10px 0;
    font-size: 22px;
    font-weight: 600;
    color: var(--color-text);
    letter-spacing: -0.015em;
    line-height: 1.25;
  }}
  .drawer-meta {{
    display: flex;
    flex-wrap: wrap;
    gap: 6px 14px;
    font-size: 12.5px;
    color: var(--color-text-muted);
    align-items: center;
  }}
  .drawer-meta-item {{
    display: inline-flex;
    align-items: center;
    gap: 5px;
    font-variant-numeric: tabular-nums;
  }}
  .drawer-meta-item svg {{ color: var(--color-text-muted); flex-shrink: 0; }}
  .drawer-meta-item .k {{ color: var(--color-text-secondary); font-weight: 500; }}

  .drawer section {{
    padding: 22px 28px;
    border-bottom: 1px solid var(--color-border);
  }}
  .drawer section:last-child {{ border-bottom: none; }}
  .drawer h3 {{
    margin: 0 0 10px;
    font-size: 11px;
    font-weight: 600;
    color: var(--color-text-muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
  }}
  .drawer-prose {{
    margin: 0;
    color: var(--color-text);
    font-size: 14.5px;
    line-height: 1.6;
    max-width: 60ch;
    white-space: pre-wrap;
  }}
  .drawer-review {{
    margin: 0 24px 24px;
    padding: 14px 16px;
    background: color-mix(in srgb, var(--color-cert-revision) 12%, var(--color-surface));
    border: 1px solid color-mix(in srgb, var(--color-cert-revision) 35%, transparent);
    border-left: 4px solid var(--color-cert-revision);
    border-radius: var(--radius-md);
    display: flex;
    gap: 12px;
    align-items: flex-start;
  }}
  .drawer-review-icon {{
    flex: none;
    color: var(--color-cert-revision);
    margin-top: 1px;
  }}
  .drawer-review-body {{ min-width: 0; max-width: 60ch; }}
  .drawer-review-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--color-cert-revision);
    letter-spacing: 0.01em;
    margin-bottom: 4px;
  }}
  .drawer-review-text {{
    font-size: 13.5px;
    line-height: 1.55;
    color: var(--color-text-secondary);
    white-space: pre-wrap;
  }}
  .drawer-tech {{
    margin: 0;
    padding: 14px 16px;
    background: var(--color-surface-alt);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    font-family: var(--font-mono);
    font-size: 12.5px;
    line-height: 1.55;
    color: var(--color-text);
    white-space: pre-wrap;
    overflow-x: auto;
  }}
  .drawer-tags {{ display: flex; flex-wrap: wrap; gap: 6px; }}
  .drawer-tag {{
    display: inline-flex;
    align-items: center;
    padding: 4px 10px;
    background: var(--color-surface-alt);
    color: var(--color-text-secondary);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-pill);
    font-size: 12px;
    font-family: var(--font-mono);
    cursor: pointer;
    transition: all var(--dur-fast) var(--ease-out);
  }}
  .drawer-tag:hover {{
    background: var(--color-accent-soft);
    color: var(--color-accent-strong);
    border-color: var(--color-accent);
  }}
  .drawer-actions {{ display: flex; gap: 10px; flex-wrap: wrap; }}
  .btn-primary {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 18px;
    background: var(--color-accent);
    color: var(--color-on-accent);
    border: none;
    border-radius: var(--radius-md);
    text-decoration: none;
    font: inherit;
    font-size: 13.5px;
    font-weight: 500;
    cursor: pointer;
    transition: background var(--dur-fast) var(--ease-out), transform var(--dur-fast) var(--ease-out), box-shadow var(--dur-fast) var(--ease-out);
    box-shadow: 0 1px 2px var(--color-mark-shadow);
  }}
  .btn-primary:hover {{
    background: var(--color-accent-strong);
    box-shadow: 0 4px 12px var(--color-mark-shadow);
    transform: translateY(-1px);
  }}
  .btn-secondary {{
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 18px;
    background: var(--color-surface);
    color: var(--color-text);
    border: 1px solid var(--color-border-strong);
    border-radius: var(--radius-md);
    font: inherit;
    font-size: 13.5px;
    font-weight: 500;
    cursor: pointer;
    transition: background var(--dur-fast) var(--ease-out), border-color var(--dur-fast) var(--ease-out);
  }}
  .btn-secondary:hover {{
    background: var(--color-surface-alt);
    border-color: var(--color-text-muted);
  }}

  /* ==================== Toast ==================== */
  .toast {{
    position: fixed;
    bottom: 28px;
    left: 50%;
    transform: translateX(-50%) translateY(20px);
    padding: 12px 20px;
    background: var(--color-toast-bg);
    color: var(--color-toast-fg);
    border-radius: var(--radius-md);
    border: 1px solid var(--color-border);
    font-size: 13.5px;
    font-weight: 500;
    box-shadow: var(--shadow-lg);
    opacity: 0;
    pointer-events: none;
    transition: opacity var(--dur-base) var(--ease-out), transform var(--dur-base) var(--ease-out);
    z-index: 60;
    display: inline-flex;
    align-items: center;
    gap: 8px;
  }}
  .toast.show {{
    opacity: 1;
    transform: translateX(-50%) translateY(0);
  }}
  .toast svg {{ color: var(--color-toast-icon); }}

  /* ==================== Footer ==================== */
  .app-footer {{
    margin-top: 32px;
    padding: 20px 32px;
    background: var(--color-surface);
    border-top: 1px solid var(--color-border);
    color: var(--color-text-muted);
    font-size: 12.5px;
    text-align: center;
  }}
  .app-footer code {{
    background: var(--color-surface-alt);
    padding: 2px 7px;
    border-radius: var(--radius-sm);
    font-size: 11.5px;
  }}

  /* ==================== Responsive ==================== */
  @media (max-width: 1100px) {{
    .hero-grid {{ grid-template-columns: repeat(3, 1fr); }}
  }}
  @media (max-width: 900px) {{
    .hero-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .app-header-inner {{ padding: 0 20px; }}
    .page {{ padding: 20px; }}
    .hero {{ padding: 24px; }}
    .hero-title {{ font-size: 22px; }}
  }}
  @media (max-width: 640px) {{
    .hero-grid {{ grid-template-columns: 1fr; }}
    .brand-sub {{ display: none; }}
    .kbd-hint {{ display: none; }}
    table.metrics thead {{ display: none; }}
    table.metrics, table.metrics tbody, table.metrics tr, table.metrics td {{ display: block; }}
    .table-card {{ background: transparent; border: none; box-shadow: none; }}
    table.metrics tbody tr {{
      background: var(--color-surface);
      border: 1px solid var(--color-border);
      border-radius: var(--radius-md);
      margin-bottom: 10px;
      padding: 14px 18px;
      box-shadow: var(--shadow-xs);
    }}
    table.metrics tbody tr::before {{
      border-radius: var(--radius-md) 0 0 var(--radius-md);
    }}
    table.metrics tbody td {{ padding: 0; width: auto !important; }}
    td.col-name {{ margin-bottom: 6px; }}
    td.col-domain {{ margin-bottom: 8px; }}
    td.col-tldr {{ margin-bottom: 8px; }}
    td.col-card {{ text-align: left; font-size: 12px; }}
    .drawer {{ width: 100%; }}
    .drawer-header, .drawer section {{ padding-left: 20px; padding-right: 20px; }}
    .filter-card {{ padding: 14px; }}
  }}

  /* Reduced motion */
  @media (prefers-reduced-motion: reduce) {{
    *, *::before, *::after {{
      animation-duration: 0.01ms !important;
      animation-iteration-count: 1 !important;
      transition-duration: 0.01ms !important;
    }}
  }}
</style>
</head>
<body>
  <header class="app-header">
    <div class="app-header-inner">
      <div class="brand">
        <div class="brand-mark">Z</div>
        <div class="brand-text">
          <div class="brand-title">Diccionario de Métricas</div>
          <div class="brand-sub">ExampleCorp &middot; {total_metrics} métricas auditadas</div>
        </div>
      </div>
      <div class="header-right">
        <div class="count-pill" id="result-count">
          <span class="count-dot"></span>
          <span id="count-text"></span>
        </div>
        <button class="kbd-hint" id="shortcuts-btn" type="button" title="Atajos de teclado">
          <span class="kbd">/</span>
          <span>Buscar</span>
        </button>
        <button class="theme-toggle" id="theme-toggle" type="button" aria-label="Cambiar tema" title="Cambiar tema">
          <svg class="icon-moon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>
          <svg class="icon-sun" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>
        </button>
      </div>
    </div>
  </header>

  <div class="page">
    <section class="hero" id="hero" hidden>
      <div class="hero-top">
        <div class="hero-title-block">
          <span class="hero-eyebrow">
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M8.5 14.5A2.5 2.5 0 0 0 11 12c0-1.38-.5-2-1-3-1.072-2.143-.224-4.054 2-6 .5 2.5 2 4.9 4 6.5 2 1.6 3 3.5 3 5.5a7 7 0 1 1-14 0c0-1.153.433-2.294 1-3a2.5 2.5 0 0 0 2.5 2.5z"/></svg>
            Tendencia
          </span>
          <h2 class="hero-title">Métricas más consultadas</h2>
          <span class="hero-meta-inline" id="hero-meta"></span>
        </div>
        <div class="hero-toggle" id="hero-toggle" role="tablist" aria-label="Ventana de tiempo">
          <span class="toggle-indicator" id="hero-toggle-indicator"></span>
          <button class="active" data-window="30d" type="button" role="tab">30 días</button>
          <button data-window="7d" type="button" role="tab">7 días</button>
        </div>
      </div>
      <div class="hero-banner" id="hero-banner" hidden>
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><path d="M12 16v-4"/><path d="M12 8h.01"/></svg>
        <span id="hero-banner-text"></span>
      </div>
      <div class="hero-grid" id="hero-grid"></div>
    </section>

    <section class="filter-card">
      <div class="filter-row">
        <label class="search-wrap">
          <svg class="search-icon" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>
          <input type="search" id="q" placeholder="Buscar por nombre, descripción, tag, card#…" spellcheck="false" autocomplete="off">
          <span class="search-shortcut"><span class="kbd">/</span></span>
        </label>
        <div class="sort-group" id="sort-group" role="tablist" aria-label="Ordenar">
          <span class="sort-indicator" id="sort-indicator"></span>
          <button data-sort="name" class="active" type="button" role="tab">A–Z</button>
          <button data-sort="popular-30d" type="button" role="tab">Top 30d</button>
          <button data-sort="popular-7d" type="button" role="tab">Top 7d</button>
          <button data-sort="recent" type="button" role="tab">Recientes</button>
        </div>
      </div>
      <div class="filter-row">
        <div class="chips" id="domain-chips"></div>
        <div class="chips" id="cert-chips"></div>
        <button class="chip tags-toggle" id="tags-toggle" type="button" aria-expanded="false">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41 13.42 20.58a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><circle cx="7" cy="7" r="1.5" fill="currentColor"/></svg>
          <span id="tags-toggle-label">Tags</span>
        </button>
        <button class="chip clear-btn" id="clear-btn" type="button">
          <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
          Limpiar
        </button>
      </div>
      <div class="tag-panel" id="tag-panel">
        <div class="tag-panel-inner" id="tag-panel-inner"></div>
      </div>
    </section>

    <div class="table-card" id="table-card">
      <table class="metrics" aria-label="Tabla de métricas">
        <thead>
          <tr>
            <th>Métrica</th>
            <th>Dominio</th>
            <th>Qué mide</th>
            <th style="text-align:right">Card</th>
          </tr>
        </thead>
        <tbody id="rows"></tbody>
      </table>
    </div>

    <div class="empty" id="empty" style="display:none">
      <div class="empty-icon">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/><path d="m8 11 6 0"/></svg>
      </div>
      <p class="empty-title">No encontramos métricas</p>
      <p class="empty-sub">Intenta con otra búsqueda o ajusta los filtros para ver más resultados.</p>
      <button class="empty-action" id="empty-clear" type="button">
        <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/></svg>
        Limpiar filtros
      </button>
    </div>
  </div>

  <div class="drawer-backdrop" id="backdrop"></div>
  <aside class="drawer" id="drawer" aria-hidden="true" aria-labelledby="d-name">
    <div class="drawer-header">
      <button class="drawer-close" aria-label="Cerrar" id="close-btn">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6 6 18"/><path d="m6 6 12 12"/></svg>
      </button>
      <h2 id="d-name"></h2>
      <div class="drawer-meta" id="d-meta"></div>
    </div>
    <div class="drawer-review" id="d-review" hidden>
      <div class="drawer-review-icon" aria-hidden="true">
        <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 9v4"/><path d="M12 17h.01"/><path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0Z"/></svg>
      </div>
      <div class="drawer-review-body">
        <div class="drawer-review-title">Métrica en revisión</div>
        <div class="drawer-review-text" id="d-review-text"></div>
      </div>
    </div>
    <section>
      <h3>Qué mide</h3>
      <p class="drawer-prose" id="d-business"></p>
    </section>
    <section>
      <h3>Cómo se calcula</h3>
      <pre class="drawer-tech" id="d-technical"></pre>
    </section>
    <section>
      <h3>Tags</h3>
      <div class="drawer-tags" id="d-tags"></div>
    </section>
    <section>
      <div class="drawer-actions">
        <a class="btn-primary" id="d-link" href="#" target="_blank" rel="noopener">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><path d="M15 3h6v6"/><path d="M10 14 21 3"/></svg>
          Abrir en Metabase
        </a>
        <button class="btn-secondary" id="copy-link-btn" type="button">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
          Copiar link
        </button>
      </div>
    </section>
  </aside>

  <div class="toast" id="toast">
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"/></svg>
    <span id="toast-text"></span>
  </div>

  <footer class="app-footer">
    Generado {generated_at} &middot; <code>{total_metrics}</code> métricas cubriendo <code>{total_cards}</code> cards activas
  </footer>

<script id="payload" type="application/json">{payload_json}</script>
<script id="popularity-payload" type="application/json">{popularity_json}</script>
<script>
(function() {{
  const DATA = JSON.parse(document.getElementById('payload').textContent);
  const POP_RAW = document.getElementById('popularity-payload').textContent;
  const POP = POP_RAW && POP_RAW.trim() && POP_RAW.trim() !== 'null' ? JSON.parse(POP_RAW) : null;
  const DOMAINS = ['Negocio', 'Producto', 'Ops', 'General'];
  const CERTS = ['Certificada por Agente', 'En Revisión', 'Deprecada'];
  const CERT_SHORT = {{ 'Certificada por Agente': 'Certificada', 'En Revisión': 'En revisión', 'Deprecada': 'Deprecada' }};
  const CERT_DOT_CLASS = {{ 'Certificada por Agente': 'cert-cert', 'En Revisión': 'cert-rev', 'Deprecada': 'cert-dep' }};

  const state = {{
    q: '',
    domain: 'Todos',
    cert: 'Todas',
    tags: new Set(),
    sort: 'name',
    heroWindow: '30d',
  }};

  // ---- DOM refs --------------------------------------------------------
  const $ = (id) => document.getElementById(id);
  const rowsEl = $('rows');
  const emptyEl = $('empty');
  const tableCardEl = $('table-card');
  const countEl = $('count-text');
  const chipsEl = $('domain-chips');
  const certChipsEl = $('cert-chips');
  const tagsToggle = $('tags-toggle');
  const tagsToggleLabel = $('tags-toggle-label');
  const tagPanel = $('tag-panel');
  const tagPanelInner = $('tag-panel-inner');
  const sortGroupEl = $('sort-group');
  const sortIndicator = $('sort-indicator');
  const heroEl = $('hero');
  const heroGridEl = $('hero-grid');
  const heroToggleEl = $('hero-toggle');
  const heroToggleIndicator = $('hero-toggle-indicator');
  const heroMetaEl = $('hero-meta');
  const heroBannerEl = $('hero-banner');
  const heroBannerText = $('hero-banner-text');
  const qEl = $('q');
  const drawer = $('drawer');
  const backdrop = $('backdrop');
  const closeBtn = $('close-btn');
  const clearBtn = $('clear-btn');
  const emptyClearBtn = $('empty-clear');
  const copyLinkBtn = $('copy-link-btn');
  const toastEl = $('toast');
  const toastText = $('toast-text');
  const shortcutsBtn = $('shortcuts-btn');

  // ---- Utilities -------------------------------------------------------
  const BY_CARD = new Map();
  DATA.forEach(m => BY_CARD.set(m.c, m));

  function escapeHTML(s) {{
    return (s == null ? '' : String(s)).replace(/[&<>"']/g, c => ({{
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }})[c]);
  }}
  function escapeAttr(s) {{ return escapeHTML(s).replace(/`/g, '&#96;'); }}

  function formatRelative(iso) {{
    if (!iso) return '';
    const t = Date.parse(iso);
    if (isNaN(t)) return '';
    const diffMs = Date.now() - t;
    const day = 86400000;
    const days = Math.floor(diffMs / day);
    if (days <= 0) return 'hoy';
    if (days === 1) return 'hace 1 día';
    if (days < 7) return `hace ${{days}} días`;
    if (days < 30) {{ const w = Math.floor(days / 7); return w === 1 ? 'hace 1 semana' : `hace ${{w}} semanas`; }}
    if (days < 365) {{ const m = Math.floor(days / 30); return m === 1 ? 'hace 1 mes' : `hace ${{m}} meses`; }}
    const y = Math.floor(days / 365);
    return y === 1 ? 'hace 1 año' : `hace ${{y}} años`;
  }}

  // Deterministic pseudo-random sparkline bars based on card id.
  function sparklineBars(cardId, n) {{
    n = n || 12;
    let seed = (cardId | 0) || 1;
    const out = [];
    for (let i = 0; i < n; i++) {{
      seed = (seed * 9301 + 49297) % 233280;
      const v = 0.3 + (seed / 233280) * 0.7;
      out.push(Math.round(v * 100));
    }}
    return out;
  }}

  function showToast(msg) {{
    toastText.textContent = msg;
    toastEl.classList.add('show');
    clearTimeout(showToast._t);
    showToast._t = setTimeout(() => toastEl.classList.remove('show'), 2200);
  }}

  // ---- Domain chips ----------------------------------------------------
  const countsByDomain = DATA.reduce((acc, m) => {{ acc[m.d] = (acc[m.d]||0)+1; return acc; }}, {{}});
  const domainChipHTML = (label, n, isAll) => {{
    const cls = label === state.domain ? 'chip active' : 'chip';
    const dot = isAll ? '' : `<span class="chip-dot dom-${{label}}"></span>`;
    return `<button class="${{cls}}" data-dom="${{escapeAttr(label)}}" type="button">${{dot}}${{escapeHTML(label)}} <span class="n">${{n}}</span></button>`;
  }};
  chipsEl.innerHTML =
    '<span class="chips-label">Dominio</span>' +
    domainChipHTML('Todos', DATA.length, true) +
    DOMAINS.map(d => domainChipHTML(d, countsByDomain[d]||0, false)).join('');
  chipsEl.addEventListener('click', (e) => {{
    const btn = e.target.closest('.chip');
    if (!btn) return;
    state.domain = btn.dataset.dom;
    chipsEl.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.dom === state.domain));
    syncURL(); render(); updateClearBtn();
  }});

  // ---- Certification chips ---------------------------------------------
  const countsByCert = DATA.reduce((acc, m) => {{ const k = m.cs || ''; acc[k] = (acc[k]||0)+1; return acc; }}, {{}});
  const certChipHTML = (label, n, isAll) => {{
    const cls = label === state.cert ? 'chip active' : 'chip';
    const dotClass = CERT_DOT_CLASS[label];
    const dot = (isAll || !dotClass) ? '' : `<span class="chip-dot ${{dotClass}}"></span>`;
    const display = isAll ? label : (CERT_SHORT[label] || label);
    return `<button class="${{cls}}" data-cert="${{escapeAttr(label)}}" type="button">${{dot}}${{escapeHTML(display)}} <span class="n">${{n}}</span></button>`;
  }};
  const certEntries = [['Todas', DATA.length, true], ...CERTS.map(c => [c, countsByCert[c]||0, false]).filter(([,n]) => n > 0)];
  certChipsEl.innerHTML =
    '<span class="chips-label">Estado</span>' +
    certEntries.map(([l, n, isAll]) => certChipHTML(l, n, isAll)).join('');
  certChipsEl.addEventListener('click', (e) => {{
    const btn = e.target.closest('.chip');
    if (!btn) return;
    state.cert = btn.dataset.cert;
    certChipsEl.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.cert === state.cert));
    syncURL(); render(); updateClearBtn();
  }});

  // ---- Tag panel (top 15 by frequency) ---------------------------------
  const tagCounts = new Map();
  DATA.forEach(m => (m.tags || []).forEach(t => tagCounts.set(t, (tagCounts.get(t)||0) + 1)));
  const topTags = [...tagCounts.entries()].sort((a,b) => b[1]-a[1]).slice(0, 15);
  tagPanelInner.innerHTML = topTags.map(([t, n]) =>
    `<button class="tag-chip" data-tag="${{escapeAttr(t)}}" type="button">${{escapeHTML(t)}} <span class="n">${{n}}</span></button>`
  ).join('');
  tagsToggle.addEventListener('click', () => {{
    const open = !tagPanel.classList.contains('open');
    tagPanel.classList.toggle('open', open);
    tagsToggle.setAttribute('aria-expanded', String(open));
  }});
  tagPanelInner.addEventListener('click', (e) => {{
    const btn = e.target.closest('.tag-chip');
    if (!btn) return;
    const t = btn.dataset.tag;
    if (state.tags.has(t)) state.tags.delete(t); else state.tags.add(t);
    btn.classList.toggle('active', state.tags.has(t));
    updateTagToggleLabel();
    syncURL(); render(); updateClearBtn();
  }});

  function updateTagToggleLabel() {{
    tagsToggleLabel.textContent = state.tags.size ? `Tags (${{state.tags.size}})` : 'Tags';
    tagsToggle.classList.toggle('has-active', state.tags.size > 0);
  }}

  // ---- Sort segmented control ------------------------------------------
  function updateSortIndicator() {{
    const active = sortGroupEl.querySelector('button.active');
    if (!active) return;
    sortIndicator.style.left = active.offsetLeft + 'px';
    sortIndicator.style.width = active.offsetWidth + 'px';
  }}
  sortGroupEl.addEventListener('click', (e) => {{
    const btn = e.target.closest('button[data-sort]');
    if (!btn) return;
    state.sort = btn.dataset.sort;
    sortGroupEl.querySelectorAll('button').forEach(b => b.classList.toggle('active', b === btn));
    updateSortIndicator();
    syncURL(); render();
  }});

  // ---- Hero toggle indicator -------------------------------------------
  function updateHeroToggleIndicator() {{
    const active = heroToggleEl.querySelector('button.active');
    if (!active || heroToggleEl.style.display === 'none') return;
    heroToggleIndicator.style.left = active.offsetLeft + 'px';
    heroToggleIndicator.style.width = active.offsetWidth + 'px';
  }}

  // ---- Search ----------------------------------------------------------
  let searchT;
  qEl.addEventListener('input', (e) => {{
    clearTimeout(searchT);
    searchT = setTimeout(() => {{
      state.q = e.target.value.trim().toLowerCase();
      syncURL(); render(); updateClearBtn();
    }}, 80);
  }});

  // ---- Clear filters ---------------------------------------------------
  function updateClearBtn() {{
    const hasFilters = state.q || state.domain !== 'Todos' || state.cert !== 'Todas' || state.tags.size > 0;
    clearBtn.classList.toggle('show', hasFilters);
  }}
  function clearFilters() {{
    state.q = '';
    state.domain = 'Todos';
    state.cert = 'Todas';
    state.tags.clear();
    qEl.value = '';
    chipsEl.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.dom === 'Todos'));
    certChipsEl.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.cert === 'Todas'));
    tagPanelInner.querySelectorAll('.tag-chip').forEach(c => c.classList.remove('active'));
    updateTagToggleLabel();
    syncURL(); render(); updateClearBtn();
  }}
  clearBtn.addEventListener('click', clearFilters);
  emptyClearBtn.addEventListener('click', clearFilters);

  // ---- URL hash (deep-linking) -----------------------------------------
  function parseURL() {{
    const h = (location.hash || '').replace(/^#/, '');
    if (!h) return;
    const params = new URLSearchParams(h);
    if (params.has('q')) {{ state.q = params.get('q').toLowerCase(); qEl.value = params.get('q'); }}
    if (params.has('dom')) state.domain = params.get('dom');
    if (params.has('cert')) state.cert = params.get('cert');
    if (params.has('sort')) state.sort = params.get('sort');
    if (params.has('tags')) params.get('tags').split(',').filter(Boolean).forEach(t => state.tags.add(t));
    if (params.has('win')) state.heroWindow = params.get('win');
    sortGroupEl.querySelectorAll('button').forEach(b => b.classList.toggle('active', b.dataset.sort === state.sort));
    chipsEl.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.dom === state.domain));
    certChipsEl.querySelectorAll('.chip').forEach(c => c.classList.toggle('active', c.dataset.cert === state.cert));
    tagPanelInner.querySelectorAll('.tag-chip').forEach(c => c.classList.toggle('active', state.tags.has(c.dataset.tag)));
    heroToggleEl.querySelectorAll('button').forEach(b => b.classList.toggle('active', b.dataset.window === state.heroWindow));
    updateTagToggleLabel();
    if (params.has('card')) {{
      const cid = parseInt(params.get('card'), 10);
      const m = BY_CARD.get(cid);
      if (m) setTimeout(() => openDrawer(m), 50);
    }}
  }}

  function syncURL() {{
    const params = new URLSearchParams();
    if (state.q) params.set('q', state.q);
    if (state.domain !== 'Todos') params.set('dom', state.domain);
    if (state.cert !== 'Todas') params.set('cert', state.cert);
    if (state.sort !== 'name') params.set('sort', state.sort);
    if (state.tags.size) params.set('tags', [...state.tags].join(','));
    if (state.heroWindow !== '30d') params.set('win', state.heroWindow);
    const s = params.toString();
    history.replaceState(null, '', s ? ('#' + s) : location.pathname + location.search);
  }}

  // ---- Filtering & sorting ---------------------------------------------
  function matches(m) {{
    if (state.domain !== 'Todos' && m.d !== state.domain) return false;
    if (state.cert !== 'Todas' && (m.cs || '') !== state.cert) return false;
    if (state.tags.size) {{
      const mtags = new Set(m.tags || []);
      for (const t of state.tags) {{ if (!mtags.has(t)) return false; }}
    }}
    if (!state.q) return true;
    const hay = (
      m.n + ' ' +
      m.t + ' ' +
      (m.b || '') + ' ' +
      (m.tags || []).join(' ') + ' ' +
      'card ' + m.c + ' #' + m.c + ' ' +
      (m.col || '')
    ).toLowerCase();
    return state.q.split(/\\s+/).every(tok => hay.includes(tok));
  }}

  function sortList(list) {{
    const arr = list.slice();
    if (state.sort === 'popular-30d') {{
      arr.sort((a,b) => (b.d30||0) - (a.d30||0) || a.n.localeCompare(b.n, 'es'));
    }} else if (state.sort === 'popular-7d') {{
      arr.sort((a,b) => (b.d7||0) - (a.d7||0) || a.n.localeCompare(b.n, 'es'));
    }} else if (state.sort === 'recent') {{
      arr.sort((a,b) => (b.lu||'').localeCompare(a.lu||'') || a.n.localeCompare(b.n, 'es'));
    }} else {{
      arr.sort((a,b) => a.n.localeCompare(b.n, 'es'));
    }}
    return arr;
  }}

  // ---- Render rows -----------------------------------------------------
  function rowHTML(m) {{
    const certDot = m.cs && CERT_DOT_CLASS[m.cs]
      ? `<span class="cert-dot ${{CERT_DOT_CLASS[m.cs]}}" title="${{escapeAttr(m.cs)}}"></span>`
      : '';
    const copies = m.copies
      ? `<span class="copies">+${{m.copies}} dashboards con el mismo nombre</span>`
      : '';
    return `
      <tr data-c="${{m.c}}" class="dom-${{m.d}}" tabindex="0">
        <td class="col-name">
          <div class="name-row">${{certDot}}${{escapeHTML(m.n)}}</div>
          ${{copies}}
        </td>
        <td class="col-domain"><span class="domain-badge dom-${{m.d}}"><span class="dot"></span>${{m.d}}</span></td>
        <td class="col-tldr"><p>${{escapeHTML(m.t)}}</p></td>
        <td class="col-card">
          <a href="${{escapeHTML(m.u)}}" target="_blank" rel="noopener" onclick="event.stopPropagation()">
            <span>#${{m.c}}</span>
            <svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><path d="M15 3h6v6"/><path d="M10 14 21 3"/></svg>
          </a>
        </td>
      </tr>`;
  }}

  function render() {{
    const filtered = sortList(DATA.filter(matches));
    countEl.textContent = `${{filtered.length.toLocaleString('es')}} de ${{DATA.length.toLocaleString('es')}}`;
    if (filtered.length === 0) {{
      rowsEl.innerHTML = '';
      tableCardEl.style.display = 'none';
      emptyEl.style.display = '';
      return;
    }}
    tableCardEl.style.display = '';
    emptyEl.style.display = 'none';
    const LIMIT = 500;
    const slice = filtered.slice(0, LIMIT);
    let html = slice.map(rowHTML).join('');
    if (filtered.length > LIMIT) {{
      html += `<tr class="show-more-row"><td colspan="4">Mostrando los primeros ${{LIMIT}} de ${{filtered.length.toLocaleString('es')}} resultados. Refina la búsqueda para ver menos.</td></tr>`;
    }}
    rowsEl.innerHTML = html;
  }}

  rowsEl.addEventListener('click', (e) => {{
    const tr = e.target.closest('tr[data-c]');
    if (!tr) return;
    const m = BY_CARD.get(+tr.dataset.c);
    if (!m) return;
    openDrawer(m);
    rowsEl.querySelectorAll('tr.selected').forEach(r => r.classList.remove('selected'));
    tr.classList.add('selected');
  }});
  rowsEl.addEventListener('keydown', (e) => {{
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const tr = e.target.closest('tr[data-c]');
    if (!tr) return;
    e.preventDefault();
    const m = BY_CARD.get(+tr.dataset.c);
    if (m) {{ openDrawer(m); rowsEl.querySelectorAll('tr.selected').forEach(r => r.classList.remove('selected')); tr.classList.add('selected'); }}
  }});

  // ---- Hero rendering --------------------------------------------------
  function deltaPill(delta, isNew) {{
    if (isNew) return `<span class="delta new">✦ Nueva</span>`;
    if (delta == null) return `<span class="delta flat">→ Sin datos</span>`;
    if (delta > 0) return `<span class="delta up">↑ +${{delta.toLocaleString('es')}}</span>`;
    if (delta < 0) return `<span class="delta down">↓ ${{delta.toLocaleString('es')}}</span>`;
    return `<span class="delta flat">→ Estable</span>`;
  }}
  function viewsPill(views) {{
    return `<span class="delta flat">${{(views||0).toLocaleString('es')}} vistas</span>`;
  }}

  function trendCardHTML(m, badge, idx, opts) {{
    opts = opts || {{}};
    const bars = sparklineBars(m.c).map(h => `<span style="height:${{h}}%"></span>`).join('');
    const rel = m.lu ? formatRelative(m.lu) : '';
    const topCls = idx === 0 ? ' is-top' : '';
    return `
      <div class="trend-card dom-${{m.d}}${{topCls}}" data-c="${{m.c}}" role="button" tabindex="0" aria-label="${{escapeAttr(m.n)}}">
        <span class="trend-rank">${{(idx+1).toString().padStart(2,'0')}}</span>
        <div class="trend-name">${{escapeHTML(m.n)}}</div>
        <div class="trend-tldr">${{escapeHTML(m.t)}}</div>
        <div class="trend-spark" aria-hidden="true">${{bars}}</div>
        <div class="trend-foot">
          ${{badge}}
          <span class="trend-relative">${{rel}}</span>
        </div>
      </div>`;
  }}

  function renderHero() {{
    if (!POP) {{ heroEl.hidden = true; return; }}
    heroEl.hidden = false;

    if (POP.degraded) {{
      heroToggleEl.style.display = 'none';
      const rows = (POP.top_lifetime || []).slice(0, 8)
        .map(r => {{ const m = BY_CARD.get(r.card_id); return m ? {{m, views: r.views}} : null; }})
        .filter(Boolean);
      heroGridEl.innerHTML = rows.map(({{m, views}}, i) => trendCardHTML(m, viewsPill(views), i)).join('');
      heroMetaEl.textContent = rows.length
        ? `Top ${{rows.length}} por vistas acumuladas`
        : 'Sin datos disponibles';
      const reasonText = POP.reason === 'no_snapshots_bootstrap'
        ? 'Ranking provisional por vistas acumuladas. Los deltas 7/30d se activan tras el primer snapshot diario.'
        : POP.reason === 'no_snapshots'
          ? 'Aún no hay datos de popularidad.'
          : POP.reason === 'insufficient'
            ? 'Snapshots insuficientes — mostramos vistas acumuladas.'
            : '';
      if (reasonText) {{
        heroBannerEl.hidden = false;
        heroBannerText.textContent = reasonText;
      }} else {{
        heroBannerEl.hidden = true;
      }}
      return;
    }}

    heroToggleEl.style.display = '';
    heroBannerEl.hidden = true;
    const win = state.heroWindow === '7d' ? POP.window_7d : POP.window_30d;
    if (!win) {{
      heroGridEl.innerHTML = '';
      heroMetaEl.textContent = 'Sin baseline suficiente para esta ventana.';
      return;
    }}
    const rows = (win.top || []).slice(0, 8)
      .map(r => {{ const m = BY_CARD.get(r.card_id); return m ? {{m, delta: r.delta, isNew: r.is_new}} : null; }})
      .filter(Boolean);
    heroGridEl.innerHTML = rows.map(({{m, delta, isNew}}, i) => trendCardHTML(m, deltaPill(delta, isNew), i)).join('');
    heroMetaEl.textContent = rows.length
      ? `${{win.from}} → ${{win.to}}`
      : `Sin movimiento del ${{win.from}} al ${{win.to}}`;
    requestAnimationFrame(updateHeroToggleIndicator);
  }}

  heroGridEl.addEventListener('click', (e) => {{
    const el = e.target.closest('.trend-card');
    if (!el) return;
    const m = BY_CARD.get(+el.dataset.c);
    if (m) openDrawer(m);
  }});
  heroGridEl.addEventListener('keydown', (e) => {{
    if (e.key !== 'Enter' && e.key !== ' ') return;
    const el = e.target.closest('.trend-card');
    if (!el) return;
    e.preventDefault();
    const m = BY_CARD.get(+el.dataset.c);
    if (m) openDrawer(m);
  }});
  heroToggleEl.addEventListener('click', (e) => {{
    const btn = e.target.closest('button[data-window]');
    if (!btn) return;
    state.heroWindow = btn.dataset.window;
    heroToggleEl.querySelectorAll('button').forEach(b => b.classList.toggle('active', b === btn));
    updateHeroToggleIndicator();
    syncURL(); renderHero();
  }});

  // ---- Drawer ----------------------------------------------------------
  let currentMetric = null;
  function openDrawer(m) {{
    currentMetric = m;
    $('d-name').textContent = m.n;

    const metaParts = [];
    metaParts.push(`<span class="drawer-meta-item"><span class="domain-badge dom-${{m.d}}"><span class="dot"></span>${{m.d}}</span></span>`);
    if (m.cs) {{
      const dotCls = CERT_DOT_CLASS[m.cs] || 'cert-rev';
      metaParts.push(`<span class="drawer-meta-item"><span class="cert-dot ${{dotCls}}"></span>${{escapeHTML(CERT_SHORT[m.cs] || m.cs)}}</span>`);
    }}
    metaParts.push(`<span class="drawer-meta-item"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg><span class="k">Card</span> #${{m.c}}</span>`);
    metaParts.push(`<span class="drawer-meta-item"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/><circle cx="12" cy="12" r="3"/></svg><span class="k">Vistas</span> ${{(m.v||0).toLocaleString('es')}}</span>`);
    if (m.lu) metaParts.push(`<span class="drawer-meta-item"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg><span class="k">Última</span> ${{escapeHTML(formatRelative(m.lu))}}</span>`);
    if (m.copies) metaParts.push(`<span class="drawer-meta-item"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg><span class="k">Copias</span> +${{m.copies}}</span>`);
    if (m.pv) metaParts.push(`<span class="drawer-meta-item"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 11l3 3L22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg><span class="k">PDF</span> validada</span>`);
    if (m.col) metaParts.push(`<span class="drawer-meta-item"><svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 19a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h5l2 3h9a2 2 0 0 1 2 2z"/></svg>${{escapeHTML(m.col)}}</span>`);
    $('d-meta').innerHTML = metaParts.join('');

    const reviewEl = $('d-review');
    if (m.ar) {{
      $('d-review-text').textContent = m.ar;
      reviewEl.hidden = false;
    }} else {{
      reviewEl.hidden = true;
    }}

    $('d-business').textContent = m.b || 'Sin definición de negocio registrada.';
    $('d-technical').textContent = m.tt || 'Sin traducción técnica registrada.';
    $('d-tags').innerHTML = (m.tags || []).length
      ? m.tags.map(t => `<span class="drawer-tag" data-tag="${{escapeAttr(t)}}">${{escapeHTML(t)}}</span>`).join('')
      : '<span style="color:var(--color-text-muted);font-size:13px">Sin tags</span>';
    $('d-link').href = m.u || '#';

    drawer.classList.add('open');
    drawer.setAttribute('aria-hidden', 'false');
    backdrop.classList.add('open');
  }}

  function closeDrawer() {{
    drawer.classList.remove('open');
    drawer.setAttribute('aria-hidden', 'true');
    backdrop.classList.remove('open');
    rowsEl.querySelectorAll('tr.selected').forEach(r => r.classList.remove('selected'));
    currentMetric = null;
  }}
  closeBtn.addEventListener('click', closeDrawer);
  backdrop.addEventListener('click', closeDrawer);

  // Click on tag inside drawer → close drawer + apply tag filter
  $('d-tags').addEventListener('click', (e) => {{
    const t = e.target.closest('.drawer-tag');
    if (!t) return;
    const tag = t.dataset.tag;
    if (!tag) return;
    state.tags.add(tag);
    tagPanelInner.querySelectorAll('.tag-chip').forEach(c => c.classList.toggle('active', state.tags.has(c.dataset.tag)));
    updateTagToggleLabel();
    closeDrawer();
    syncURL(); render(); updateClearBtn();
  }});

  // Copy link
  copyLinkBtn.addEventListener('click', () => {{
    if (!currentMetric) return;
    const base = location.origin + location.pathname;
    const params = new URLSearchParams();
    params.set('card', currentMetric.c);
    const url = base + '#' + params.toString();
    const fallback = () => {{
      const ta = document.createElement('textarea');
      ta.value = url;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try {{ document.execCommand('copy'); }} catch (_) {{ /* ignore */ }}
      document.body.removeChild(ta);
    }};
    if (navigator.clipboard && navigator.clipboard.writeText) {{
      navigator.clipboard.writeText(url).then(
        () => showToast('Link copiado al portapapeles'),
        () => {{ fallback(); showToast('Link copiado'); }}
      );
    }} else {{
      fallback();
      showToast('Link copiado');
    }}
  }});

  // Shortcuts hint button: focus search
  shortcutsBtn.addEventListener('click', () => {{ qEl.focus(); qEl.select(); }});

  // ---- Keyboard shortcuts ----------------------------------------------
  document.addEventListener('keydown', (e) => {{
    if (e.key === 'Escape') {{ closeDrawer(); return; }}
    if (e.target.matches('input, select, textarea')) return;
    if (e.key === '/') {{ e.preventDefault(); qEl.focus(); qEl.select(); }}
    else if (e.key.toLowerCase() === 'g') {{ e.preventDefault(); heroEl.scrollIntoView({{behavior:'smooth', block:'start'}}); }}
  }});

  // ---- Theme manager ---------------------------------------------------
  const THEME_KEY = 'diccionario-theme';
  const themeToggleBtn = $('theme-toggle');
  function applyTheme(theme) {{
    document.documentElement.setAttribute('data-theme', theme);
    if (themeToggleBtn) {{
      themeToggleBtn.setAttribute('aria-label', theme === 'dark' ? 'Cambiar a modo claro' : 'Cambiar a modo oscuro');
      themeToggleBtn.setAttribute('title', theme === 'dark' ? 'Modo claro' : 'Modo oscuro');
    }}
  }}
  (function initTheme() {{
    let saved = null;
    try {{ saved = localStorage.getItem(THEME_KEY); }} catch (_) {{}}
    applyTheme(saved === 'light' ? 'light' : 'dark');
  }})();
  if (themeToggleBtn) {{
    themeToggleBtn.addEventListener('click', () => {{
      const current = document.documentElement.getAttribute('data-theme') || 'dark';
      const next = current === 'dark' ? 'light' : 'dark';
      applyTheme(next);
      try {{ localStorage.setItem(THEME_KEY, next); }} catch (_) {{}}
    }});
  }}

  // ---- Init ------------------------------------------------------------
  parseURL();
  renderHero();
  render();
  updateSortIndicator();
  updateHeroToggleIndicator();
  updateClearBtn();
  window.addEventListener('resize', () => {{ updateSortIndicator(); updateHeroToggleIndicator(); }});
}})();
</script>
</body>
</html>
"""


def main() -> int:
    if not METRICS_PATH.exists():
        print(f"ERROR: no existe {METRICS_PATH}")
        return 1

    data = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
    entries, exclusions = collect_entries(data.get("metrics", []))
    total_cards = sum(1 + e["copies"] for e in entries)

    popularity = _load_popularity()
    pop_lookup = _popularity_lookup(popularity)
    for e in entries:
        cid = e.get("c")
        if cid is not None and cid in pop_lookup:
            p = pop_lookup[cid]
            if "d7" in p:
                e["d7"] = p["d7"]
            if "d30" in p:
                e["d30"] = p["d30"]
            if "lu" in p:
                e["lu"] = p["lu"]

    payload_json = json.dumps(entries, ensure_ascii=False, separators=(",", ":"))
    payload_json = payload_json.replace("</", "<\\/")

    popularity_json = json.dumps(popularity, ensure_ascii=False, separators=(",", ":")) if popularity else "null"
    popularity_json = popularity_json.replace("</", "<\\/")

    html = HTML_TEMPLATE.format(
        total_metrics=len(entries),
        total_cards=total_cards,
        generated_at=data.get("generated_at", "desconocida"),
        payload_json=payload_json,
        popularity_json=popularity_json,
    )
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(
        f"OK: escrito {OUTPUT_PATH} "
        f"({len(entries)} metricas, {total_cards} cards, {size_kb:.0f} KB)"
    )
    if popularity is None:
        print("  Popularidad: sin datos (corre src.core.extraction.snapshot_popularity para activar hero)")
    elif popularity.get("degraded"):
        print(f"  Popularidad: modo degradado ({popularity.get('reason', 'N/D')})")
    else:
        w30 = popularity.get("window_30d") or {}
        w7 = popularity.get("window_7d") or {}
        n30 = len(w30.get("top") or [])
        n7 = len(w7.get("top") or [])
        print(f"  Popularidad: 30d={n30}  7d={n7}")
    if exclusions:
        print("\nFiltros aplicados (via auditoria_detalle.json):")
        for k, v in sorted(exclusions.items(), key=lambda kv: -kv[1]):
            print(f"  - {k:<28s}: {v:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
