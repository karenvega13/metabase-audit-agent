"""Genera docs/glosario.md desde metrics_master.json.

Entregable D1 del backlog de mejoras (docs/mejoras.md): glosario alfabetico
por dominio (Negocio/Producto/Ops) con TL;DR de 2 lineas para consumo
de equipos no-tecnicos.

Invocar: python -m src.core.catalog.build_glosario
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
METRICS_PATH = ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
OUTPUT_PATH = ROOT / "docs" / "glosario.md"

DOMAIN_PRIORITY = [
    ("Negocio",  ["#ventas", "#fees", "#fx", "#mto", "#payouts",
                  "#lead-pricing", "#user-pricing", "#reembolsos"]),
    ("Producto", ["#nuevo-usuario", "#recurrente", "#retencion",
                  "#conversion", "#funnel", "#1tu", "#mtu"]),
    ("Ops",      ["#pagos", "#transacciones", "#fraude", "#ops", "#merchants"]),
]
DOMAIN_FALLBACK = "General"

TLDR_MAX_CHARS = 140


def assign_domain(tags: list[str] | None) -> str:
    tagset = set(tags or [])
    for domain, priority_tags in DOMAIN_PRIORITY:
        if tagset.intersection(priority_tags):
            return domain
    return DOMAIN_FALLBACK


def build_tldr(business_definition: str | None) -> str:
    if not business_definition:
        return "_sin definicion de negocio_"
    text = business_definition.strip().replace("\n", " ")
    # first sentence heuristic: split on .!? followed by space or EOS
    match = re.split(r"(?<=[.!?])\s+", text, maxsplit=1)
    first = match[0].strip() if match else text
    if len(first) <= TLDR_MAX_CHARS:
        return first
    cut = first[:TLDR_MAX_CHARS].rsplit(" ", 1)[0]
    return cut.rstrip(",;:") + "..."


def is_active(rec: dict) -> bool:
    if not rec.get("is_primary"):
        return False
    if rec.get("certification_status") == "Deprecada":
        return False
    if rec.get("accion") in ("ARCHIVAR", "ELIMINAR", "REVISAR_ELIMINAR"):
        return False
    return True


def md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").strip()


def first_letter(name: str) -> str:
    for ch in name:
        if ch.isalnum():
            return ch.upper()
    return "#"


def main() -> int:
    if not METRICS_PATH.exists():
        print(f"ERROR: no existe {METRICS_PATH}")
        return 1

    data = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
    metrics = data.get("metrics", [])
    generated_at = data.get("generated_at", "desconocida")

    # Group by lowercased-stripped name to dedupe near-identical metrics
    # that live in multiple dashboards.
    grouped: dict[str, list[dict]] = defaultdict(list)
    for rec in metrics:
        if not is_active(rec):
            continue
        name = (rec.get("primary_card_name") or "").strip()
        if not name:
            continue
        grouped[name.lower()].append(rec)

    entries = []
    for key, recs in grouped.items():
        # representative = card with most views
        recs.sort(key=lambda r: r.get("vistas_primary") or 0, reverse=True)
        rep = recs[0]
        # merge tags across the group for domain assignment
        merged_tags = []
        seen = set()
        for r in recs:
            for t in (r.get("tags") or []):
                if t not in seen:
                    seen.add(t)
                    merged_tags.append(t)
        entries.append({
            "name": (rep.get("primary_card_name") or "").strip(),
            "card_id": rep.get("primary_card_id"),
            "url": rep.get("url") or "",
            "collection": rep.get("collection") or "",
            "domain": assign_domain(merged_tags),
            "tldr": build_tldr(rep.get("business_definition")),
            "vistas": rep.get("vistas_primary") or 0,
            "n_copies": len(recs),
        })

    entries.sort(key=lambda e: (e["name"].lower(), -e["vistas"]))

    by_letter = defaultdict(list)
    for e in entries:
        by_letter[first_letter(e["name"])].append(e)

    letters_sorted = sorted(by_letter.keys())

    # counts per domain for header
    domain_counts = defaultdict(int)
    for e in entries:
        domain_counts[e["domain"]] += 1

    lines = []
    total_cards_covered = sum(e["n_copies"] for e in entries)
    lines.append("# Glosario de Metricas")
    lines.append(
        f"**Generado:** {generated_at} | **Metricas distintas:** {len(entries)} "
        f"(cubren {total_cards_covered} cards activas) "
        f"| **Fuente:** `data/processed/diccionario/metrics_master.json`"
    )
    lines.append("")
    lines.append(
        "Indice alfabetico de metricas activas (excluye deprecadas, archivadas y "
        "marcadas para eliminar). Para la ficha tecnica completa, seguir el link "
        "de la card a Metabase o consultar `data/processed/diccionario/wiki/`."
    )
    lines.append("")
    lines.append("## Dominios")
    lines.append("")
    lines.append("| Dominio | Que cubre | Cantidad |")
    lines.append("|---|---|---|")
    lines.append(
        f"| **Negocio** | Revenue, fees, FX, payouts, pricing, reembolsos | "
        f"{domain_counts.get('Negocio', 0)} |"
    )
    lines.append(
        f"| **Producto** | Usuarios nuevos/recurrentes, retencion, conversion, funnel, MTU | "
        f"{domain_counts.get('Producto', 0)} |"
    )
    lines.append(
        f"| **Ops** | Pagos, transacciones, fraude, operacion, merchants | "
        f"{domain_counts.get('Ops', 0)} |"
    )
    lines.append(
        f"| **General** | Metricas sin tag de dominio especifico | "
        f"{domain_counts.get('General', 0)} |"
    )
    lines.append("")

    lines.append("## Navegar por letra")
    lines.append("")
    lines.append(" | ".join(f"[{L}](#letra-{L.lower()})" for L in letters_sorted))
    lines.append("")

    for L in letters_sorted:
        lines.append(f"## Letra {L}")
        lines.append(f"<a id=\"letra-{L.lower()}\"></a>")
        lines.append("")
        lines.append("| Termino | Dominio | TL;DR | Card canonica |")
        lines.append("|---|---|---|---|")
        for e in by_letter[L]:
            card_link = (
                f"[#{e['card_id']}]({e['url']})" if e["url"] else f"#{e['card_id']}"
            )
            copies = (
                f" _(+{e['n_copies'] - 1} dashboards)_" if e["n_copies"] > 1 else ""
            )
            lines.append(
                f"| **{md_escape(e['name'])}** "
                f"| {e['domain']} "
                f"| {md_escape(e['tldr'])} "
                f"| {card_link}{copies} |"
            )
        lines.append("")

    OUTPUT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"OK: escrito {OUTPUT_PATH} ({len(entries)} entradas, {len(letters_sorted)} letras)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
