"""
src/core/catalog/enrichment.py — Enriquecimiento semántico con LLM (gpt-4o-mini).

Características:
- Cache persistente en data/processed/diccionario/enrichment_cache.json
  → Las cards ya enriquecidas no se reprocesarán en ejecuciones futuras.
- Procesamiento por lotes de BATCH_SIZE cards para reducir latencia.
- Respuesta en JSON estructurado (business_definition + technical_translation).
"""

import json
import os
import sys
import time
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parents[3]
CACHE_FILE = _ROOT / "data" / "processed" / "diccionario" / "enrichment_cache.json"

BATCH_SIZE = 8           # Cards por llamada LLM
MAX_SQL_CHARS = 1200     # Truncar SQL para no gastar tokens
MODEL = "gpt-4o-mini"


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------

def load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Enrichment logic
# ---------------------------------------------------------------------------

def _build_system_prompt() -> str:
    """
    Construye el system prompt inyectando el ground truth canónico del PDF de training
    de ExampleCorp para que el LLM genere definiciones alineadas con la terminología oficial.
    """
    canonical_file = _ROOT / "data" / "raw" / "ground_truth" / "pdf_metrics_canonical.json"
    try:
        canonical = json.loads(canonical_file.read_text(encoding="utf-8"))
        tax = canonical.get("taxonomies", {})
        lup = tax.get("lead_user_pricing", {})
        tax_1tu = tax.get("1tu_mtu", {})
        tax_nr = tax.get("nuevo_recurrente", {})
        states = canonical.get("user_states", {})

        context = (
            "\n\n## Contexto de negocio ExampleCorp (OBLIGATORIO respetar)\n\n"
            "### Las 3 Taxonomías críticas\n"
            f"1. **Lead Pricing / User Pricing** — ancla: `counter_auth`\n"
            f"   - Lead Pricing: counter_auth = 1 → comisión $0, primera autorización del usuario\n"
            f"   - User Pricing: counter_auth >= 2 → comisión $2.99-$3.99 + FX take rate 1.1%\n"
            f"   - REGLA DE ORO: el precio se fija en la Autorización, NO en el pago\n"
            f"2. **1TU / MTU** — ancla: `counter_paid`\n"
            f"   - 1TU (First Transaction User): counter_paid = 1\n"
            f"   - MTU (Multi-Transaction User): counter_paid >= 2\n"
            f"3. **Nuevo / Recurrente** — ancla: fecha de primera TXN Paid relativa al período\n"
            f"   - Nuevo: primera TXN Paid dentro del período medido\n"
            f"   - Recurrente: primera TXN Paid ANTES del período, pero también pagó en el período\n\n"
            "### Estados de usuario\n"
            "- Activo: 0-30 días desde última TXN Paid\n"
            "- Inactivo Cálido: 30-90 días + inició conversación en últimos 30d\n"
            "- Inactivo Frío: 30-90 días sin conversación\n"
            "- Churned: >90 días desde última TXN Paid\n\n"
            "### Reglas inviolables\n"
            "- MAU/WAU/DAU cuentan SOLO `transaction_status = 'Paid'`, nunca Auth ni Sent\n"
            "- 'Sent' es pre-autorización, NO es un estado autorizado\n"
            "- Los 9 statuses autorizados: Hold, Paid, Payable, Cancelled, Client_refund, Chargeback, Chargeback_won, Chargeback_lost, Chargeback_unir\n"
            "- ATV se desglosa en Lead Pricing (counter_auth=1) y User Pricing (counter_auth>=2)\n"
            "- Retención ≠ Recurrente. Retención es una tasa de cohorte M0→Mn.\n"
            "- 'Rejection Rate' fue ELIMINADA el 10/04/26 — marcar como Deprecada\n"
        )
    except Exception:
        context = ""

    return (
        "Eres un experto en Data Governance y analítica de pagos digitales. "
        "Recibirás métricas de Metabase de ExampleCorp (fintech de remesas latinoamericana). "
        "Responde SIEMPRE con JSON válido, sin texto fuera del JSON."
        + context
    )


_SYSTEM_PROMPT = _build_system_prompt()

_ITEM_PROMPT = """\
Analiza esta métrica y genera:
- "business_definition": 1-2 oraciones claras para negocio (en español)
- "technical_translation": explicación humana de los filtros WHERE/CASE más relevantes (en español, máx 2 oraciones)

Nombre: {name}
Tablas: {tables}
SQL:
{sql}"""

_VARIATION_ITEM_PROMPT = """\
Esta card es una variación de "{parent_name}". Su nombre específico es "{name}".
Genera definiciones propias para ESTA variación, NO para "{parent_name}":
- "business_definition": 1-2 oraciones claras para negocio (en español), específicas para "{name}"
- "technical_translation": explicación humana de los filtros WHERE/CASE que la diferencian (en español, máx 2 oraciones)

Nombre: {name}
Variante de: {parent_name}
Tablas: {tables}
SQL:
{sql}"""


def _build_batch_prompt(batch: list[dict]) -> str:
    items = []
    for i, card in enumerate(batch, 1):
        tables_str = ", ".join(sorted(card.get("tables", []))) or "N/A"
        sql_excerpt = card.get("sql_raw", "")[:MAX_SQL_CHARS]
        if card.get("is_variation") and card.get("parent_card_name"):
            template = _VARIATION_ITEM_PROMPT.format(
                name=card.get("card_name", ""),
                parent_name=card["parent_card_name"],
                tables=tables_str,
                sql=sql_excerpt,
            )
        else:
            template = _ITEM_PROMPT.format(
                name=card.get("card_name", ""),
                tables=tables_str,
                sql=sql_excerpt,
            )
        items.append(f"--- MÉTRICA {i} (card_id={card['card_id']}) ---\n" + template)

    return (
        "\n\n".join(items)
        + f"\n\nDevuelve un JSON con la forma: "
        + '{"results": [{"card_id": <id>, "business_definition": "...", "technical_translation": "..."}, ...]}'
    )


def _empty_enrichment() -> dict:
    return {"business_definition": "", "technical_translation": ""}


def enrich_batch(
    batch: list[dict],
    client,
    cache: dict,
    retries: int = 3,
) -> dict[int, dict]:
    """
    Enriquece un lote de cards. Devuelve dict {card_id: {business_definition, technical_translation}}.
    Cards ya en cache no se reprocesarán.
    """
    def _cache_key(card: dict) -> str:
        if card.get("is_variation"):
            return f"enrich_var_{card['card_id']}"
        return f"enrich_{card['sql_fingerprint']}"

    # Separar los que ya tienen cache
    pending = [c for c in batch if _cache_key(c) not in cache]
    result: dict[int, dict] = {}

    # Resolver desde cache
    for card in batch:
        key = _cache_key(card)
        if key in cache:
            result[card["card_id"]] = cache[key]

    if not pending:
        return result

    prompt = _build_batch_prompt(pending)

    for attempt in range(retries):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": _SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
                timeout=60,
            )
            raw = json.loads(response.choices[0].message.content)
            items = raw.get("results", [])

            # Almacenar resultados y actualizar cache
            id_to_card = {c["card_id"]: c for c in pending}
            matched_ids = set()

            for item in items:
                cid = item.get("card_id")
                if cid and cid in id_to_card:
                    enriched = {
                        "business_definition": item.get("business_definition", ""),
                        "technical_translation": item.get("technical_translation", ""),
                    }
                    result[cid] = enriched
                    cache[_cache_key(id_to_card[cid])] = enriched
                    matched_ids.add(cid)

            # Cards que el LLM no retornó
            for card in pending:
                if card["card_id"] not in matched_ids:
                    result[card["card_id"]] = _empty_enrichment()

            return result

        except Exception as exc:
            if attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  [RETRY {attempt+1}] Error LLM: {exc}. Reintentando en {wait}s...")
                time.sleep(wait)
            else:
                print(f"  [ERROR] Falló enriquecimiento tras {retries} intentos: {exc}")
                for card in pending:
                    result[card["card_id"]] = _empty_enrichment()

    return result


def enrich_all(
    cards_to_enrich: list[dict],
    cache: dict,
    verbose: bool = True,
) -> dict[int, dict]:
    """
    Enriquece todas las cards únicas/primarias en lotes de BATCH_SIZE.
    Requiere OPENAI_API_KEY en el entorno.
    """
    sys.path.insert(0, str(_ROOT))
    from src.utils.env_loader import load_env

    load_env()
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        print("  [WARN] OPENAI_API_KEY no encontrada. Enriquecimiento omitido.", file=sys.stderr)
        return {c["card_id"]: _empty_enrichment() for c in cards_to_enrich}

    try:
        from openai import OpenAI
        client = OpenAI(api_key=api_key)
    except ImportError:
        print("  [WARN] openai no instalado. Enriquecimiento omitido.", file=sys.stderr)
        return {c["card_id"]: _empty_enrichment() for c in cards_to_enrich}

    total = len(cards_to_enrich)
    all_results: dict[int, dict] = {}

    for i in range(0, total, BATCH_SIZE):
        batch = cards_to_enrich[i : i + BATCH_SIZE]
        batch_result = enrich_batch(batch, client, cache)
        all_results.update(batch_result)

        processed = min(i + BATCH_SIZE, total)
        if verbose and (processed % 50 == 0 or processed == total):
            pct = processed / total * 100
            cached = sum(1 for v in all_results.values() if v.get("business_definition"))
            print(f"  Enriquecimiento: {processed}/{total} ({pct:.0f}%) — {cached} con definición")
            save_cache(cache)

    save_cache(cache)
    return all_results
