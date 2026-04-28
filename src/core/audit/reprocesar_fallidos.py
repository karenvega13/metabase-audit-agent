"""
reprocesar_fallidos.py
Reprocesa únicamente los batches que quedaron como raw_response (JSON inválido)
después de una corrida completa de audit_agent.py.

Flujo:
1. Lee todos los JSON de resultados en ./resultados/
2. Identifica batches con raw_response (JSON cortado/inválido)
3. Localiza las cards originales en ./lotes/
4. Re-manda solo esos batches a OpenAI con MAX_TOKENS=8192
5. Actualiza el JSON de resultados in-place

Uso:
    python reprocesar_fallidos.py
    python reprocesar_fallidos.py --dry-run    # solo muestra qué va a reprocesar, sin llamar API
"""

import os
import re
import json
import time
import argparse
from openai import OpenAI
from datetime import datetime
from pathlib import Path

# Importar funciones compartidas de audit_agent
import sys
sys.path.insert(0, str(Path(__file__).parent))
from audit_agent import (
    load_context, parse_lote, build_system_prompt,
    compute_score, validate_card_ids,
    CONTEXT_FILE, LOTES_DIR, OUTPUT_DIR,
    MODEL_FULL, MODEL_MINI, LOTE_MINI_FROM,
    COST_PER_TOKEN_FULL, COST_PER_TOKEN_MINI,
)

MAX_TOKENS_RETRY = 8192   # más tokens para evitar cortes
MAX_RETRIES      = 3


def find_failed_batches(results_dir: str) -> list:
    """
    Escanea todos los JSON de resultados y devuelve lista de batches fallidos.
    Un batch fallido tiene una entrada con "raw_response" en lugar de "card_id".
    """
    failed = []
    skipped_corrupted = []
    for json_path in sorted(Path(results_dir).glob("*_resultados.json")):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            skipped_corrupted.append(f"{json_path.name}: {e}")
            continue

        lote_name = data.get("lote", json_path.stem)
        cards = data.get("cards", [])

        for i, entry in enumerate(cards):
            if isinstance(entry, dict) and "raw_response" in entry:
                failed.append({
                    "json_path":  str(json_path),
                    "lote_name":  lote_name,
                    "entry_idx":  i,
                    "card_ids":   entry.get("cards", []),
                    "raw_response": entry.get("raw_response", ""),
                })

    if skipped_corrupted:
        print(f"\n⚠️  {len(skipped_corrupted)} JSON(s) corruptos omitidos (raw_response con escape inválido):")
        for msg in skipped_corrupted:
            print(f"   - {msg}")

    return failed


def find_lote_file(lote_name: str) -> Path | None:
    """Busca el archivo .sql correspondiente al nombre del lote."""
    lote_dir = Path(LOTES_DIR)
    # Buscar coincidencia exacta primero
    exact = lote_dir / f"{lote_name}.sql"
    if exact.exists():
        return exact
    # Buscar por prefijo (ej: lote_03 → lote_03_algo.sql)
    matches = list(lote_dir.glob(f"{lote_name}*.sql"))
    return matches[0] if matches else None


def retry_batch(client: OpenAI, system_prompt: str, cards: list, model: str) -> tuple:
    """Reintenta un batch fallido con MAX_TOKENS_RETRY."""
    cards_text = ""
    for card in cards:
        cards_text += f"\n\n--- Card ID: {card['id']} | {card['name']} | {card['vistas']:,} vistas ---\n"
        cards_text += card["sql"]

    for attempt in range(MAX_RETRIES):
        try:
            message = client.chat.completions.create(
                model=model,
                max_tokens=MAX_TOKENS_RETRY,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Audita estas {len(cards)} queries SQL de Metabase:\n{cards_text}"}
                ]
            )

            tokens_used   = message.usage.total_tokens if message.usage else 0
            response_text = message.choices[0].message.content.strip()
            response_text = re.sub(r"^```json\s*", "", response_text)
            response_text = re.sub(r"\s*```$", "", response_text)

            results = json.loads(response_text)
            if not isinstance(results, list):
                raise ValueError("La respuesta no es una lista JSON")

            for r in results:
                if isinstance(r, dict) and "hallazgos" in r:
                    r["score_salud"] = compute_score(r["hallazgos"])

            validation = validate_card_ids(cards, results)
            return results, tokens_used, validation, None

        except json.JSONDecodeError as e:
            if attempt < MAX_RETRIES - 1:
                print(f"      ⚠️  JSON inválido (intento {attempt+1}/{MAX_RETRIES}) — reintentando...")
                time.sleep(5)
                continue
            return None, 0, {}, f"JSONDecodeError tras {MAX_RETRIES} intentos: {e}"

        except Exception as e:
            err_str = str(e)
            if any(code in err_str for code in ["429", "500", "503"]):
                wait = (2 ** attempt) * 5
                print(f"      ⏳ Error API — reintentando en {wait}s")
                time.sleep(wait)
            else:
                return None, 0, {}, str(e)

    return None, 0, {}, "Max reintentos alcanzados"


def main():
    parser = argparse.ArgumentParser(description="Reprocesa batches fallidos de audit_agent")
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra qué se va a reprocesar")
    args = parser.parse_args()

    print("=" * 60)
    print("  Reprocesador de batches fallidos — Metabase ExampleCorp")
    print("=" * 60)

    # Buscar fallidos
    failed = find_failed_batches(OUTPUT_DIR)
    if not failed:
        print("✓ No hay batches fallidos. Todo OK.")
        return

    print(f"\n⚠️  {len(failed)} batch(es) fallido(s) encontrado(s):\n")
    for f in failed:
        print(f"  • {f['lote_name']} — {len(f['card_ids'])} cards: {f['card_ids']}")

    if args.dry_run:
        print("\n[dry-run] Sin cambios. Quita --dry-run para reprocesar.")
        return

    # Cargar contexto y cliente
    context = load_context(CONTEXT_FILE)
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        api_key = input("\n🔑 OpenAI API key: ").strip()

    client        = OpenAI(api_key=api_key)
    system_prompt = build_system_prompt(context)

    total_tokens_full = 0
    total_tokens_mini = 0
    resueltos = 0

    for failed_batch in failed:
        lote_name = failed_batch["lote_name"]
        card_ids  = failed_batch["card_ids"]
        json_path = failed_batch["json_path"]
        entry_idx = failed_batch["entry_idx"]

        print(f"\n🔄 Reprocesando {lote_name} — cards {card_ids}...")

        # Localizar el SQL original
        lote_file = find_lote_file(lote_name)
        if not lote_file:
            print(f"   ✗ No se encontró el archivo SQL para {lote_name}")
            continue

        # Parsear todas las cards del lote y filtrar las que necesitamos
        all_cards = parse_lote(str(lote_file))
        batch_cards = [c for c in all_cards if c["id"] in card_ids]

        if not batch_cards:
            print(f"   ✗ No se encontraron las cards {card_ids} en {lote_file.name}")
            continue

        # Determinar modelo según número de lote
        lote_num = int(re.search(r"lote_(\d+)", lote_name).group(1))
        model    = MODEL_MINI if lote_num >= LOTE_MINI_FROM else MODEL_FULL

        print(f"   {len(batch_cards)} cards | modelo: {model} | max_tokens: {MAX_TOKENS_RETRY}")

        results, tokens, validation, error = retry_batch(client, system_prompt, batch_cards, model)

        if error:
            print(f"   ✗ Falló de nuevo: {error}")
            continue

        # Actualizar el JSON de resultados in-place
        with open(json_path, "r", encoding="utf-8") as f:
            lote_data = json.load(f)

        # Reemplazar la entrada raw_response con los resultados nuevos
        lote_data["cards"][entry_idx:entry_idx+1] = results

        # Recalcular totales
        all_card_results = [c for c in lote_data["cards"] if isinstance(c, dict) and "card_id" in c]
        lote_data["total_hallazgos"] = sum(len(c.get("hallazgos", [])) for c in all_card_results)
        lote_data["total_tokens"]    = lote_data.get("total_tokens", 0) + tokens
        lote_data["reprocesado_en"]  = datetime.now().isoformat()

        cost_rate = COST_PER_TOKEN_FULL if model == MODEL_FULL else COST_PER_TOKEN_MINI
        lote_data["costo_estimado"]  = round(
            lote_data.get("costo_estimado", 0) + tokens * cost_rate, 5
        )

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(lote_data, f, ensure_ascii=False, indent=2, default=str)

        alucinados = validation.get("alucinados", [])
        omitidos   = validation.get("omitidos", [])
        status = f"✓ {len(results)} cards resueltas | {tokens:,} tokens"
        if alucinados: status += f" | ⚠️ alucinados: {alucinados}"
        if omitidos:   status += f" | ⚠️ omitidos: {omitidos}"
        print(f"   {status}")

        if model == MODEL_FULL:
            total_tokens_full += tokens
        else:
            total_tokens_mini += tokens
        resueltos += 1

    costo_full  = total_tokens_full * COST_PER_TOKEN_FULL
    costo_mini  = total_tokens_mini * COST_PER_TOKEN_MINI
    costo_total = costo_full + costo_mini

    print(f"\n{'=' * 60}")
    print(f"  ✓ {resueltos}/{len(failed)} batches resueltos")
    print(f"  🪙 Tokens adicionales: {total_tokens_full + total_tokens_mini:,}")
    print(f"  💰 Costo adicional: ~${costo_total:.4f} USD")
    print(f"{'=' * 60}")
    print(f"\nCorre audit_agent.py de nuevo (sin borrar JSONs) para regenerar el reporte final.")


if __name__ == "__main__":
    main()
