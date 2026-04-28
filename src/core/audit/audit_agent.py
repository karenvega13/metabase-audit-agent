"""
audit_agent.py
Agente auditor de dashboards de Metabase.
Procesa los lotes de cards con GPT-4o usando el contexto de analytics_db.

Uso:
    python audit_agent.py                          # procesa todos los lotes
    python audit_agent.py --lote 01                # procesa solo el lote 01
    python audit_agent.py --lote 01 --lote 02      # procesa lotes específicos
    python audit_agent.py --reaudit 198 345 652    # re-audita solo esas cards (post-fix)
    python audit_agent.py --reaudit-file ids.txt   # re-audita cards listadas en archivo

Outputs en ./resultados/
    - lote_01_resultados.json
    - lote_02_resultados.json
    - ...
    - reporte_final.md

Outputs de re-auditoría en ./resultados/reaudit/<timestamp>/
    - lote_XX_reaudit.json
    - reporte_reauditoria.md     ← comparación antes/después con score y hallazgos

"""

import os
import re
import json
import sys
import time
import argparse
from openai import OpenAI
from datetime import datetime
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# ============================================================
# CONFIG
# ============================================================

_ROOT = Path(__file__).resolve().parent.parent.parent.parent  # src/core/audit/ → raíz del proyecto

LOTES_DIR      = str(_ROOT / "data" / "raw" / "lotes")
CONTEXT_FILE   = str(_ROOT / "config" / "app_context.md")
OUTPUT_DIR     = str(_ROOT / "data" / "processed" / "resultados")
REAUDIT_DIR    = str(_ROOT / "data" / "processed" / "resultados" / "reaudit")

MODEL_FULL     = "gpt-4o"        # lotes 01-54
MODEL_MINI     = "gpt-4o-mini"   # lotes 55-61 (cards con 0 vistas)
LOTE_MINI_FROM = 55              # a partir de este número de lote se usa mini

MAX_TOKENS     = 8192   # subido de 4096 — evita que el JSON se corte en batches con muchos hallazgos

# Costo estimado por token (blended input+output)
COST_PER_TOKEN_FULL = 0.0000025   # gpt-4o ~$2.50/1M
COST_PER_TOKEN_MINI = 0.00000038  # gpt-4o-mini ~$0.38/1M (blended)

# Cuántas cards mandar por llamada
CARDS_PER_CALL = 10

# Score determinístico: penalización por severidad de hallazgo
SCORE_PENALTY = {"alta": 25, "media": 10, "baja": 5}


# ============================================================
# SETUP
# ============================================================

def load_context(filepath: str) -> str:
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"No se encontró {filepath} — necesitas app_context.md")
    with open(filepath, "r", encoding="utf-8") as f:
        return f.read()


def parse_lote(filepath: str) -> list:
    """
    Lee un archivo de lote y devuelve lista de cards con nombre, id, vistas y SQL.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    cards = []
    blocks = re.split(r"\n(?=-- Card: )", content)

    for block in blocks:
        lines = block.strip().splitlines()
        if not lines or not lines[0].startswith("-- Card:"):
            continue

        header = lines[0]
        card_match = re.match(r"-- Card: (.+?) \(id:(\d+) \| vistas:([\d,]+)\)", header)
        if not card_match:
            continue

        name     = card_match.group(1).strip()
        card_id  = int(card_match.group(2))
        vistas   = int(card_match.group(3).replace(",", ""))

        sql_lines = []
        for line in lines[2:]:  # skip header y colección
            sql_lines.append(line)

        sql = "\n".join(sql_lines).strip()

        if sql and not sql.startswith("-- [QUERY BUILDER]"):
            cards.append({
                "id":     card_id,
                "name":   name,
                "vistas": vistas,
                "sql":    sql,
            })

    return cards


def get_lote_number(lote_path: Path) -> int:
    """
    Extrae el número de lote del nombre del archivo.
    Ej: lote_55_sin_uso.sql → 55, lote_02.sql → 2
    """
    match = re.match(r"lote_(\d+)", lote_path.stem)
    return int(match.group(1)) if match else 0


def select_model(lote_path: Path) -> str:
    """
    Devuelve el modelo a usar para el lote.
    Lotes >= 55 (cards con 0 vistas) usan gpt-4o-mini para reducir costos ~65%.
    """
    lote_num = get_lote_number(lote_path)
    return MODEL_MINI if lote_num >= LOTE_MINI_FROM else MODEL_FULL


# ============================================================
# SCORE DETERMINÍSTICO (Mejora 6)
# ============================================================

def compute_score(hallazgos: list) -> int:
    """
    Calcula el score de salud de una card a partir de sus hallazgos.
    Score base: 100
    Penalizaciones: -25 por alta, -10 por media, -5 por baja
    Mínimo: 0

    Usar este cálculo en lugar de pedirle el score al modelo elimina
    inconsistencias entre lotes y hace el score auditable.
    """
    score = 100
    for h in hallazgos:
        severidad = h.get("severidad", "baja").lower()
        score -= SCORE_PENALTY.get(severidad, 0)
    return max(0, score)


# ============================================================
# VALIDACIÓN DE CARD IDs (Mejora 7)
# ============================================================

def validate_card_ids(input_cards: list, results: list) -> dict:
    """
    Cruza los IDs del input (lo que mandamos al modelo) contra
    los IDs del output (lo que el modelo devolvió).

    Detecta:
      - alucinados:  IDs en el output que NO estaban en el input
      - omitidos:    IDs en el input que NO aparecen en el output

    Devuelve un dict con los dos conjuntos (como listas).
    """
    input_ids  = {c["id"] for c in input_cards}
    output_ids = {
        r["card_id"] for r in results
        if isinstance(r, dict) and "card_id" in r
    }

    alucinados = sorted(output_ids - input_ids)
    omitidos   = sorted(input_ids - output_ids)

    return {"alucinados": alucinados, "omitidos": omitidos}


# ============================================================
# OPENAI API
# ============================================================

def build_system_prompt(context: str) -> str:
    return f"""Eres un auditor experto en SQL y análisis de datos para ExampleCorp, una fintech mexicana de remesas internacionales.

Tu tarea es auditar queries SQL de dashboards de Metabase y detectar problemas de calidad, performance y lógica de negocio.

CONTEXTO DE NEGOCIO — LEER ANTES DE AUDITAR:
- El array OFICIAL de "transacciones autorizadas" (confirmado por el CTO) es EXACTAMENTE estos 9 status:
  'Hold', 'Payable', 'Cancelled', 'Client_refund', 'Paid', 'Chargeback', 'Chargeback_won', 'Chargeback_lost', 'Chargeback_unir'
- 'Sent' e 'Injected' son status de PRE-autorización. IMPORTANTE: el ~95% de las cards que incluyen 'Sent' en sus filtros lo hacen INTENCIONALMENTE — es un patrón documentado desde 2024 (confirmado por CTO, Analytics). 'Sent' = 'Injected' en BD. Solo reportar P1 con 'Sent' cuando: (a) el título dice explícitamente "authorized" SIN ningún componente de funnel/yield/tasa, Y (b) no hay lógica de denominador/conversión en la query. NO reportar P1 si la card mide yield, tasa de éxito, rechazo, intentos, o tiene 'Sent' solo en el denominador.
- Una query que filtre solo 'Paid' cuando el título dice "authorized" tiene un desajuste confirmado (validado con manager Data el 27 Mar 2026). El fix correcto es RENOMBRAR EL TÍTULO para que diga "paid" en lugar de "authorized" — no necesariamente expandir el filtro, ya que la card puede estar midiendo intencionalmente solo transacciones pagadas. Reportar siempre como: "título dice 'authorized' pero el filtro es solo 'Paid' — renombrar título a 'paid'".
- ATV (Average Transaction Value), MAU, QAU, WAU, DAU en ExampleCorp usan EXCLUSIVAMENTE transaction_status = 'Paid' o counter_paid >= 1. NO aplicar el array de 9 authorized para estas métricas. Lo mismo aplica para NPS (encuesta a usuarios con status = 'Paid'). Si una card usa solo 'Paid' y el título o contexto menciona ATV, MAU, DAU, WAU, QAU, NPS, usuarios activos, o retención → es correcto, NO reportar como error.
- counter IN (0,1) es un patrón VÁLIDO e intencional para capturar el desfase de timing entre counter y counter_paid (17,597+ registros afectados). NO reportar como error.
- El LEFT JOIN con tabla 'mto' aparece en muchas queries como herencia de copy-paste — NO reportar como hallazgo crítico.
- Las queries que recalculan user_label via CASE WHEN counter=1 pueden ser intencionales si transaction_details.user_label no es confiable en ese contexto.
- disbursement_type en la BD tiene naming mixto REAL e intencional: 'Account Credit' (175k), 'Cash Pickup' (91k), 'bankDeposit' (1.1k), 'directCash' (789), 'mobileWallet' (124). El casing mixto NO es un error a corregir — es el estado real de la BD. NO reportar como error de homologación.
- Cards de funnel webchat / Lead→1TU / conversión: miden etapas del proceso de adquisición (Lead, Quote, Initiated, Sent, 1TU). Estas cards INTENCIONALMENTE no usan el array de 9 authorized — su propósito es medir el funnel, no las transacciones finales. NO reportar como P2.
- status 'Payment' NO existe en la BD (ni en transaction_details ni en transaction). Si una card usa transaction_status = 'Payment', sí es un error real — reportar como P9/error de negocio.
- Los siguientes patrones son deuda técnica sistémica o estilo, NO son errores de datos: SQL comentado (-- o /* */), filtros opcionales de Metabase ([[AND ...]]), ORDER BY en subquery sin LIMIT, subqueries o JOINs redundantes sin impacto en resultados. Ninguno afecta la correctitud del output. NO reportar como hallazgo de datos.
- status 'Rejected' SÍ existe en transaction_details (~40 registros, fraude). NO reportar como status inexistente.
- El NOMBRE de la card define su scope. Si el nombre dice "Paid", "NPS", "ATV", "MAU", "Funnel", "Leads", "Injected", "Intentos", "Yield", el filtro de SQL DEBE concordar con ese nombre — no con el array de 9 authorized. Si hay concordancia entre nombre y SQL → no reportar P3.
- DISTINGUE EXPLÍCITAMENTE entre: (a) error que afecta números hoy, (b) workaround intencional documentado, (c) deuda técnica que no afecta resultados actuales.

{context}

---
EJEMPLOS DE CLASIFICACIÓN CORRECTA (few-shot):
Usa estos ejemplos para calibrar tu criterio. El ~73% de los hallazgos del modelo son falsos positivos; estos casos te muestran cuándo NO reportar. En particular: el ~95% de los casos donde 'Sent' aparece en un filtro son intencionales — solo reportar P1 cuando el contexto sea claramente un error (ver Ejemplo 4 vs. Ejemplo 3 y 7).

## EJEMPLO 1 — P9 FALSO POSITIVO (ATV/MAU usan solo 'Paid' → correcto)
SQL:
  SELECT COUNT(DISTINCT user_id) FROM transaction_details
  WHERE transaction_status = 'Paid' AND counter_paid >= 1
  AND created_date >= CURRENT_DATE - INTERVAL '7 days'
Título: "Weekly Active Users (ATV)"
Respuesta correcta → hallazgos: []
Por qué: ATV y MAU en ExampleCorp se definen EXCLUSIVAMENTE con transaction_status = 'Paid' (confirmado por Head de Marketing y definición de negocio 04/04/26). No usar los 9 status de authorized para estas métricas.

## EJEMPLO 2 — P9 VERDADERO POSITIVO (filtro real incompleto para "authorized")
SQL:
  SELECT COUNT(*) FROM transaction_details
  WHERE transaction_status IN ('Paid', 'Hold', 'Payable')
Título: "# Authorized Transactions by Month"
Respuesta correcta → reportar P9 severidad alta:
  descripcion: "Filtro de status incompleto para métricas de authorized: falta 'Cancelled', 'Client_refund', 'Chargeback', 'Chargeback_won', 'Chargeback_lost', 'Chargeback_unir'"
  recomendacion: "Usar el array completo de 9 status: IN ('Hold','Payable','Cancelled','Client_refund','Paid','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir')"

## EJEMPLO 3 — P1 FALSO POSITIVO (array correcto de 9 — 'Sent' ausente es intencional)
SQL:
  WHERE transaction_status IN (
    'Hold','Payable','Cancelled','Client_refund','Paid',
    'Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'
  )
Título: "# Authorized Transactions"
Respuesta correcta → hallazgos: []
Por qué: El array usa exactamente los 9 status oficiales. 'Sent' = pre-autorización y su ausencia aquí es CORRECTA. No reportar P1.

## EJEMPLO 4 — P1 VERDADERO POSITIVO ('Sent' incluido en filtro de "authorized")
SQL:
  WHERE transaction_status IN (
    'Hold','Payable','Cancelled','Paid','Sent','Client_refund',
    'Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'
  )
Título: "Volume Authorized USD"
Respuesta correcta → reportar P1 severidad alta:
  descripcion: "'Sent' está incluido en el filtro de authorized — es un status de pre-autorización que sobrecontará las métricas"
  recomendacion: "Eliminar 'Sent' del IN clause. El array oficial no lo incluye."

## EJEMPLO 5 — P2 FALSO POSITIVO (funnel intencionalmente sin filtro de authorized)
SQL:
  SELECT COUNT(DISTINCT user_id) FROM transaction_details
  WHERE transaction_status IN ('Initiated','Quote','Pl_Start')
  AND created_date >= CURRENT_DATE - INTERVAL '30 days'
Título: "# Users in Quote Step"
Respuesta correcta → hallazgos: []
Por qué: Esta card mide datos de funnel, no transacciones authorized. El objetivo del análisis ES el funnel — no debe filtrarse por los 9 status de authorized.

## EJEMPLO 6 — P2 VERDADERO POSITIVO (tabla transaction_details sin filtro de status)
SQL:
  SELECT COUNT(*) AS total_txs FROM transaction_details
  WHERE created_date >= CURRENT_DATE - INTERVAL '30 days'
Título: "# Authorized Transactions Last 30 Days"
Respuesta correcta → reportar P2 severidad alta:
  descripcion: "transaction_details mezcla ~245k registros de funnel con ~193k transacciones reales. Sin filtro de transaction_status, sobrecontará ~2.3x vs. las transacciones reales."
  fragmento: "SELECT COUNT(*) AS total_txs FROM transaction_details WHERE created_date >= ..."
  recomendacion: "Agregar WHERE transaction_status IN ('Hold','Payable','Cancelled','Client_refund','Paid','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir')"

## EJEMPLO 7 — P1 FALSO POSITIVO ('Sent' en denominador de yield/rechazo → intencional)
SQL:
  WITH authorized AS (
    SELECT COUNT(*) FROM transaction_details
    WHERE transaction_status IN ('Hold','Paid','Payable','Cancelled','Client_refund',
      'Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir')
  ),
  intentos AS (
    SELECT COUNT(*) FROM transaction_details
    WHERE transaction_status IN ('Sent','Payable','Paid')
  )
  SELECT authorized.count::float / intentos.count AS yield_rate
Título: "Yield Rate (authorized / intentos)"
Respuesta correcta → hallazgos: []
Por qué: 'Sent' está en el DENOMINADOR de una métrica de yield/conversión. En ExampleCorp, 'Sent' = 'Injected' = intento pre-autorizado. Usar 'Sent' como base de "intentos de transacción" es correcto y documentado. El ~95% de cards con 'Sent' en filtros son de este tipo — yield, rechazo, tasas de conversión.

## EJEMPLO 8 — P9 FALSO POSITIVO (disbursement_type con valores canónicos → correcto)
SQL:
  WHERE disbursement_type IN ('Account Credit', 'Cash Pickup', 'bankDeposit')
Título: "Disbursements by Type"
Respuesta correcta → hallazgos: []
Por qué: 'Account Credit' (175k filas), 'Cash Pickup' (91k), 'bankDeposit' (1.1k) son los valores REALES en analytics_db. El naming mixto ES el dato canónico — no existe un estándar diferente. Nunca reportar como error de homologación o normalización.

## EJEMPLO 9 — P9 VERDADERO POSITIVO ('Sent' en card de "authorized" sin contexto de funnel)
SQL:
  WHERE transaction_status IN ('Sent', 'Paid', 'Payable')
Título: "# Authorized Transactions"
Respuesta correcta → reportar P1 severidad alta:
  descripcion: "'Sent' incluido en filtro de authorized — es pre-autorización y sobrecontará las métricas de transacciones reales"
  recomendacion: "Eliminar 'Sent'. Array correcto: IN ('Hold','Payable','Cancelled','Client_refund','Paid','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir')"

## EJEMPLO 10 — P9 VERDADERO POSITIVO (status no-existente en transaction_details)
SQL:
  WHERE transaction_status IN ('Success', 'Fail', 'Paid')
Título: "Dashboard Calidad de Datos"
Respuesta correcta → reportar P9 severidad alta:
  descripcion: "'Success' y 'Fail' no existen en transaction_details (solo en tabla legada 'transaction'). El filtro es código muerto."
  recomendacion: "Verificar si la query debería apuntar a tabla 'transaction' o reemplazar por status existentes."

## EJEMPLO 11 — FALSO POSITIVO (nombre de card define scope — concordancia título-SQL)
SQL:
  WHERE transaction_status = 'Paid'
Título: "DoD (Txs Paid)"
Respuesta correcta → hallazgos: []
Por qué: El nombre dice "Paid" y el SQL filtra 'Paid'. Concordancia perfecta — no hay desajuste entre título y filtro. No reportar P3.

## EJEMPLO 12 — FALSO POSITIVO (subquery redundante → performance, no error de datos)
SQL:
  SELECT * FROM (SELECT * FROM transaction_details WHERE ...) sub
  WHERE sub.transaction_status = 'Paid'
Título: "Paid Transactions Summary"
Respuesta correcta → hallazgos: []
Por qué: La subquery es redundante (se puede colapsar), pero no afecta los resultados. Es deuda técnica de performance. No reportar como error de datos.

## EJEMPLO 13 — FALSO POSITIVO (card Duplicate — hallazgo de estilo, baja prioridad)
SQL:
  WHERE transaction_status IN ('Paid', 'Hold')
Título: "Main Dashboard KPI — Duplicate-TESTING"
Respuesta correcta → hallazgos: []
Por qué: Cards con "Duplicate" en el nombre son copias de testing/desarrollo. Los hallazgos de optimización en ellas no son accionables — la card principal puede ya tener el problema resuelto. No escalar.

---
ANTES DE REPORTAR CUALQUIER HALLAZGO, verifica mentalmente:
1. ¿El nombre de la card define el scope? (Paid, NPS, MAU, Funnel, Injected, Yield → no aplicar reglas de authorized)
2. ¿'Sent' está en denominador de ratio o en card de funnel/webchat/Lead→1TU? → no reportar P1
3. ¿El hallazgo es de performance/estilo y NO afecta el resultado? (subquery redundante, SQL comentado, ORDER BY sin LIMIT, filtro [[AND ...]]) → no reportar
4. ¿disbursement_type usa 'Account Credit', 'Cash Pickup' u otros valores canónicos? → no reportar
5. ¿La card es ATV/MAU/DAU/WAU/QAU/NPS/retención y filtra solo 'Paid'? → no reportar
Si tienes duda entre error y FP, escribe la descripción como "POSIBLE" y especifica qué información adicional resolvería la ambigüedad. No reportes errores que no puedas observar directamente en el SQL.

---
FORMATO DE RESPUESTA:
Responde ÚNICAMENTE con un array JSON válido. Sin texto adicional, sin markdown, sin explicaciones fuera del JSON.

Estructura por card:
[
  {{
    "card_id": 123,
    "card_name": "nombre de la card",
    "vistas": 1000,
    "hallazgos": [
      {{
        "tipo": "tipo del problema",
        "severidad": "alta | media | baja",
        "descripcion": "qué está mal y por qué es un problema",
        "fragmento": "fragmento relevante del SQL",
        "recomendacion": "qué cambiar exactamente"
      }}
    ]
  }}
]

Si una card no tiene hallazgos, devuelve hallazgos: [].
NUNCA devuelvas texto fuera del array JSON.
NUNCA incluyas un campo score_salud — ese valor se calcula externamente."""


def analyze_cards_batch(
    client: OpenAI,
    system_prompt: str,
    cards: list,
    model: str,
    max_retries: int = 3
) -> tuple:
    """
    Manda un batch de cards a OpenAI y parsea la respuesta.
    Incluye retry con backoff exponencial para errores 429/500.

    Después de parsear el JSON:
      - Inyecta score_salud calculado deterministicamente (Mejora 6)
      - Valida card_ids contra el input (Mejora 7)

    Devuelve (results: list, tokens_used: int, validation: dict).
    """
    cards_text = ""
    for card in cards:
        cards_text += f"\n\n--- Card ID: {card['id']} | {card['name']} | {card['vistas']:,} vistas ---\n"
        cards_text += card["sql"]

    last_error = None
    for attempt in range(max_retries):
        try:
            message = client.chat.completions.create(
                model=model,
                max_tokens=MAX_TOKENS,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"Audita estas {len(cards)} queries SQL de Metabase:\n{cards_text}"}
                ]
            )

            tokens_used   = message.usage.total_tokens if message.usage else 0
            response_text = message.choices[0].message.content.strip()

            # Limpiar posible markdown
            response_text = re.sub(r"^```json\s*", "", response_text)
            response_text = re.sub(r"\s*```$", "", response_text)

            try:
                results = json.loads(response_text)
                if not isinstance(results, list):
                    return [], tokens_used, {"alucinados": [], "omitidos": [c["id"] for c in cards]}

                # Mejora 6: calcular score determinístico e inyectar en cada resultado
                for r in results:
                    if isinstance(r, dict) and "hallazgos" in r:
                        r["score_salud"] = compute_score(r["hallazgos"])

                # Mejora 7: validar card_ids
                validation = validate_card_ids(cards, results)

                return results, tokens_used, validation

            except json.JSONDecodeError:
                # Retry automático en fallo de parseo: puede ser JSON cortado por límite de tokens
                if attempt < max_retries - 1:
                    print(f"      ⚠️  JSON inválido (intento {attempt + 1}/{max_retries}) — reintentando...")
                    last_error = "JSONDecodeError"
                    continue  # volver al bucle for con el siguiente attempt
                print(f"      ⚠️  Error parseando JSON tras {max_retries} intentos — guardando respuesta raw")
                raw = [{"raw_response": response_text, "cards": [c["id"] for c in cards]}]
                return raw, tokens_used, {"alucinados": [], "omitidos": [c["id"] for c in cards]}

        except Exception as e:
            last_error = e
            err_str = str(e)
            if any(code in err_str for code in ["429", "500", "503", "RateLimitError", "APIStatusError"]):
                wait = (2 ** attempt) * 5  # 5s, 10s, 20s
                print(f"      ⏳ Error API ({err_str[:60]}) — reintentando en {wait}s ({attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise

    print(f"      ✗ Max reintentos alcanzados: {last_error}")
    return [{"error": str(last_error), "cards": [c["id"] for c in cards]}], 0, {"alucinados": [], "omitidos": []}


# ============================================================
# PROCESS LOTE
# ============================================================

def process_lote(client: OpenAI, system_prompt: str, lote_path: str) -> dict:
    """Procesa un lote completo y devuelve todos los resultados."""

    lote_path_obj = Path(lote_path)
    lote_name     = lote_path_obj.stem
    model         = select_model(lote_path_obj)

    model_label = f"{model} {'🔵' if model == MODEL_FULL else '🟢'}"
    print(f"\n📂 Procesando {lote_name} [{model_label}]...")

    cards = parse_lote(lote_path)
    if not cards:
        print(f"   ⚠️  Sin cards parseables en {lote_name}")
        return {"lote": lote_name, "model": model, "cards": [], "total_hallazgos": 0}

    print(f"   {len(cards)} cards | batches de {CARDS_PER_CALL}...")

    all_results      = []
    total_hallazgos  = 0
    total_tokens     = 0
    total_alucinados = 0
    total_omitidos   = 0

    for i in range(0, len(cards), CARDS_PER_CALL):
        batch       = cards[i:i + CARDS_PER_CALL]
        batch_num   = i // CARDS_PER_CALL + 1
        total_batches = (len(cards) + CARDS_PER_CALL - 1) // CARDS_PER_CALL

        print(f"   Batch {batch_num}/{total_batches} ({len(batch)} cards)...", end=" ")

        try:
            results, tokens, validation = analyze_cards_batch(client, system_prompt, batch, model)
            all_results.extend(results)
            total_tokens += tokens

            hallazgos = sum(len(r.get("hallazgos", [])) for r in results if isinstance(r, dict))
            total_hallazgos += hallazgos

            # Reporte de validación de IDs
            alucinados = validation.get("alucinados", [])
            omitidos   = validation.get("omitidos", [])
            total_alucinados += len(alucinados)
            total_omitidos   += len(omitidos)

            status_line = f"✓ {hallazgos} hallazgos | {tokens:,} tokens"
            if alucinados:
                status_line += f" | ⚠️  IDs alucinados: {alucinados}"
            if omitidos:
                status_line += f" | ⚠️  IDs omitidos: {omitidos}"
            print(status_line)

        except Exception as e:
            print(f"✗ Error: {e}")
            all_results.append({"error": str(e), "cards": [c["id"] for c in batch]})

    # Calcular costo estimado para este lote
    cost_rate = COST_PER_TOKEN_FULL if model == MODEL_FULL else COST_PER_TOKEN_MINI
    costo_lote = total_tokens * cost_rate

    result = {
        "lote":             lote_name,
        "model":            model,
        "cards":            all_results,
        "total_cards":      len(cards),
        "total_hallazgos":  total_hallazgos,
        "total_tokens":     total_tokens,
        "costo_estimado":   round(costo_lote, 5),
        "ids_alucinados":   total_alucinados,
        "ids_omitidos":     total_omitidos,
        "procesado_en":     datetime.now().isoformat(),
    }

    if total_alucinados or total_omitidos:
        print(f"   ⚠️  IDs con problema: {total_alucinados} alucinados, {total_omitidos} omitidos en todo el lote")

    return result


# ============================================================
# REPORTE FINAL
# ============================================================

def generate_report(all_lote_results: list) -> str:
    """Genera un reporte Markdown consolidado."""

    all_cards = []
    for lote in all_lote_results:
        for card in lote.get("cards", []):
            if isinstance(card, dict) and "card_id" in card:
                all_cards.append(card)

    if not all_cards:
        return "# Reporte de Auditoría\n\nSin resultados para mostrar."

    total_cards     = len(all_cards)
    con_hallazgos   = [c for c in all_cards if c.get("hallazgos")]
    total_hallazgos = sum(len(c.get("hallazgos", [])) for c in all_cards)

    alta  = sum(1 for c in all_cards for h in c.get("hallazgos", []) if h.get("severidad") == "alta")
    media = sum(1 for c in all_cards for h in c.get("hallazgos", []) if h.get("severidad") == "media")
    baja  = sum(1 for c in all_cards for h in c.get("hallazgos", []) if h.get("severidad") == "baja")

    sorted_cards = sorted(all_cards, key=lambda x: x.get("score_salud", 100))
    peores_10    = sorted_cards[:10]

    scores    = [c.get("score_salud", 100) for c in all_cards]
    avg_score = sum(scores) / len(scores) if scores else 100

    # Costo total por modelo
    costo_full = sum(
        l.get("costo_estimado", 0) for l in all_lote_results if l.get("model") == MODEL_FULL
    )
    costo_mini = sum(
        l.get("costo_estimado", 0) for l in all_lote_results if l.get("model") == MODEL_MINI
    )
    costo_total = costo_full + costo_mini

    # Resumen de validación de IDs
    total_alucinados = sum(l.get("ids_alucinados", 0) for l in all_lote_results)
    total_omitidos   = sum(l.get("ids_omitidos",   0) for l in all_lote_results)

    lines = [
        "# Reporte de Auditoría — Metabase ExampleCorp",
        f"*Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "",
        "---",
        "",
        "## Resumen ejecutivo",
        "",
        "| Métrica | Valor |",
        "|---|---|",
        f"| Cards auditadas | {total_cards} |",
        f"| Cards con hallazgos | {len(con_hallazgos)} ({len(con_hallazgos)*100//total_cards if total_cards else 0}%) |",
        f"| Total hallazgos | {total_hallazgos} |",
        f"| Severidad alta | {alta} 🔴 |",
        f"| Severidad media | {media} 🟡 |",
        f"| Severidad baja | {baja} 🟢 |",
        f"| Score promedio de salud | {avg_score:.0f}/100 |",
        f"| Costo estimado total | ~${costo_total:.3f} USD |",
        f"| — gpt-4o (lotes 01-54) | ~${costo_full:.3f} USD |",
        f"| — gpt-4o-mini (lotes 55-61) | ~${costo_mini:.3f} USD |",
        f"| IDs alucinados por el modelo | {total_alucinados} |",
        f"| IDs omitidos silenciosamente | {total_omitidos} |",
        "",
        "---",
        "",
        "## Top 10 cards más problemáticas",
        "",
        "| Score | Card | Vistas | Hallazgos |",
        "|---|---|---|---|",
    ]

    for card in peores_10:
        hallazgos  = card.get("hallazgos", [])
        altas_card = sum(1 for h in hallazgos if h.get("severidad") == "alta")
        lines.append(
            f"| {card.get('score_salud', 100)} | {card.get('card_name', '?')} (id:{card.get('card_id')}) "
            f"| {card.get('vistas', 0):,} | {len(hallazgos)} ({altas_card} alta) |"
        )

    lines += [
        "",
        "---",
        "",
        "## Hallazgos de severidad alta",
        "",
    ]

    alta_cards = [c for c in all_cards if any(h.get("severidad") == "alta" for h in c.get("hallazgos", []))]
    alta_cards.sort(key=lambda x: x.get("vistas", 0), reverse=True)

    for card in alta_cards:
        altas = [h for h in card.get("hallazgos", []) if h.get("severidad") == "alta"]
        lines += [
            f"### {card.get('card_name')} (id:{card.get('card_id')} | {card.get('vistas', 0):,} vistas)",
            "",
        ]
        for h in altas:
            lines += [
                f"**{h.get('tipo', 'Problema')}**",
                f"- Descripción: {h.get('descripcion', '')}",
                f"- Recomendación: {h.get('recomendacion', '')}",
                "",
            ]

    return "\n".join(lines)


# ============================================================
# RE-AUDITORÍA POST-FIX (Mejora 8)
# ============================================================

def build_card_index(lote_files: list) -> dict:
    """
    Escanea los headers de todos los lotes y devuelve {card_id: lote_path}.
    Solo lee los comentarios de cabecera — no carga el SQL completo.
    Se usa en modo --reaudit para encontrar rápidamente qué lote contiene cada card.
    """
    index = {}
    for lote_path in lote_files:
        with open(lote_path, "r", encoding="utf-8") as f:
            content = f.read()
        for match in re.finditer(r"-- Card: .+? \(id:(\d+) \|", content):
            card_id = int(match.group(1))
            index[card_id] = lote_path
    return index


def process_lote_reaudit(
    client: OpenAI,
    system_prompt: str,
    lote_path: Path,
    target_ids: set,
    original_lote_result: dict
) -> dict:
    """
    Re-audita SOLO las cards con IDs en target_ids dentro del lote dado.
    Nunca hace skip: siempre procesa, aunque el lote ya tenga resultado previo.
    Devuelve el resultado del lote con un campo extra 'comparaciones' (antes/después).
    """
    lote_name   = lote_path.stem
    model       = select_model(lote_path)
    model_label = f"{model} {'🔵' if model == MODEL_FULL else '🟢'}"
    print(f"\n📂 Re-auditando {lote_name} [{model_label}]...")

    all_cards    = parse_lote(str(lote_path))
    target_cards = [c for c in all_cards if c["id"] in target_ids]

    if not target_cards:
        print(f"   ⚠️  Ninguna card target encontrada en {lote_name}")
        return {}

    print(f"   {len(target_cards)} cards target de {len(all_cards)} en este lote")

    # Índice de resultados originales para comparar antes/después
    orig_by_id = {}
    for card_result in original_lote_result.get("cards", []):
        if isinstance(card_result, dict) and "card_id" in card_result:
            orig_by_id[card_result["card_id"]] = card_result

    all_results     = []
    total_hallazgos = 0
    total_tokens    = 0
    comparaciones   = []

    for i in range(0, len(target_cards), CARDS_PER_CALL):
        batch         = target_cards[i:i + CARDS_PER_CALL]
        batch_num     = i // CARDS_PER_CALL + 1
        total_batches = (len(target_cards) + CARDS_PER_CALL - 1) // CARDS_PER_CALL

        print(f"   Batch {batch_num}/{total_batches} ({len(batch)} cards)...", end=" ")

        try:
            results, tokens, validation = analyze_cards_batch(client, system_prompt, batch, model)
            all_results.extend(results)
            total_tokens += tokens

            hallazgos = sum(len(r.get("hallazgos", [])) for r in results if isinstance(r, dict))
            total_hallazgos += hallazgos

            # Construir comparación antes/después para cada card del batch
            for r in results:
                if isinstance(r, dict) and "card_id" in r:
                    cid  = r["card_id"]
                    orig = orig_by_id.get(cid, {})
                    comparaciones.append({
                        "card_id":            cid,
                        "card_name":          r.get("card_name", ""),
                        "vistas":             r.get("vistas", 0),
                        "score_antes":        orig.get("score_salud", "N/A"),
                        "score_despues":      r.get("score_salud", 100),
                        "hallazgos_antes":    len(orig.get("hallazgos", [])),
                        "hallazgos_despues":  len(r.get("hallazgos", [])),
                    })

            alucinados  = validation.get("alucinados", [])
            omitidos    = validation.get("omitidos", [])
            status_line = f"✓ {hallazgos} hallazgos | {tokens:,} tokens"
            if alucinados:
                status_line += f" | ⚠️  IDs alucinados: {alucinados}"
            if omitidos:
                status_line += f" | ⚠️  IDs omitidos: {omitidos}"
            print(status_line)

        except Exception as e:
            print(f"✗ Error: {e}")
            all_results.append({"error": str(e), "cards": [c["id"] for c in batch]})

    cost_rate = COST_PER_TOKEN_FULL if model == MODEL_FULL else COST_PER_TOKEN_MINI
    return {
        "lote":             lote_name,
        "model":            model,
        "cards":            all_results,
        "total_cards":      len(target_cards),
        "total_hallazgos":  total_hallazgos,
        "total_tokens":     total_tokens,
        "costo_estimado":   round(total_tokens * cost_rate, 5),
        "comparaciones":    comparaciones,
        "procesado_en":     datetime.now().isoformat(),
    }


def generate_reaudit_report(all_lote_results: list, target_ids: set) -> str:
    """
    Genera un reporte Markdown de re-auditoría con tabla comparativa antes/después
    y listado de hallazgos de severidad alta que persisten post-fix.
    """
    all_comparaciones = []
    for lote in all_lote_results:
        all_comparaciones.extend(lote.get("comparaciones", []))

    mejoradas  = 0
    sin_cambio = 0
    empeoradas = 0
    sin_baseline = 0

    lines = [
        "# Re-Auditoría Post-Fix — Metabase ExampleCorp",
        f"*Generado: {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        f"*Cards re-auditadas: {len(target_ids)}*",
        "",
        "---",
        "",
        "## Comparación Antes / Después",
        "",
        "| Card ID | Nombre | Score antes | Score después | Hallazgos antes | Hallazgos después | Resultado |",
        "|---------|--------|-------------|---------------|-----------------|-------------------|-----------|",
    ]

    for c in sorted(all_comparaciones, key=lambda x: x["card_id"]):
        antes   = c["score_antes"]
        despues = c["score_despues"]

        if antes == "N/A":
            resultado = "⚠️ Sin baseline"
            sin_baseline += 1
        elif despues > antes:
            resultado = "✅ Mejoró"
            mejoradas += 1
        elif despues == antes:
            resultado = "➡️ Sin cambio"
            sin_cambio += 1
        else:
            resultado = "🔴 Empeoró"
            empeoradas += 1

        nombre_corto = c["card_name"][:45]
        lines.append(
            f"| {c['card_id']} | {nombre_corto} | {antes} | {despues} "
            f"| {c['hallazgos_antes']} | {c['hallazgos_despues']} | {resultado} |"
        )

    total_costo  = sum(l.get("costo_estimado", 0) for l in all_lote_results)
    total_tokens = sum(l.get("total_tokens", 0) for l in all_lote_results)

    lines += [
        "",
        "---",
        "",
        "## Resumen",
        "",
        f"- ✅ Cards que mejoraron su score: **{mejoradas}**",
        f"- ➡️  Sin cambio de score: **{sin_cambio}**",
        f"- 🔴 Cards que empeoraron: **{empeoradas}** ← verificar manualmente",
        f"- ⚠️  Sin baseline previo: **{sin_baseline}**",
        "",
        f"Costo re-auditoría: ~${total_costo:.4f} USD | Tokens: {total_tokens:,}",
    ]

    # Hallazgos alta que persisten después del fix
    all_cards = [
        c for lote in all_lote_results
        for c in lote.get("cards", [])
        if isinstance(c, dict) and "card_id" in c
    ]
    alta_cards = [
        c for c in all_cards
        if any(h.get("severidad") == "alta" for h in c.get("hallazgos", []))
    ]

    if alta_cards:
        lines += [
            "",
            "---",
            "",
            "## ⚠️  Hallazgos Alta Severidad que Persisten Post-Fix",
            "*(Estas cards requieren revisión manual adicional)*",
            "",
        ]
        for card in sorted(alta_cards, key=lambda x: x.get("score_salud", 100)):
            altas = [h for h in card.get("hallazgos", []) if h.get("severidad") == "alta"]
            lines += [
                f"### {card.get('card_name')} (id:{card.get('card_id')} | {card.get('vistas', 0):,} vistas)",
                "",
            ]
            for h in altas:
                lines += [
                    f"**{h.get('tipo', 'Problema')}**",
                    f"- Descripción: {h.get('descripcion', '')}",
                    f"- Fix sugerido: {h.get('recomendacion', '')}",
                    "",
                ]
    else:
        lines += [
            "",
            "---",
            "",
            "## ✅ Sin hallazgos de severidad alta post-fix",
            "Todas las cards re-auditadas quedaron limpias de errores críticos.",
        ]

    return "\n".join(lines)


def run_reaudit(client: OpenAI, system_prompt: str, card_ids: set):
    """
    Orquesta el modo --reaudit:
    1. Construye índice card_id → lote
    2. Agrupa las cards target por lote
    3. Re-audita solo esas cards por lote
    4. Guarda resultados en resultados/reaudit/<timestamp>/
    5. Genera reporte comparativo antes/después
    """
    if not card_ids:
        print("⚠️  --reaudit: no se especificaron card IDs.")
        return

    print(f"\n🔄 MODO RE-AUDITORÍA — {len(card_ids)} cards: {sorted(card_ids)}")

    all_lote_files = sorted(Path(LOTES_DIR).glob("lote_*.sql"))

    print("   Construyendo índice card → lote...", end=" ", flush=True)
    card_index = build_card_index(all_lote_files)
    print(f"✓ ({len(card_index)} cards indexadas en {len(all_lote_files)} lotes)")

    encontradas    = card_ids & set(card_index.keys())
    no_encontradas = card_ids - encontradas

    if no_encontradas:
        print(f"   ⚠️  Cards NO encontradas en ningún lote: {sorted(no_encontradas)}")
    if not encontradas:
        print("   ✗ Ninguna card encontrada. Verifica los IDs.")
        return

    # Agrupar cards por lote
    lotes_a_procesar: dict = {}
    for card_id in encontradas:
        lote_path = card_index[card_id]
        lotes_a_procesar.setdefault(lote_path, set()).add(card_id)

    print(f"   {len(encontradas)} cards distribuidas en {len(lotes_a_procesar)} lotes")

    # Crear carpeta de sesión con timestamp
    os.makedirs(REAUDIT_DIR, exist_ok=True)
    timestamp   = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_dir = os.path.join(REAUDIT_DIR, timestamp)
    os.makedirs(session_dir, exist_ok=True)

    all_results = []

    for lote_path, ids_en_lote in sorted(lotes_a_procesar.items()):
        # Cargar JSON original para la comparación antes/después
        orig_path = os.path.join(OUTPUT_DIR, f"{lote_path.stem}_resultados.json")
        original  = {}
        if os.path.exists(orig_path):
            try:
                with open(orig_path, "r", encoding="utf-8") as f:
                    original = json.load(f)
            except json.JSONDecodeError:
                print(f"   ⚠️  JSON corrupto en {lote_path.stem}_resultados.json — se re-auditará sin comparación antes/después")
        else:
            print(f"   ⚠️  No existe resultado original para {lote_path.stem} — comparación antes/después no disponible")

        result = process_lote_reaudit(client, system_prompt, lote_path, ids_en_lote, original)
        if not result:
            continue

        all_results.append(result)

        out_path = os.path.join(session_dir, f"{lote_path.stem}_reaudit.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        print(f"   💾 Guardado: {out_path}")

    # Generar reporte comparativo
    print(f"\n📝 Generando reporte de re-auditoría...")
    report_md   = generate_reaudit_report(all_results, encontradas)
    report_path = os.path.join(session_dir, "reporte_reauditoria.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_md)

    total_costo = sum(r.get("costo_estimado", 0) for r in all_results)
    print(f"\n{'=' * 60}")
    print(f"  ✓ Re-auditoría completada")
    print(f"  ✓ {len(encontradas)} cards procesadas")
    if no_encontradas:
        print(f"  ⚠️  {len(no_encontradas)} cards no encontradas: {sorted(no_encontradas)}")
    print(f"  📁 Resultados en: {os.path.abspath(session_dir)}")
    print(f"  📝 Reporte en: {os.path.abspath(report_path)}")
    print(f"  💰 Costo estimado: ~${total_costo:.4f} USD")
    print(f"{'=' * 60}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Agente auditor de Metabase")
    parser.add_argument("--lote", action="append", help="Número de lote a procesar (ej: 01)")
    # Mejora 8: re-auditoría post-fix
    parser.add_argument(
        "--reaudit", nargs="+", type=int, metavar="CARD_ID",
        help="Re-auditar solo estas card IDs (post-fix). Ej: --reaudit 198 345 652"
    )
    parser.add_argument(
        "--reaudit-file", metavar="FILE",
        help="Archivo con card IDs a re-auditar, uno por línea. Ej: --reaudit-file ids_fixeadas.txt"
    )
    args = parser.parse_args()

    print("=" * 60)
    print("  Agente Auditor — Metabase ExampleCorp")
    print(f"  Modelos: {MODEL_FULL} (lotes 01-54) | {MODEL_MINI} (lotes 55-61)")
    print("=" * 60)

    # Cargar contexto
    context = load_context(CONTEXT_FILE)
    print(f"✓ Contexto cargado ({len(context)} chars)")

    # Inicializar cliente OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        api_key = input("\n🔑 OpenAI API key: ").strip()
        os.environ["OPENAI_API_KEY"] = api_key

    client        = OpenAI(api_key=api_key)
    system_prompt = build_system_prompt(context)

    # ── MODO RE-AUDITORÍA (Mejora 8) ─────────────────────────
    reaudit_ids: set = set()
    if args.reaudit:
        reaudit_ids.update(args.reaudit)
    if args.reaudit_file:
        if not os.path.exists(args.reaudit_file):
            print(f"✗ --reaudit-file: archivo no encontrado: {args.reaudit_file}")
            return
        with open(args.reaudit_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.isdigit():
                    reaudit_ids.add(int(line))
    if reaudit_ids:
        run_reaudit(client, system_prompt, reaudit_ids)
        return
    # ─────────────────────────────────────────────────────────

    # Seleccionar lotes a procesar (modo normal)
    all_lote_files = sorted(Path(LOTES_DIR).glob("lote_*.sql"))

    if args.lote:
        selected = []
        for num in args.lote:
            matches = [f for f in all_lote_files if f.stem.startswith(f"lote_{num.zfill(2)}")]
            selected.extend(matches)
        lote_files = sorted(set(selected))
    else:
        lote_files = all_lote_files

    if not lote_files:
        print("⚠️  No se encontraron archivos de lote en ./lotes/")
        return

    # Mostrar plan de ejecución
    mini_lotes  = [f for f in lote_files if get_lote_number(f) >= LOTE_MINI_FROM]
    full_lotes  = [f for f in lote_files if get_lote_number(f) < LOTE_MINI_FROM]
    print(f"✓ {len(lote_files)} lotes a procesar")
    print(f"  {len(full_lotes)} lotes con {MODEL_FULL}  (lotes 01-54)")
    print(f"  {len(mini_lotes)} lotes con {MODEL_MINI} (lotes 55-61)")

    # Crear carpeta de resultados
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Procesar lotes
    all_results              = []
    total_hallazgos_global   = 0
    total_tokens_full        = 0
    total_tokens_mini        = 0
    lotes_skipped            = 0

    for lote_file in lote_files:
        out_path = os.path.join(OUTPUT_DIR, f"{lote_file.stem}_resultados.json")

        # Skip lotes ya procesados
        if Path(out_path).exists():
            print(f"\n⏭️  {lote_file.stem} ya procesado — skip")
            with open(out_path, "r", encoding="utf-8") as f:
                all_results.append(json.load(f))
            lotes_skipped += 1
            continue

        result = process_lote(client, system_prompt, str(lote_file))
        all_results.append(result)
        total_hallazgos_global += result.get("total_hallazgos", 0)

        if result.get("model") == MODEL_MINI:
            total_tokens_mini += result.get("total_tokens", 0)
        else:
            total_tokens_full += result.get("total_tokens", 0)

        # Guardar resultado del lote inmediatamente
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)

        print(f"   💾 Guardado: {out_path}")
        print(f"   🪙 Tokens este lote: {result.get('total_tokens', 0):,} | ~${result.get('costo_estimado', 0):.4f} USD")

    # Generar reporte final
    print(f"\n📝 Generando reporte final...")
    report_md  = generate_report(all_results)
    report_path = os.path.join(OUTPUT_DIR, "reporte_final.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_md)

    costo_full  = total_tokens_full * COST_PER_TOKEN_FULL
    costo_mini  = total_tokens_mini * COST_PER_TOKEN_MINI
    costo_total = costo_full + costo_mini

    print(f"\n{'=' * 60}")
    print(f"  ✓ Auditoría completada")
    print(f"  ✓ {total_hallazgos_global} hallazgos encontrados")
    print(f"  ⏭️  Lotes skipped (ya procesados): {lotes_skipped}")
    print(f"  🪙 Tokens gpt-4o:      {total_tokens_full:,}  (~${costo_full:.3f} USD)")
    print(f"  🪙 Tokens gpt-4o-mini: {total_tokens_mini:,}  (~${costo_mini:.3f} USD)")
    print(f"  💰 Costo estimado sesión: ~${costo_total:.3f} USD")
    print(f"  ✓ Reporte en: {os.path.abspath(report_path)}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
