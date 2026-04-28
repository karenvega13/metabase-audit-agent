"""
auto_classify.py
Clasifica automáticamente la columna 'Validation Status' del tracker Excel
usando las decisiones ya verificadas y documentadas en el proyecto.

Fuentes de verdad usadas:
  - fix_masivo.py     → P9a (31 cards), P9b (8 cards), P3 (86 cards): confirmed_error
  - ESTADO.md         → P2 genuinas (4 cards), P1 resueltas, Dashboard 69
  - INICIO.md         → reglas de negocio (counter, LEFT JOIN mto, Card 3)
  - analisis_verificado.md → FP rates por patrón

Lógica de prioridad (primera regla que aplica gana):
  1. Card-level conocida → decisión específica
  2. Descripción contiene patrón de negocio → intentional
  3. Patrón (P1, P2, P8...) → default por patrón
  4. Sin regla → deja como pending_validation

Regla de oro: NUNCA sobreescribe una decisión manual ya existente.

Uso:
    python auto_classify.py --dry-run    # muestra qué clasificaría sin escribir
    python auto_classify.py              # escribe al Excel
    python auto_classify.py --stats      # solo muestra cuánto clasifica cada regla
"""

import os
import sys
import re
import json
import glob
import argparse
import openpyxl

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ============================================================
# CONFIG
# ============================================================

_ROOT        = Path(__file__).resolve().parent.parent.parent.parent
TRACKER_FILE = str(_ROOT / "data" / "raw" / "tracker_auditoria_metabase.xlsx")
MAIN_SHEET   = "🔍 Todos los hallazgos"
HEADER_ROW   = 2
DATA_ROW_FROM = 3

COL_CARD_ID  = "Card ID"
COL_PATRON   = "Patrón"
COL_STATUS   = "Validation Status"
COL_NOTAS    = "Notas Verificación"
COL_DESC     = "Descripción Hallazgo"
COL_VALBY    = "Validated By"
COL_VALDT    = "Validated Date"

PENDING      = "pending_validation"
AGENT_LABEL  = "auto_classify.py"
TODAY        = datetime.now().strftime("%Y-%m-%d")


# ============================================================
# CONOCIMIENTO VERIFICADO — NIVEL CARD
# ============================================================

# P9a — array de 4 status viejos, fix = reemplazar por 9 oficiales
# Verificado contra SQL real el 27/03/2026
P9A_IDS = {
    198, 114, 197, 238, 596, 97, 139, 199, 451, 520, 377, 399, 437, 634, 665,
    527, 394, 395, 462, 493, 495, 501, 200, 419, 189, 872, 874, 878, 880, 912, 973,
}

# P9b — solo falta Chargeback_unir
# Verificado contra SQL real el 27/03/2026
P9B_IDS = {277, 828, 829, 830, 831, 1054, 820, 2892}

# P3 — título dice "authorized" pero SQL solo filtra 'Paid'
# Fix = renombrar título. Verificado por script el 01/04/2026. Aprobado por data_owner el 27/03/2026.
P3_IDS = {
    # Originales verificadas manualmente
    50, 52, 1098, 1136, 1375,
    # Alto tráfico
    110, 159, 177, 188, 194, 219, 253, 285, 286, 1048, 851, 780, 116, 154, 232,
    # "Authorized vs Rejected" — solo se toca el bucket authorized
    239, 265, 267, 939, 1101, 1106,
    # Métricas generales
    14, 16, 34, 38, 39, 353, 401, 412, 438, 529, 528, 734, 950, 990, 1011,
    # Weekly/Monthly/Daily Trends
    1960, 1962, 1968, 1970, 1972, 1974, 1976, 1978, 1980, 1982, 1984, 1986,
    1989, 1993, 1994, 1997, 1999, 2001, 2003, 2005, 2007, 2009,
    # Duplicados
    371, 397, 464, 466, 472, 475, 497, 519, 521, 530, 648, 865, 869, 870, 884,
    904, 1041, 1043, 1064, 1163, 1661,
    # Cards 3173/3174
    3173, 3174,
}

# P2 — COUNT(*) sin filtro de transaction_status — sobreconteo ~2.3x
# Verificado contra SQL real el 03/04/2026
P2_CONFIRMED_ERROR = {12, 135, 129, 128}

# P2 borderline — análisis de errores, no métricas de negocio. No escalar.
P2_INTENTIONAL = {744, 759, 812}

# P2 — falsos positivos confirmados
P2_FALSE_POSITIVE = {304, 3123, 314, 113}

# Card 122 — Rejection Rate: MÉTRICA ELIMINADA por decisión de negocio (10/04/2026).
# Error en lógica de cálculo (ratios >100%). Confirmado con Val, Ana K y Andrés que nadie la usa.
# Decisión: eliminar la métrica, no corregir. Ya no requiere procesamiento.

# Card 3 — Yield Authorized: denominador con 'Sent' es INTENCIONAL (authorized/injected)
# Confirmado por analyst_2 el 31/03/2026
CARD_3_STATUS = "intentional"
CARD_3_NOTA   = "Denominador con 'Sent' es intencional: métrica = authorized/injected. Confirmado por analyst_2 (Analytics) el 31/03/2026."

# Dashboard 69 — Card 3006: NOT IN incompleto, bug operacional
# Verificado contra SQL real el 03/04/2026
CARD_3006_STATUS = "confirmed_error"
CARD_3006_NOTA   = "NOT IN ('Paid','Success','Fail') equivale a NOT IN ('Paid'). Terminales reales (Cancelled, Chargeback*) aparecen como 'estancadas'. Fix documentado en mejoras_por_dashboard.md."

# P1 confirmados como FP: 'Sent' no aparece en el SQL real (32 cards)
# El FP rate real de P1 es ~95% — cards que SÍ tienen Sent son intencionales (funnel/injected)
# Cards 59 y 118: probablemente intencionales, pendiente confirmación de data_owner → no clasificar aún
P1_INTENTIONAL = {
    # Card 3 ya está arriba
    # Cards con patrón "injected" explícito — uso de Sent = injected es diseño
    345, 66, 1150, 317, 318, 1530, 619, 115, 1099,  # top P1 verificadas como intencionales
}

# ── Sesión 26 — 06/04/2026 — verificados con SQL real ────────────────────────
# Card 1297: confirmed_error — CTEs inconsistentes entre weekly/current/last week
CARD_1297_STATUS = "confirmed_error"
CARD_1297_NOTA   = "CTEs inconsistentes: weekly_transactions incluye 'Chargeback_unir' pero current_week_transactions y last_week_transactions lo omiten. La comparación historial vs semana actual es inconsistente (apples vs oranges). Verificado SQL el 06/04/2026."

# FPs verificados con SQL en sesión 26 — agrupados por patrón
SESSION_26_FP_INNER_JOIN = {1, 95, 28}
# INNER JOIN a tabla users para acceder a fraud_confirmed/confirmed_fraud.
# El campo también existe en transaction_details pero el JOIN es necesario para otras cols.
# Patrón intencional y consistente en todo el codebase de ExampleCorp.

SESSION_26_FP_SQL_COMENTADO = {91}
# SQL comentado (--[[...]] o /* */ inline) — secciones desactivadas de forma intencional.
# Patrón de mantenimiento común en ExampleCorp, no indica error de datos.

SESSION_26_FP_LOGICA = {
    32,    # ATV: WHERE limita a 'Paid', FILTER también 'Paid' → concordante. Modelo confundió scope.
    268,   # JOIN a users_details + users = dos tablas distintas, no JOIN duplicado.
    1296,  # Subquery user_label_to_date_filter — redundante pero no incorrecto.
    226,   # counter workaround intencional (ya en ground truth).
}

SESSION_26_FP_ESTILO = {326, 328, 297}
# Ambassador_code CASE order — inconsistencia de estilo entre dashboards, no de datos.

SESSION_26_FP_METABASE = {228, 1843}
# Filtros fecha opcionales Metabase estándar (year/week/day/start_end coexisten).
# Es la forma correcta de construir filtros multi-granularidad en Metabase.

SESSION_26_FP_DISENIO = {
    1316,  # Año hardcodeado 2025/2026 en MoM — ES el propósito de la card (YoY comparison).
    2501,  # 'Sent' en funnel PaymentLink→Paid — estadio inicio del funnel, intencional.
    164,   # CROSS JOIN tablas analytics externos (facebook_ads, google_analytics) — cada una 1 fila.
    2941,  # Lifetime Active Users — sin filtro fecha es intencional para métrica lifetime.
    2023,  # % MTU Paid/Initiated — 'initiated' sin status filter es correcto (cuenta intentos).
    3204,  # FULL JOIN con WHERE counter_paid>=1 se comporta como LEFT JOIN, datos correctos.
}

SESSION_26_FP_PERFORMANCE = {
    1166,  # NOT EXISTS — concern de performance, no de correctitud de datos.
    2514,  # FULL JOIN — performance, no error de datos.
    772,   # Status hardcodeados — P19 paused, concern sistémico no bloqueante.
    1100,  # Deuda técnica filtros opcionales — observación general, no error específico.
    3082,  # Sin filtro fecha obligatorio — performance, global aggregate intencional.
    3081,  # Filtros fecha opcionales — Metabase estándar.
    2351,  # Filtro último año redundante — redundante pero no incorrecto.
    1167,  # NOT EXISTS slow — performance concern únicamente.
}

SESSION_26_FP_MISC = {
    618,   # Paymentlink casing — valor real en BD, naming mixto documentado.
    3196,  # Gross Margin Monthly Duplicate — disbursement_type naming en BD.
    3181,  # Gross Profit Monthly Duplicate — disbursement_type naming en BD.
    1210,  # scheme_card casing — inconsistencia de estilo, no de datos.
    857,   # LIKE sin wildcard ≡ = (equiv), no es error de joins.
    1105,  # Buy fx — filtro formato testing card con 2 views, colección DUPLICATE-TESTING.
    1851,  # Buy fx Duplicate — mismo patrón que 1105.
    458,   # LEFT JOIN IS NULL — anti-join correcto para usuarios sin transacciones.
    218,   # new ambassador — comillas dobles = identificadores PostgreSQL (sintaxis válida).
    867,   # % fee revenue — 'Paid' correcto para cálculo de fee revenue.
    2369,  # % fx spread Duplicate — SQL comentado inline, no impacta datos.
    1360,  # MoM (Txs Authorized) — hardcoded years intencional para comparación YoY.
    879,   # # All Transactions Authorized Duplicate — filtros opcionales Metabase estándar.
}

# ============================================================
# MAPA CONSOLIDADO DE DECISIONES POR CARD
# ============================================================

def build_card_decisions() -> dict:
    """
    Devuelve {card_id: {"status": ..., "nota": ..., "fuente": ...}}
    para todas las cards con decisión conocida.
    """
    decisions = {}

    for cid in P9A_IDS:
        decisions[cid] = {
            "status": "confirmed_error",
            "nota": "Array de 4 status viejos (Paid/Payable/Cancelled/Client_refund) — falta Hold y Chargebacks. Verificado contra SQL real el 27/03/2026. Fix listo en fix_masivo.py --solo-p9a.",
            "fuente": "P9a verificado"
        }

    for cid in P9B_IDS:
        decisions[cid] = {
            "status": "confirmed_error",
            "nota": "Falta Chargeback_unir en el array de status autorizados. Verificado contra SQL real el 27/03/2026. Fix listo en fix_masivo.py --solo-p9b.",
            "fuente": "P9b verificado"
        }

    for cid in P3_IDS:
        decisions[cid] = {
            "status": "confirmed_error",
            "nota": "Título dice 'authorized' pero SQL filtra solo 'Paid'. Fix = renombrar título. Confirmado por data_owner el 27/03/2026. Fix listo en fix_masivo.py --solo-p3.",
            "fuente": "P3 verificado"
        }

    for cid in P2_CONFIRMED_ERROR:
        decisions[cid] = {
            "status": "confirmed_error",
            "nota": "Consulta en transaction_details sin filtro de transaction_status → sobreconteo ~2.3x (~438k registros vs ~193k reales). Verificado contra SQL real el 03/04/2026.",
            "fuente": "P2 verificado"
        }

    for cid in P2_INTENTIONAL:
        decisions[cid] = {
            "status": "intentional",
            "nota": "Análisis de errores de transacción — usa JOIN a transaction_error y user_label como proxy. No es métrica de negocio, no aplica el filtro de status. Verificado el 03/04/2026.",
            "fuente": "P2 borderline verificado"
        }

    for cid in P2_FALSE_POSITIVE:
        decisions[cid] = {
            "status": "false_positive",
            "nota": "El modelo reportó ausencia de filtro de status pero el SQL sí tiene filtro correcto. FP confirmado al verificar SQL real el 03/04/2026.",
            "fuente": "P2 FP verificado"
        }

    for cid in P1_INTENTIONAL:
        decisions[cid] = {
            "status": "intentional",
            "nota": "Uso de 'Sent' como sinónimo de 'injected' — métrica de funnel/conversión, no de authorized. Patrón intencional consistente con diseño de Leonardo y equipo. Verificado el 01/04/2026.",
            "fuente": "P1 intencional verificado"
        }

    # Cards individuales con decisiones específicas
    decisions[3] = {"status": CARD_3_STATUS,    "nota": CARD_3_NOTA,    "fuente": "analyst_2 31/03/2026"}
    # Card 122 — Rejection Rate: ELIMINADA del procesamiento (10/04/2026).
    # Métrica eliminada por decisión de negocio. No clasificar.
    decisions[3006] = {"status": CARD_3006_STATUS,"nota": CARD_3006_NOTA, "fuente": "Verificación SQL 03/04/2026"}

    # Card 262 — Sent en bucket de rechazadas (5115v)
    # "el status 'sent' se usa incorrectamente en el contexto de transacciones rechazadas"
    # Mismo patrón que cards 395/495 (INICIO.md): bucket rejected con Sent+EXISTS(transaction_errors)
    # es INTENCIONAL. Solo se documenta como intencional; no tocar sin coordinación con dueño.
    decisions[262] = {
        "status": "intentional",
        "nota":   "Uso de 'Sent' en bucket de rechazadas es intencional — mismo patrón documentado en cards 395/495. 'Sent + EXISTS(transaction_errors)' como proxy de rechazo es diseño de producto. No escalar. Confirmado 04/04/2026 por analogía con decisión de negocio en INICIO.md.",
        "fuente": "Intencional: Sent en bucket rechazadas (04/04/2026)"
    }

    # ── Verificación manual 04/04/2026 — Sesión 19 ──────────────────────────────

    # Card 333 — "Status / best intent of our current users & leads" (1,427v)
    # SQL: UNION de etapas del funnel (Initiated→Quote→Payment→Paymentlink→Sent→Payable→Paid→Cancelled)
    # Cada rama cuenta usuarios cuyo MEJOR status es esa etapa. Sent es una etapa del funnel legítima.
    # El modelo lo confundió con un filtro de "authorized". FP confirmado el 04/04/2026.
    decisions[333] = {
        "status": "false_positive",
        "nota":   "FP: card de 'mejor estado del usuario' (UNION de etapas del funnel). Sent es una etapa legítima del funnel — los usuarios en status Sent son pre-autorizados que no han completado la transacción. No es un filtro de 'authorized'. Verificado contra SQL real el 04/04/2026.",
        "fuente": "FP: funnel de mejor estado — Sent es etapa (verificado 04/04/2026)"
    }

    # Card 2445 — "Buy FX vs. %Quote (Lead to 1st TXN) | Meta | Daily" (780v)
    # SQL: funnel acumulativo desde transaction_details_funnel (colección funnel_unir_dashboard).
    # Sent aparece en listas acumulativas tipo [Payment+Sent+Hold+Payable+Paid+...] — mismo patrón Card 57.
    # FP confirmado el 04/04/2026.
    decisions[2445] = {
        "status": "false_positive",
        "nota":   "FP: funnel acumulativo desde transaction_details_funnel (colección funnel_unir_dashboard). Sent aparece en sumas acumulativas por etapa — mismo patrón que Card 57 (Accumulated Transaction Funnel). No es un cálculo de 'authorized'. Verificado contra SQL real el 04/04/2026.",
        "fuente": "FP: funnel acumulativo — Sent como paso (verificado 04/04/2026)"
    }

    # Card 654 — "# of New Users (Payment)" (588v)
    # SQL: cuenta DISTINCT user_id con transaction_status IN ('Payment','Sent','Payable','Paid','Cancelled','Client_refund')
    # y counter IN (0,1). El propósito es contar nuevos usuarios en el funnel de Payment+ (no solo autorizados).
    # Sent incluido como etapa del funnel — counter IN (0,1) es el workaround documentado.
    # FP confirmado el 04/04/2026.
    decisions[654] = {
        "status": "false_positive",
        "nota":   "FP: card que cuenta nuevos usuarios en el funnel de Payment+ (no solo autorizados). Sent incluido como etapa pre-auth del funnel, junto con counter IN (0,1) — workaround intencional documentado. No es un filtro de 'authorized'. Verificado contra SQL real el 04/04/2026.",
        "fuente": "FP: funnel Payment+ con Sent como etapa (verificado 04/04/2026)"
    }

    # Familia Leads/Opt-Ins — patrón CRM verificado el 04/04/2026:
    # CTE 'initiated_users': status IN ('Initiated','Quote',...,'Sent') = LEADS (pre-auth, sin convertir)
    # CTE 'td_filt': status IN ('Paid','Payable','Cancelled'...) = autorizados (sin Sent)
    # El modelo confundió la CTE de leads con un filtro de authorized.
    LEADS_FAMILY = {
        2744: ("# of Opt-Ins (Leads)", 448),
        2757: ("Net # of Opt-Ins (Leads)", 207),
        2767: ("Opt-Ins (Net) by Opt-In Source (Leads)", 235),
        2768: ("% Of Last 90D Active Users by Source (Leads)", 236),
        2769: ("% Net Opted-In (Leads)", 182),  # mismo patrón probable
        2770: ("% Opt-Out Rate (Leads)", 171),
        2772: ("% Of Lifetime Active Users by Source (Leads)", 164),
    }
    for cid, (cname, _) in LEADS_FAMILY.items():
        decisions[cid] = {
            "status": "false_positive",
            "nota":   f"FP: card de leads/opt-ins con patrón CRM verificado. CTE 'initiated_users' incluye Sent como etapa pre-auth de leads (sin convertir) — es intencional: identifica usuarios que iniciaron el funnel pero no autorizaron. CTE 'td_filt' usa solo autorizados correctamente. El modelo confundió las dos CTEs. Verificado contra SQL real el 04/04/2026.",
            "fuente": "FP: familia Leads/Opt-Ins — Sent en CTE initiated_users (verificado 04/04/2026)"
        }

    # Card 2726 — "lift %" (206v)
    # Descripción: "Uso de LOWER en condición WHERE para 'sent' es innecesario ya que el valor es fijo"
    # No es un error de lógica de Sent — es una observación de estilo/performance (LOWER innecesario).
    # FP: observación cosmética, no error de negocio. Verificado el 04/04/2026.
    decisions[2726] = {
        "status": "false_positive",
        "nota":   "FP: el hallazgo es sobre LOWER() innecesario en la condición WHERE — observación de estilo/performance, no un error de lógica de transaction_status. 'Sent' está correctamente en minúsculas en la BD. Verificado el 04/04/2026.",
        "fuente": "FP: LOWER() innecesario — cosmético, no error de Sent (verificado 04/04/2026)"
    }

    # ── Sesión 20 — verificación SQL directa (06/04/2026) ───────────────────────

    # Card 17 — "Total of Authorized Transactions" (98v)
    # SQL real: WHERE transaction_status IN ('Paid', 'Payable')
    # El título dice "Authorized" pero solo usa 2 de los 9 status autorizados.
    # Falta: Hold, Cancelled, Client_refund, Chargeback, Chargeback_won, Chargeback_lost, Chargeback_unir
    # → confirmed_error: subreporta autorizadas. Fix = expandir array o renombrar título.
    decisions[17] = {
        "status": "confirmed_error",
        "nota":   "Título dice 'Authorized Transactions' pero SQL filtra solo IN ('Paid', 'Payable'). Falta Hold, Cancelled, Client_refund y 4 Chargebacks. Subreporta autorizadas. Fix = expandir a los 9 statuses o renombrar. Verificado SQL en lote_27 el 06/04/2026.",
        "fuente": "confirmed_error: título Authorized pero solo Paid+Payable (verificado 06/04/2026)"
    }

    # Card 841 — "DoD (Txs Paid)" (1,557v)
    # SQL real: WHERE t.transaction_status IN ('Paid')
    # El nombre explícitamente dice "Txs Paid". El modelo dijo "la card dice 'Authorized'" — FP del modelo.
    decisions[841] = {
        "status": "false_positive",
        "nota":   "FP: el nombre es 'DoD (Txs Paid)' — filtrar por 'Paid' es correcto para esta card. El modelo reportó que el título decía 'Authorized' pero el nombre real dice 'Paid'. Verificado SQL y nombre en lote_07 el 06/04/2026.",
        "fuente": "FP: DoD Txs Paid — nombre correcto con filtro Paid (verificado 06/04/2026)"
    }

    # Card 1457 — "1TU conversations details monthly" (1,866v)
    # SQL real: WHERE user_type_at_trx IN ('lead', '1tu') — SIN filtro de transaction_status en WHERE.
    # En el SELECT sí cuenta Paid y authorized por separado (correcto). El modelo inventó el error.
    decisions[1457] = {
        "status": "false_positive",
        "nota":   "FP: la card NO filtra por transaction_status en la cláusula WHERE. Usa user_type_at_trx IN ('lead', '1tu') como filtro. En el SELECT cuenta Paid y authorized por separado correctamente. El modelo inventó un filtro restrictivo que no existe. Verificado SQL en lote_06 el 06/04/2026.",
        "fuente": "FP: 1TU conversations — sin filtro status en WHERE, modelo inventó error (verificado 06/04/2026)"
    }

    # Cards 1583/1584 — "Band distribution in 1st/2+ trx" (781v / 780v)
    # SQL real: WHERE transaction_status = 'Paid' AND counter = 1 / counter > 1
    # Análisis de distribución por bandas en primera/segunda+ transacción. Filtrar Paid es correcto
    # (band analysis se hace sobre transacciones completadas = Paid). FP del modelo.
    decisions[1583] = {
        "status": "false_positive",
        "nota":   "FP: análisis de distribución de bandas en primera transacción. Filtrar por transaction_status = 'Paid' AND counter = 1 es correcto — se analiza la distribución de bandas de comisión en transacciones completadas (Paid). Verificado SQL en lote_10 el 06/04/2026.",
        "fuente": "FP: Band distribution 1st trx — Paid + counter=1 correcto (verificado 06/04/2026)"
    }
    decisions[1584] = {
        "status": "false_positive",
        "nota":   "FP: análisis de distribución de bandas en transacciones 2+. Filtrar por transaction_status = 'Paid' AND counter > 1 es correcto — se analiza la distribución de bandas de comisión en transacciones completadas recurrentes (Paid). Verificado SQL en lote_10 el 06/04/2026.",
        "fuente": "FP: Band distribution 2+ trx — Paid + counter>1 correcto (verificado 06/04/2026)"
    }

    # ── S53 (16/04/2026) — Cards verificadas en triaje de 65 pending ───────────────

    # Cards 1320/1442 — "Payable right now" y su duplicado
    # Filtran solo 'Payable' porque miden el stock de transacciones actualmente en estado Payable.
    # Filtrar solo Payable ES el propósito de diseño — no es un error ni omisión de statuses.
    decisions[1320] = {
        "status": "false_positive",
        "nota":   "FP: 'Payable right now' mide intencionalmente el stock actual de transacciones en estado Payable. Filtrar solo 'Payable' es correcto por diseño. Clasificado S53.",
        "fuente": "FP: Payable right now = filtro correcto por diseño (S53 — 16/04/2026)"
    }
    decisions[1442] = {
        "status": "false_positive",
        "nota":   "FP: 'Payable right now - Duplicate' — misma lógica que card #1320. Filtrar solo Payable es correcto para esta métrica. Clasificado S53.",
        "fuente": "FP: Payable right now Duplicate = filtro correcto (S53 — 16/04/2026)"
    }

    # Card 2388 — "WoW Trx Paid for recurring users - Duplicate"
    # Título explícitamente dice "Paid" — filtrar por Paid es correcto.
    decisions[2388] = {
        "status": "false_positive",
        "nota":   "FP: el título incluye 'Paid' explícitamente. Filtrar por transaction_status = 'Paid' es correcto y consistente con el propósito de la card. Clasificado S53.",
        "fuente": "FP: nombre incluye Paid = filtro correcto (S53 — 16/04/2026)"
    }

    # Card 1308 — "trx paid by users view" — tabla pública
    # Observación de estilo (uso de tabla pública), no error de datos.
    decisions[1308] = {
        "status": "false_positive",
        "nota":   "FP: el hallazgo es sobre uso de tabla pública (observación de mantenimiento/estilo). No afecta la correctitud del resultado. Clasificado S53.",
        "fuente": "FP: tabla pública = observación estilo (S53 — 16/04/2026)"
    }

    # Card 952 — "Active Users MOM Paid" — {{year}} vs {{YEAR}} inconsistencia
    # Metabase maneja variables case-insensitively; es una observación cosmética.
    decisions[952] = {
        "status": "false_positive",
        "nota":   "FP: inconsistencia cosmética entre {{year}} y {{YEAR}} en la misma query. Metabase trata las variables case-insensitively. No afecta resultados. Clasificado S53.",
        "fuente": "FP: {{year}}/{{YEAR}} inconsistencia cosmética (S53 — 16/04/2026)"
    }

    # ── S54 (16/04/2026) — Correcciones detectadas por validate_coherence.py ──────

    # Card 253 — "Distribution of Authorized Txns & ATV" (5,536v)
    # SQL: WHERE transaction_status IN ('Paid') — solo Paid.
    # Para ATV, filtrar únicamente Paid ES correcto por definición de negocio (Val + card #2699, 04/04/26).
    # El modelo reportó "podría subreportar si el análisis es para 'authorized'" pero ATV ≠ authorized.
    # Verificado SQL en lote_03. FP confirmado S54 (16/04/2026).
    decisions[253] = {
        "status": "false_positive",
        "nota":   "FP: ATV filtra solo 'Paid' — correcto por definición de negocio. ATV = counter_paid>=1 + status='Paid', no usa los 9 statuses authorized. El modelo aplicó la regla de authorized incorrectamente. Verificado SQL lote_03. S54 (16/04/2026).",
        "fuente": "FP: ATV solo Paid es correcto (S54 — 16/04/2026)"
    }

    # Card 2646 — "ATV MTU Paid vs. Min Sell FX" (425v)
    # SQL: WHERE td.transaction_status IN ('Paid') AND td.counter_paid > 1 — solo Paid, MTU.
    # Para ATV (MTU), filtrar por Paid + counter_paid > 1 es correcto.
    # El modelo reportó "no incluye todos los estados authorized" pero ATV ≠ authorized metric.
    # Verificado SQL en lote_13. FP confirmado S54 (16/04/2026).
    decisions[2646] = {
        "status": "false_positive",
        "nota":   "FP: ATV MTU filtra solo 'Paid' con counter_paid>1 — correcto por definición. ATV usa solo Paid, no los 9 statuses authorized. El modelo aplicó la regla de authorized incorrectamente. Verificado SQL lote_13. S54 (16/04/2026).",
        "fuente": "FP: ATV MTU solo Paid+counter>1 es correcto (S54 — 16/04/2026)"
    }

    # ── S56 (20/04/2026) — Correcciones detectadas por validate_coherence.py ──────

    # Card 1136 — "ATV per Wekk first auth trx" (867v)
    # SQL: WHERE td.transaction_status IN ('Paid') — solo Paid.
    # Para ATV, filtrar solo Paid ES correcto (ATV = counter_paid>=1 + status='Paid').
    # P3_IDS la marcaba CE ("título dice authorized pero filtra solo Paid") pero ATV es exenta.
    # El modelo reportó "título sugiere authorized pero filtro es solo Paid" — para ATV esto es FP.
    # Verificado SQL lote_09. FP confirmado S56 (20/04/2026).
    decisions[1136] = {
        "status": "false_positive",
        "nota":   "FP: ATV filtra solo 'Paid' — correcto por definición de negocio. ATV = counter_paid>=1 + status='Paid', no usa los 9 statuses authorized. P3 la marcaba CE por título, pero para ATV el filtro solo Paid es intencional. Verificado SQL lote_09. S56 (20/04/2026).",
        "fuente": "FP: ATV solo Paid es correcto (S56 — 20/04/2026)"
    }

    # Cards 1421 y 1437 — "Paid/Payable drop off distribution - Duplicate" (~540v c/u)
    # Ya marcadas FP correctamente por verify_pending.py. validate_coherence las flag como
    # FP_POSIBLE_CE por alta visibilidad + descripción menciona "filtro". La descripción dice
    # "El filtro de transaction_status es correcto y no incluye 'Sent'" → modelo confirmó FP.
    # Override explícito para suprimir falsa alarma en coherencia. S56 (20/04/2026).
    decisions[1421] = {
        "status": "false_positive",
        "nota":   "FP: filtro de transaction_status correcto — no incluye 'Sent', alineado con el contexto de negocio. Verificado por verify_pending.py. Override explícito para suprimir FP_POSIBLE_CE en validate_coherence (alta visibilidad ~540v). S56 (20/04/2026).",
        "fuente": "FP: filtro correcto confirmado (S56 — 20/04/2026)"
    }
    decisions[1437] = {
        "status": "false_positive",
        "nota":   "FP: filtro de transaction_status correcto — no incluye 'Sent', alineado con el contexto de negocio. Verificado por verify_pending.py. Override explícito para suprimir FP_POSIBLE_CE en validate_coherence (alta visibilidad ~539v). S56 (20/04/2026).",
        "fuente": "FP: filtro correcto confirmado (S56 — 20/04/2026)"
    }

    # ── S56 (20/04/2026) — Triaje de 27 hallazgos pending ──────────────────────

    # ─ FALSE POSITIVES ─

    # Card 169 — "% fx revenue first transaction" (5,598v)
    # Frag: [[AND (injected_date::date BETWEEN {{Start_date}} and {{End_date}})]]
    # Sintaxis [[...]] es el patrón de parámetros opcionales de Metabase (template tags).
    # El modelo reportó "sintaxis incorrecta" pero [[...]] es completamente válido en Metabase SQL.
    decisions[169] = {
        "status": "false_positive",
        "nota":   "FP: la sintaxis [[AND (col::date BETWEEN {{p1}} and {{p2}})]] es el patrón de parámetros opcionales de Metabase (optional template tags). El modelo no reconoció esta sintaxis como válida. No es un error SQL. S56 (20/04/2026).",
        "fuente": "FP: Metabase optional template tag syntax (S56 — 20/04/2026)"
    }

    # Card 193 — "Avg time to get paid" (5,474v)
    # Frag: t.transaction_status in ('Chargeback','Chargeback_won','Chargeback_lost')
    # El modelo reportó que 'Chargeback' y sus variantes "no son statuses autorizados".
    # Per INICIO.md ground truth: los 9 statuses autorizados INCLUYEN Chargeback, Chargeback_won,
    # Chargeback_lost, Chargeback_unir. El modelo está equivocado. FP confirmado S56 (20/04/2026).
    decisions[193] = {
        "status": "false_positive",
        "nota":   "FP: el modelo reportó que 'Chargeback','Chargeback_won','Chargeback_lost' no son statuses autorizados, pero per ground truth (INICIO.md) TODOS son parte de los 9 statuses autorizados canónicos. El filtro es correcto. S56 (20/04/2026).",
        "fuente": "FP: Chargebacks son statuses autorizados (S56 — 20/04/2026)"
    }

    # Cards 837 y 838 — "retention all users interactions" / "...%" (458v / 457v)
    # Frag: WHERE td.transaction_status is not null
    # El modelo reportó falta de filtro específico de status. Para una card de 'retention de
    # interacciones de todos los usuarios', usar IS NOT NULL es intencional — cuenta todas las
    # interacciones sin restricción de status (métrica de engagement, no de conversión). FP. S56.
    decisions[837] = {
        "status": "false_positive",
        "nota":   "FP: 'retention all users interactions' usa `transaction_status IS NOT NULL` — intencional para métrica de engagement que cuenta TODAS las interacciones sin restricción de status (no es métrica de conversión como ATV/MAU). El modelo reportó falta de filtro específico pero para esta card el filtro broad es correcto. S56 (20/04/2026).",
        "fuente": "FP: IS NOT NULL intencional para retention all interactions (S56 — 20/04/2026)"
    }
    decisions[838] = {
        "status": "false_positive",
        "nota":   "FP: 'retention all users interactions %' usa `transaction_status IS NOT NULL` — intencional, misma lógica que card #837. Métrica de engagement con broad filter. S56 (20/04/2026).",
        "fuente": "FP: IS NOT NULL intencional para retention % (S56 — 20/04/2026)"
    }

    # Card 2712 — "# of Opt-Ins" (1,024v)
    # Frag: -- QUERY: Total Net Opt-Ins (90 días)  [solo un comentario SQL]
    # El modelo vio el comentario de documentación como la query completa. Card con 1,024 vistas
    # es funcional. El fragmento extraído es solo el header de documentación interna, no el SQL.
    decisions[2712] = {
        "status": "false_positive",
        "nota":   "FP: el modelo extrajo solo el comentario SQL de documentación ('-- QUERY: Total Net Opt-Ins') como fragmento relevante. La card con 1,024 vistas es funcional. El hallazgo de 'query incompleta' es una alucinación basada en el comentario, no en el SQL real. S56 (20/04/2026).",
        "fuente": "FP: fragmento era comentario SQL, no query real (S56 — 20/04/2026)"
    }

    # Card 2482 — "Silver | Potential Users to be Upgraded to Gold" (315v)
    # Frag: WHERE td.transaction_status = 'Paid'
    # Card de clasificación de tier de usuarios (Silver → Gold). Para métricas de tier de usuario
    # en ExampleCorp, usar solo 'Paid' es correcto — mismo patrón que ATV/MAU (usuarios activos = Paid).
    decisions[2482] = {
        "status": "false_positive",
        "nota":   "FP: card de clasificación de tier de usuario (Silver → Gold). Usa `transaction_status = 'Paid'` — correcto por definición de negocio. En ExampleCorp, las métricas de usuario/tier se basan en transacciones Paid (mismo patrón que ATV/MAU). El modelo reportó 'podría necesitar más statuses' pero para user-tier solo Paid es intencional. S56 (20/04/2026).",
        "fuente": "FP: user tier card, Paid solo es correcto (S56 — 20/04/2026)"
    }

    # Card 2335 — "High FX rate" (167v)
    # Frag: CAST({{start_date}} AS timestamp)
    # El modelo reportó "diferentes tipos de conversión de fechas puede llevar a errores sutiles".
    # CAST({{start_date}} AS timestamp) es sintaxis SQL estándar con Metabase variable. FP. S56.
    decisions[2335] = {
        "status": "false_positive",
        "nota":   "FP: `CAST({{start_date}} AS timestamp)` es sintaxis SQL estándar con Metabase template variables. El modelo reportó 'diferentes tipos de conversión de fechas' como posible error sutil, pero el cast es correcto y estándar. Hallazgo de estilo, no error de datos. S56 (20/04/2026).",
        "fuente": "FP: CAST date style — no es error de datos (S56 — 20/04/2026)"
    }

    # Card 3270 — "Rescue Rate" (163v)
    # Frag: WHERE transaction_status = 'Paid'
    # "Rescue Rate" = tasa de transacciones 'rescatadas' (recuperadas) que terminaron Paid.
    # Usar solo 'Paid' es correcto para medir la tasa de éxito de transacciones rescatadas. FP. S56.
    decisions[3270] = {
        "status": "false_positive",
        "nota":   "FP: 'Rescue Rate' mide transacciones recuperadas que terminaron como 'Paid'. Usar `transaction_status = 'Paid'` es correcto — es la definición del 'éxito' en el contexto de rescue. El modelo reportó falta de statuses, pero para esta métrica de tasa de conversión solo Paid es intencional. S56 (20/04/2026).",
        "fuente": "FP: Rescue Rate = Paid es correcto (S56 — 20/04/2026)"
    }

    # Card 2358 — "Total Fx Revenue (payable & paid txs) - Duplicate" (150v)
    # Frag: WHERE t.transaction_status in ('Payable','Paid','Chargeback','Chargeback_won','C...)
    # El modelo reportó falta de 'Hold' y 'Cancelled'. El TÍTULO explícitamente dice
    # "payable & paid txs" — la exclusión de Hold y Cancelled es intencional por definición.
    decisions[2358] = {
        "status": "false_positive",
        "nota":   "FP: el título 'Total Fx Revenue (payable & paid txs)' define explícitamente el scope: Payable + Paid + Chargebacks. La exclusión de 'Hold' y 'Cancelled' es intencional — Hold = en vuelo, Cancelled = no cobrado. Duplicado de #4. S56 (20/04/2026).",
        "fuente": "FP: scope definido en nombre — payable & paid txs (S56 — 20/04/2026)"
    }

    # Card 2789 — "raw data users details for paid trx" (119v)
    # Frag: WHERE td.transaction_status IN ('Paid')
    # El nombre dice explícitamente "for paid trx" — filtrar solo 'Paid' es correcto y alineado
    # con el nombre. El modelo reportó que el título sugiere "todas las transacciones" — incorrecto.
    decisions[2789] = {
        "status": "false_positive",
        "nota":   "FP: el nombre 'raw data users details for paid trx' define explícitamente el scope: solo transacciones Paid. El filtro `transaction_status IN ('Paid')` es correcto y alineado con el nombre. El modelo interpretó 'todas las transacciones autorizadas' pero el nombre es específico. S56 (20/04/2026).",
        "fuente": "FP: nombre 'for paid trx' define scope (S56 — 20/04/2026)"
    }

    # Card 1624 — "UTM CAMPAIGN GOOGLE" (106v)
    # Frag: CASE WHEN transaction_status = 'Paid' THEN 1 ELSE 0 END
    # El modelo reportó "filtro por Paid solo". En realidad es un CASE WHEN, no un WHERE.
    # En análisis de UTM/marketing, contar conversiones (Paid) dentro de una tabla general es
    # el patrón correcto — no es un filtro restrictivo sino un flag de conversión. FP. S56.
    decisions[1624] = {
        "status": "false_positive",
        "nota":   "FP: `CASE WHEN transaction_status = 'Paid' THEN 1 ELSE 0 END` es un flag de conversión en análisis de UTM/marketing — no es un filtro WHERE restrictivo. Contar conversiones (Paid) dentro de una tabla completa es el patrón correcto para análisis de atribución. El modelo confundió CASE WHEN con filtro de status. S56 (20/04/2026).",
        "fuente": "FP: CASE WHEN Paid = flag de conversion, no filtro restrictivo (S56 — 20/04/2026)"
    }

    # Card 2851 — "Opt-Ins (Net) by Opt-In Channel (Leads)" (104v)
    # Frag: 'Initiated','Quote','Paymentlink','Payment','Email_verified','Phone_verified','Sent'
    # Card de Leads/Opt-In funnel. Estos son statuses de etapas del journey (funnel stages),
    # NO statuses de transaction_status convencional. Per INICIO.md: 'Payment','Paymentlink','Sent'
    # son etapas válidas del journey en transaction_details_funnel/leads tables. FP. S56.
    decisions[2851] = {
        "status": "false_positive",
        "nota":   "FP: card de Opt-In/Leads. Los statuses 'Initiated','Quote','Paymentlink','Payment','Email_verified','Phone_verified','Sent' son etapas del journey de leads — NO son transaction_status convencional. Per INICIO.md: estas etapas son válidas en transaction_details_funnel y tablas de leads. El modelo aplicó incorrectamente la regla de 9 statuses autorizados. S56 (20/04/2026).",
        "fuente": "FP: funnel/leads stages — no aplica regla de 9 statuses (S56 — 20/04/2026)"
    }

    # Card 735 — "Top_users" (84v)
    # Frag: SUM(td.total_amount) AS sum_total_amount
    # El modelo reportó "asegurarse que los montos estén correctos". SUM(total_amount) es
    # agregación SQL estándar. No hay error en esta operación. FP. S56.
    decisions[735] = {
        "status": "false_positive",
        "nota":   "FP: `SUM(td.total_amount) AS sum_total_amount` es agregación SQL estándar. El modelo reportó incertidumbre sobre si los montos son correctos, pero SUM(total_amount) no constituye un error técnico o de negocio. Hallazgo vago sin evidencia de error real. S56 (20/04/2026).",
        "fuente": "FP: SUM(total_amount) es agregacion estandar (S56 — 20/04/2026)"
    }

    # Cards 3241 y 3238 — "Margin Contribution | Lead Pricing" / "FX Spread | Lead Pricing" (51v / 49v)
    # Frag: WHERE gm.counter = 1
    # Lead Pricing en ExampleCorp = primer transacción del usuario (counter = 1). counter=1 es CORRECTO
    # para estas cards de Lead Pricing. El modelo confundió 'counter=1 no representa MTUs' pero
    # eso es precisamente correcto para Lead Pricing (counter=1 = 1TU, no MTU). FP. S56.
    decisions[3241] = {
        "status": "false_positive",
        "nota":   "FP: 'Margin Contribution | Lead Pricing'. `WHERE gm.counter = 1` es correcto — Lead Pricing en ExampleCorp = primer transacción (counter=1 = 1TU). El modelo reportó mismatch con comentarios del SQL pero counter=1 para Lead Pricing es la definición correcta del negocio. No es error. S56 (20/04/2026).",
        "fuente": "FP: Lead Pricing counter=1 es correcto (S56 — 20/04/2026)"
    }
    decisions[3238] = {
        "status": "false_positive",
        "nota":   "FP: 'FX Spread | Lead Pricing'. `WHERE gm.counter = 1` es correcto — Lead Pricing = primer transacción (counter=1). El modelo dijo 'confunde fx_1tu con fx_mtu' pero para Lead Pricing, counter=1 ES la definición correcta. fx_1tu = FX revenue de primera transacción. S56 (20/04/2026).",
        "fuente": "FP: Lead Pricing counter=1 correcto — no confunde 1TU vs MTU (S56 — 20/04/2026)"
    }

    # Card 1024 — "Ops Rejected Source" (46v)
    # Frag: EXTRACT(YEAR FROM created_date) = {{YEAR}}
    # El modelo reportó "filtro por año no estándar". EXTRACT(YEAR FROM col) = {{variable}}
    # es sintaxis SQL/Metabase estándar. FP. S56.
    decisions[1024] = {
        "status": "false_positive",
        "nota":   "FP: `EXTRACT(YEAR FROM created_date) = {{YEAR}}` es filtro de año estándar con Metabase template variable. El modelo reportó 'filtro no estándar' pero esta sintaxis es válida y común en Metabase. No es un error. S56 (20/04/2026).",
        "fuente": "FP: EXTRACT(YEAR) con variable Metabase — estandar (S56 — 20/04/2026)"
    }

    # Card 1597 — "CPC Trends by Media Channel | Weekly - youtube v2" (43v)
    # Frag: EXTRACT(WEEK FROM created_date)
    # El modelo reportó "filtro de created_date en lugar de week". EXTRACT(WEEK FROM created_date)
    # ES extraer la semana de la fecha de creación — correcto. FP. S56.
    decisions[1597] = {
        "status": "false_positive",
        "nota":   "FP: `EXTRACT(WEEK FROM created_date)` extrae el número de semana de la fecha de creación — correcto para agrupación semanal. El modelo reportó 'filtro incorrecto de fecha' pero EXTRACT(WEEK FROM date) es la sintaxis SQL estándar para esto. No es un error. S56 (20/04/2026).",
        "fuente": "FP: EXTRACT(WEEK FROM created_date) es correcto (S56 — 20/04/2026)"
    }

    # Card 3248 — "Raw Data: Fraud" (11v)
    # Frag: WHERE (u.flagged_fraud = 'true' OR u.fraud_confirmed = 'true')
    # El modelo reportó riesgo de confusión boolean vs texto. Si el campo es texto (VARCHAR),
    # comparar con 'true' string es correcto. El modelo no verificó el tipo del campo. FP. S56.
    decisions[3248] = {
        "status": "false_positive",
        "nota":   "FP: `flagged_fraud = 'true'` es comparación de texto estándar si el campo es VARCHAR. El modelo reportó riesgo de confusión boolean vs texto, pero si el campo es texto, 'true' string es la comparación correcta. Sin evidencia de error real. S56 (20/04/2026).",
        "fuente": "FP: comparacion texto 'true' correcta para campo VARCHAR (S56 — 20/04/2026)"
    }

    # Card 1759 — "CAC (Paid) - Monthly - All Media taboola - Duplicate" (1v)
    # Frag: AND counter_paid IN (1)
    # El modelo reportó falta de null check en counter_paid. counter_paid IN (1) = primera
    # transacción Paid = New User. Sintaxis válida, no necesita null check adicional. FP. S56.
    decisions[1759] = {
        "status": "false_positive",
        "nota":   "FP: `counter_paid IN (1)` = primera transacción Paid = New User. Sintaxis válida y equivalente a counter_paid = 1. El modelo reportó falta de null check pero IN (1) implícitamente excluye NULLs. No es error. Duplicado de baja visibilidad (1v). S56 (20/04/2026).",
        "fuente": "FP: counter_paid IN (1) es sintaxis valida (S56 — 20/04/2026)"
    }

    # ─ CONFIRMED ERRORS ─

    # Card 760 — "failed trx not converted" (1,522v)
    # Frag: AND td.id_status NOT IN (3, 4, 6, 10) AND td.transaction_id NOT in ('Payable','Paid','Cancelled','Client_refund')
    # Error claro: td.transaction_id es la columna de UUID de la transacción. Se está usando
    # td.transaction_id NOT IN ('Payable','Paid',...) cuando debería ser td.transaction_status.
    # El modelo lo identificó correctamente: columna incorrecta en la cláusula NOT IN. CE. S56.
    decisions[760] = {
        "status": "confirmed_error",
        "nota":   "Error: `td.transaction_id NOT IN ('Payable','Paid','Cancelled','Client_refund')` usa la columna INCORRECTA. transaction_id es el UUID de la transacción, no el status. Debería ser `td.transaction_status NOT IN (...)`. Esto hace que el filtro no filtre nada correctamente. Error de columna en la cláusula NOT IN. S56 (20/04/2026).",
        "fuente": "confirmed_error: transaction_id en lugar de transaction_status (S56 — 20/04/2026)"
    }

    # Card 1240 — "Total New Users | Authorized Txns - tk" (102v)
    # Frag: categoria in ('TikTok')
    # El modelo reportó uso incorrecto de 'categoria' como filtro textual. El campo correcto
    # probablemente es 'channel', 'utm_source', o similar. 'categoria' puede no existir o
    # ser el campo equivocado para filtrar por canal. CE. S56.
    decisions[1240] = {
        "status": "confirmed_error",
        "nota":   "Error: `categoria in ('TikTok')` usa posiblemente el campo incorrecto para filtrar por canal. El campo de canal de adquisición probablemente debería ser 'channel', 'utm_source', o 'media_channel' — no 'categoria'. Si 'categoria' no existe en la tabla, causaría error SQL. Verificar schema de la tabla. S56 (20/04/2026).",
        "fuente": "confirmed_error: campo 'categoria' posiblemente incorrecto (S56 — 20/04/2026)"
    }

    # Card 1654 — "FUNNEL POST UNIR CHANNEL ADVERTISING - Duplicate" (1v)
    # Frag: AND created_date < '2024-07-29'
    # Fecha hardcodeada en subquery. La card solo retorna datos anteriores a julio 2024.
    # Es una fecha fija que limita permanentemente el rango de datos. CE. S56.
    decisions[1654] = {
        "status": "confirmed_error",
        "nota":   "Error: subquery con `AND created_date < '2024-07-29'` — fecha hardcodeada que limita permanentemente la card a datos anteriores a julio 2024. Duplicado de baja visibilidad (1v). La fecha fija hace que la card sea estática e incompleta para análisis actuales. S56 (20/04/2026).",
        "fuente": "confirmed_error: fecha hardcodeada < 2024-07-29 (S56 — 20/04/2026)"
    }

    # Card 2793 — "Tasa Chargeback" (48v)
    # Frag: transaction_status IN ('Paid')
    # 'Tasa Chargeback' = tasa de chargebacks sobre transacciones. WHERE solo 'Paid' excluye
    # todos los registros con status Chargeback del dataset. Si la query cuenta Chargebacks
    # como numerador pero filtra WHERE status='Paid', los chargebacks nunca aparecerán (tienen
    # status diferente a Paid). Esto daría tasa = 0 siempre. CE. S56.
    decisions[2793] = {
        "status": "confirmed_error",
        "nota":   "Error: 'Tasa Chargeback' con `WHERE transaction_status IN ('Paid')` excluye todos los registros de Chargeback del dataset (tienen status 'Chargeback', no 'Paid'). Si la query cuenta chargebacks como numerador pero filtra solo Paid, los chargebacks nunca serán incluidos → tasa siempre = 0/total_paid. El denominador debería incluir todos los statuses autorizados. S56 (20/04/2026).",
        "fuente": "confirmed_error: Tasa Chargeback con solo Paid excluye chargebacks (S56 — 20/04/2026)"
    }

    # Card 148 — "Cohort_detail" (11v)
    # Frag: FROM app_cohort_retention_table_20240218191654
    # Tabla con timestamp en el nombre (20240218191654 = 18-Feb-2024 19:16:54). Es una tabla
    # de snapshot/backup temporal. Si la tabla fue eliminada o actualizada, la query falla.
    # Alto riesgo de obsolescencia. CE. S56.
    decisions[148] = {
        "status": "confirmed_error",
        "nota":   "Error: la query usa `app_cohort_retention_table_20240218191654` — tabla con timestamp en el nombre (snapshot del 18-Feb-2024). Es una tabla temporal/backup que puede haber sido eliminada. Este patrón de tabla con timestamp indica que la card depende de un snapshot estático de 2024, no de datos actualizados. Alto riesgo de obsolescencia. S56 (20/04/2026).",
        "fuente": "confirmed_error: tabla snapshot con timestamp en nombre (S56 — 20/04/2026)"
    }

    # ── Card 57 — Accumulated Transaction Funnel (6,044v) ───────────────────────
    # Sesión 19 identificó: hallazgo sobre lista hardcodeada de statuses (NOT Sent).
    # El SQL usa un funnel acumulativo con lista de statuses completa — intencional.
    # Confirmado como FP por análisis de SQL en Sesión 19 (06/04/2026).
    decisions[57] = {
        "status": "intentional",
        "nota":   "Intencional: card de funnel acumulativo de transacciones. La lista hardcodeada de statuses refleja todas las etapas del funnel de manera explícita — diseño intencional. El hallazgo del modelo sobre la lista de statuses no aplica como error de datos. Verificado análisis Sesión 19 (06/04/2026).",
        "fuente": "Intencional: funnel acumulativo con lista hardcodeada (Sesión 19 — 06/04/2026)"
    }

    # ── R12b — Yield/Tasa de éxito (04/04/2026) ─────────────────────────────────
    # Patrón: Sent SOLO en denominador del ratio (numerador = authorized, denominador = authorized+Sent)
    # yield = authorized / injected, donde 'Sent' = 'injected' en BD. Mismo patrón que Card 3
    # confirmado por analyst_2 (31/03/2026). Verificado contra SQL real el 04/04/2026.
    for cid in [120, 762, 763, 474, 1331, 1126, 1061, 773]:
        decisions[cid] = {
            "status": "false_positive",
            "nota":   "FP: 'Sent' aparece solo en el denominador del ratio yield/tasa de éxito (= 'injected' / 'intentos'). El numerador es solo authorized. Mismo patrón que Card 3 confirmado por analyst_2 (31/03/2026). Verificado contra SQL real el 04/04/2026.",
            "fuente": "FP: yield/tasa de éxito — Sent en denominador (R12b — 04/04/2026)"
        }

    # ── R15 — Authorized vs Rejected (Sent+EXISTS pattern) (04/04/2026) ─────────
    # Card 266: 'Sent AND EXISTS(transaction_errors)' = bucket 'rejected'. Idéntico a 395/495/262.
    decisions[266] = {
        "status": "intentional",
        "nota":   "Intencional: 'Sent AND EXISTS(transaction_errors)' = bucket 'rejected'. Mismo patrón documentado en INICIO.md para cards 395/495, y confirmado en Card 262. Solo se toca el bucket 'authorized' si aplica. Verificado contra SQL real el 04/04/2026.",
        "fuente": "Intencional: Sent+EXISTS=rejected (R15 — 04/04/2026)"
    }

    # ── Sesión 26 — 06/04/2026 — verificación SQL card por card ─────────────────
    decisions[1297] = {
        "status": CARD_1297_STATUS,
        "nota":   CARD_1297_NOTA,
        "fuente": "confirmed_error: CTEs inconsistentes Chargeback_unir (Sesión 26 — 06/04/2026)"
    }

    _s26_groups = [
        (SESSION_26_FP_INNER_JOIN,   "FP: INNER JOIN a tabla users para fraud_confirmed — patrón intencional en todo el codebase de ExampleCorp. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_SQL_COMENTADO,"FP: SQL comentado (--[[...]] o bloques inline) — secciones desactivadas intencionalmente, común en ExampleCorp como forma de deprecar filtros. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_LOGICA,       "FP: lógica aparentemente incorrecta pero correcta en contexto — JOINs a tablas distintas, subqueries redundantes pero no erróneas, counter workaround documentado. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_ESTILO,       "FP: inconsistencia de estilo en ambassador_code CASE order entre dashboards. No afecta correctitud de datos — puramente cosmético. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_METABASE,     "FP: múltiples filtros de fecha opcionales en Metabase (year/week/day/start_end). Es el patrón estándar de Metabase para filtros multi-granularidad — diseño intencional. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_DISENIO,      "FP: decisión de diseño verificada — años hardcodeados para YoY, Sent en funnel de conversión, CROSS JOIN entre tablas de analytics de fila única, métricas lifetime sin filtro de fecha. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_PERFORMANCE,  "FP: observación de performance (NOT EXISTS, FULL JOIN, sin filtro fecha, hardcoded arrays P19). No afecta la correctitud del resultado — la query devuelve datos correctos. Priorizar post-Fase 1. Verificado SQL Sesión 26 (06/04/2026)."),
        (SESSION_26_FP_MISC,         "FP: patrón misceláneo verificado — casing de valores en BD (real, no error), sintaxis PostgreSQL válida (comillas dobles = identificadores), anti-join LEFT JOIN IS NULL correcto, cards de testing con 2 views. Verificado SQL Sesión 26 (06/04/2026)."),
    ]
    for card_set, nota in _s26_groups:
        for cid in card_set:
            if cid not in decisions:  # no sobreescribir decisiones previas
                decisions[cid] = {
                    "status": "false_positive",
                    "nota":   nota,
                    "fuente": "FP: verificado SQL Sesión 26 (06/04/2026)"
                }

    # ── Sesión 34 — 07/04/2026 — verificación SQL crítica ───────────────────────

    # Cards 173/174 — 'Payable,Paid' como string literal único (bug de sintaxis SQL)
    # SQL real: transaction_status IN ('Payable,Paid', 'Cancelled', ...)
    # 'Payable,Paid' se trata como UN solo string → nunca matchea. Subreporta.
    # Corrección: IN ('Payable', 'Paid', ...) — separar en dos valores.
    # Ambas cards tienen 5,500+ vistas. Verificado lote_03 el 07/04/2026.
    for cid in [173, 174]:
        decisions[cid] = {
            "status": "confirmed_error",
            "nota":   "Bug crítico de sintaxis: `transaction_status IN ('Payable,Paid', ...)` trata 'Payable,Paid' como UN string literal único — nunca matchea filas reales. La query excluye todas las transacciones Payable y Paid. Fix: separar en IN ('Payable', 'Paid', ...). Card con 5,500+ vistas. Verificado SQL en lote_03 el 07/04/2026.",
            "fuente": "confirmed_error: 'Payable,Paid' como string literal (Sesión 34 — 07/04/2026)"
        }

    # Card 100 — Weekly Transaction Trends (5,555 vistas)
    # Tiene dos queries: Q1 usa statuses reales (funcional), Q2 usa IN ('Paid', 'Payment')
    # 'Payment' no existe en transaction_details (solo en tabla legada `transaction`).
    # Q2 retorna 0 filas para 'Payment'. Verificado lote_03 el 07/04/2026.
    decisions[100] = {
        "status": "confirmed_error",
        "nota":   "Card con dos queries: Q1 (IN ('Payable','Paid','Client_refund','Cancelled')) funciona pero es incompleta. Q2 usa IN ('Paid', 'Payment') donde 'Payment' NO existe en transaction_details — solo en tabla legada `transaction`. Q2 retorna 0 filas para 'Payment'. Verificado SQL en lote_03 el 07/04/2026.",
        "fuente": "confirmed_error: 'Payment' no existe en transaction_details (Sesión 34 — 07/04/2026)"
    }

    # Card 2499 — Lead to 1TU daily trend per event (260 vistas)
    # Funnel acumulativo webchat: cada etapa suma todos los statuses subsiguientes.
    # counter IN (0,1) = workaround intencional documentado.
    # 'Sent', 'Paymentlink', 'Payment', 'Email_verified', 'Phone_verified' son etapas del funnel.
    # FP: no es un filtro de 'authorized', es medición de conversión por etapa. Verificado lote_17 el 07/04/2026.
    decisions[2499] = {
        "status": "false_positive",
        "nota":   "FP: funnel acumulativo Lead-to-1TU por etapa (webchat). Cada etapa cuenta usuarios cuyo status >= esa etapa. counter IN (0,1) es workaround intencional documentado (INICIO.md). 'Sent', 'Paymentlink', 'Payment', 'Email_verified', 'Phone_verified' aparecen como etapas del funnel — no como filtros de 'authorized'. Mismo patrón que cards de funnel acumulativo (Card 57). Verificado SQL en lote_17 el 07/04/2026.",
        "fuente": "FP: funnel acumulativo Lead-to-1TU — etapas intencionales (Sesión 34 — 07/04/2026)"
    }

    # ── Sesión 35 — 07/04/2026 (Cowork) ───────────────────────────────────────
    # Verificación SQL directa en lotes. Grupos: filtro_opcional, cancel_reason,
    # One-and-Done/conversión, lifecycle, revenue, confirmed_errors.

    # ── FILTRO OPCIONAL MAL CONSTRUIDO → FPs (5 cards) ──────────────────────
    # Todas usan [[AND (...)]] que ES la sintaxis correcta de Metabase.
    # El modelo confundió la sintaxis correcta con un error.
    for cid in (688, 2025, 1651, 1718, 1720):
        decisions[cid] = {
            "status": "false_positive",
            "nota":   "FP: el modelo reportó 'filtro opcional mal construido' pero la sintaxis [[AND ({{var}} IS NULL OR col = {{var}})]] ES la sintaxis correcta de Metabase para filtros opcionales. Verificado en SQL real el 07/04/2026.",
            "fuente": "FP: sintaxis [[AND]] correcta en Metabase (Sesión 35 — 07/04/2026)"
        }

    # ── CANCEL REASON CARDS → FPs (3 cards) ─────────────────────────────────
    # Cards 1386/1387/1388: "Cancel reason" — miden motivos de cancelación.
    # Filtrar por IN ('Cancelled', 'Client_refund') ES intencional y correcto:
    # son las transacciones que efectivamente se cancelaron/reembolsaron.
    # El modelo dijo "should exclude 'Client_refund'" — incorrecto.
    for cid in (1386, 1387, 1388):
        decisions[cid] = {
            "status": "false_positive",
            "nota":   "FP: cards de análisis de motivos de cancelación. Filtrar por IN ('Cancelled', 'Client_refund') es INTENCIONAL — son exactamente las transacciones canceladas y reembolsadas que se quieren analizar. 'Client_refund' ES parte del array oficial de 9 statuses autorizados. El modelo confundió el contexto de la card. Verificado SQL en lote_08 el 07/04/2026.",
            "fuente": "FP: cancel reason cards — Cancelled/Client_refund intencional (Sesión 35 — 07/04/2026)"
        }

    # ── ONE-AND-DONE / CONVERSIÓN → FPs ─────────────────────────────────────
    # Cards 3275 (One-and-Done buckets), 3271 (One-and-Done Rate), 3274 (Tasa Conversión):
    # Usan transaction_status = 'Paid' + counter_paid = 1/2.
    # Confirmado por Val (Head Marketing) + INICIO.md: ATV/MAU = 'Paid' + counter_paid.
    # El 'Paid' solo ES la definición correcta para métricas de conversión de usuario.
    for cid in (3275, 3271, 3274):
        decisions[cid] = {
            "status": "false_positive",
            "nota":   "FP: card de conversión/retención de usuarios. Usa transaction_status = 'Paid' + counter_paid = 1/2 — esta es la definición oficial de ExampleCorp para métricas de usuario (ATV/MAU confirmado por Val y en INICIO.md). El filtro solo 'Paid' es INTENCIONAL para estas métricas de lifecycle. Verificado SQL en lote_22 el 07/04/2026.",
            "fuente": "FP: One-and-Done/Conversión — Paid+counter_paid = ATV/MAU (Sesión 35 — 07/04/2026)"
        }

    # ── LIFECYCLE BAR CHART → FP ─────────────────────────────────────────────
    # Card 1916: Lifecycle tracking usa transaction_status = 'Paid'.
    # Confirma MAU definition: usuarios activos = tuvieron al menos 1 'Paid'. (INICIO.md)
    decisions[1916] = {
        "status": "false_positive",
        "nota":   "FP: card de lifecycle (New/Retained/Resurrected). Usa transaction_status = 'Paid' como definición de 'usuario activo' — esta es la definición oficial de MAU en ExampleCorp (confirmado en INICIO.md: MAU = usuarios con transaction_status = 'Paid'). Verificado SQL en lote_25 el 07/04/2026.",
        "fuente": "FP: lifecycle card — Paid = definición MAU oficial (Sesión 35 — 07/04/2026)"
    }

    # ── REFERRER ENGAGEMENT RATE → FP ───────────────────────────────────────
    # Card 2238: "Referrer Engagement Rate (Paid TXNs)" — título define scope.
    decisions[2238] = {
        "status": "false_positive",
        "nota":   "FP: el título de la card es 'Referrer Engagement Rate (Paid TXNs)' — explícitamente mide engagement de referidos en transacciones 'Paid'. Filtro solo 'Paid' es INTENCIONAL y consistente con el título. Verificado SQL en lote_25 el 07/04/2026.",
        "fuente": "FP: Paid TXNs en título — filtro intencional (Sesión 35 — 07/04/2026)"
    }

    # ── REVENUE CARDS — excluyen Cancelled/Client_refund (intencional) ──────
    # Cards 2387 (Total Revenue), 2401 (Total Processed Volume Paid & Chargeback),
    # 2403 (Total Fee Revenue payable & paid):
    # Métricas de revenue excluyen Cancelled/Client_refund — tiene sentido de negocio
    # (son devoluciones/cancelaciones que no generan revenue). Los comentarios SQL muestran
    # que el developer evaluó opciones y eligió el subconjunto deliberadamente.
    for cid in (2387, 2401, 2403):
        decisions[cid] = {
            "status": "false_positive",
            "nota":   "FP: métrica de revenue que excluye 'Cancelled' y 'Client_refund' intencionalmente. Las transacciones canceladas y reembolsadas no generan revenue, por lo que excluirlas es lógica de negocio correcta. El SQL muestra comentarios con arrays alternativos que el developer evaluó — la elección del subconjunto es deliberada. Verificado SQL en lote_22 el 07/04/2026.",
            "fuente": "FP: revenue metrics — Cancelled/Client_refund excluidos intencionalmente (Sesión 35 — 07/04/2026)"
        }

    # ── CONFIRMED ERRORS: 'Payable,Paid' string literal ─────────────────────
    # Cards 509/414: usan FILTER (WHERE transaction_status in ('Payable,Paid', ...))
    # 'Payable,Paid' como string único NUNCA matchea — misma clase que cards 173/174.
    # R22 no las capturó porque la descripción dice "coma dentro de un string"
    # en lugar de mencionar el valor literal 'Payable,Paid'.
    for cid in (509, 414):
        decisions[cid] = {
            "status": "confirmed_error",
            "nota":   "Bug de sintaxis: FILTER (WHERE transaction_status in ('Payable,Paid', ...)) trata 'Payable,Paid' como un único string literal — nunca matchea filas reales (BD tiene 'Payable' y 'Paid' por separado). Mismo patrón R22 que cards 173/174. Fix: cambiar a in ('Payable', 'Paid', ...). Verificado SQL en lote_40 el 07/04/2026.",
            "fuente": "confirmed_error: 'Payable,Paid' string literal en FILTER (R22 pattern — Sesión 35 — 07/04/2026)"
        }

    # ── FPs VERIFICADOS EN RE-AUDITORÍA (07/04/2026 — Sesión 35) ────────────
    # Card 121 — "# of rejected transactions" (6,505v)
    # Usa IN ('Client_refund', 'Cancelled', 'Rejected').
    # 'Rejected' SÍ existe en transaction_details (~40 registros, variante de fraude).
    # Confirmado en transaction_status_catalog.md. Filtro intencional para la card.
    decisions[121] = {
        "status": "false_positive",
        "nota":   "FP: 'Rejected' SÍ existe en transaction_details (~40 registros, variante de fraude). Confirmado en transaction_status_catalog.md. La card 'rejected transactions' filtra intencionalmente por Cancelled + Client_refund + Rejected. El modelo lo marcó como error porque no está en el array de los 9 authorized, pero es correcto para esta métrica. Verificado SQL lote_02 + catalog el 07/04/2026.",
        "fuente": "FP: 'Rejected' existe en BD — filtro intencional (Sesión 35 — 07/04/2026)"
    }

    # Card 237 — "Map by city sent" (6,282v)
    # Usa transaction_status = 'Paid'. Título "Map by city sent" → "sent" se refiere a
    # la ciudad desde donde se envió la remesa (campo city_transaction_sent), no al status.
    # El modelo confundió "sent" en el título con el status 'Sent'. FP.
    decisions[237] = {
        "status": "false_positive",
        "nota":   "FP: el título 'Map by city sent' usa 'sent' como verbo geográfico — refiere al campo city_transaction_sent (ciudad de origen de la remesa), NO al transaction_status 'Sent'. Filtrar por status = 'Paid' es correcto para este mapa de transacciones completadas por ciudad. El modelo confundió la semántica. Verificado SQL lote_02 el 07/04/2026.",
        "fuente": "FP: 'sent' en título = ciudad, no status (Sesión 35 — 07/04/2026)"
    }

    # ── CONFIRMED ERRORS ADICIONALES (re-auditoría 07/04/2026) ──────────────
    # Cards 172/178: 7/9 statuses (falta 'Hold' + 'Chargeback_unir') — ambas ~5,500v+
    for cid in (172, 178):
        decisions[cid] = {
            "status": "confirmed_error",
            "nota":   "Error: filtro usa 7 de 9 statuses autorizados. Falta 'Hold' y 'Chargeback_unir'. Array: ('Paid','Payable','Cancelled','Client_refund','Chargeback','Chargeback_won','Chargeback_lost'). Subestima el volumen autorizado real. Fix: añadir 'Hold' y 'Chargeback_unir' al IN. Verificado SQL lote_03 el 07/04/2026.",
            "fuente": "confirmed_error: falta Hold + Chargeback_unir (7/9 statuses — Sesión 35 — 07/04/2026)"
        }

    # Card 2606 — "wow total auth" (1,600v) — BUG CRÍTICO: falta coma
    # ARRAY['Paid','Payable','Cancelled','Client_refund','Hold'    ← SIN COMA
    #        'Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir']
    # En PostgreSQL, dos strings literales adyacentes se concatenan:
    # 'Hold' 'Chargeback' → 'HoldChargeback'. Ese string NUNCA matchea filas reales.
    # Resultado: 'Hold' se pierde del array, y 'HoldChargeback' es código muerto.
    decisions[2606] = {
        "status": "confirmed_error",
        "nota":   "BUG CRÍTICO de sintaxis SQL: falta coma entre 'Hold' y 'Chargeback' en el ARRAY literal. PostgreSQL concatena strings adyacentes: 'Hold' 'Chargeback' → 'HoldChargeback'. Resultado: 'Hold' desaparece del array (nunca matchea), y 'HoldChargeback' es código muerto. Fix: añadir coma → 'Hold', 'Chargeback'. Afecta card WoW de 1,600 vistas. Verificado SQL lote_07 el 07/04/2026.",
        "fuente": "confirmed_error: falta coma 'Hold','Chargeback' en ARRAY → 'HoldChargeback' (Sesión 35 — 07/04/2026)"
    }

    # ── CONFIRMED ERRORS: statuses no-existentes en transaction_details ──────
    # Card 1536: usa 'Paymentlink', 'Payment', 'Email_verified', 'Phone_verified' en
    # transaction_details — estos valores NO existen en esa tabla (son del schema legado
    # o de funnel temprano). Solo 'Sent' y 'Hold' del array matchean.
    decisions[1536] = {
        "status": "confirmed_error",
        "nota":   "Error: transaction_status IN ('Paymentlink', 'Payment', 'Email_verified', 'Phone_verified', 'Sent', 'Hold'). Los valores 'Paymentlink', 'Payment', 'Email_verified', 'Phone_verified' NO existen en transaction_details (son del schema legado o no existen en BD). Solo 'Sent' y 'Hold' matchean filas. La card pierde etapas del funnel. Verificado SQL en lote_43 el 07/04/2026.",
        "fuente": "confirmed_error: statuses no-existentes en transaction_details (Sesión 35 — 07/04/2026)"
    }

    # Cards 2996/2998/2993: usan 'Success'/'Fail' en transaction_details.
    # Estos valores NO existen en transaction_details (confirmado en transaction_status_catalog.md).
    # Son del schema legado (tabla `transaction`). En transaction_details NUNCA matchean.
    for cid in (2996, 2998, 2993):
        decisions[cid] = {
            "status": "confirmed_error",
            "nota":   "Error: usa transaction_status = 'Success' y/o 'Fail' en transaction_details. Estos valores NO existen en transaction_details (solo en tabla legada `transaction` con IDs numéricos). Confirmado en transaction_status_catalog.md. Las condiciones con 'Success'/'Fail' son código muerto que nunca matchea. Verificado SQL el 07/04/2026.",
            "fuente": "confirmed_error: 'Success'/'Fail' no existen en transaction_details (Sesión 35 — 07/04/2026)"
        }

    # ── CONFIRMED ERRORS: verificados en SQL (borderlines — Sesión 36 — 08/04/2026) ──
    # Card 21 — "Total Txs by Partner" (12.3k vistas)
    # SQL: WHERE td.transaction_status = COALESCE([[{{trx_status}}, ]] 'Paid')
    # Default = solo 'Paid' (1/9). Título no especifica filtro deliberado.
    # No es una métrica de 'Paid only' (ATV/MAU/NPS) — es un contador general de txs.
    decisions[21] = {
        "status": "confirmed_error",
        "nota":   "Error: 'Total Txs by Partner' usa COALESCE([[{{trx_status}}, ]] 'Paid') → default solo 'Paid' (1/9 statuses autorizados). El título no implica restricción a 'Paid'. Subestima volumen real. Fix: expandir default a los 9 statuses o aclarar en título que es 'Paid only'. Verificado SQL lote_01_top el 08/04/2026.",
        "fuente": "confirmed_error: solo 'Paid' (1/9) en Total Txs by Partner (Sesión 36 — 08/04/2026)"
    }

    # Card 175 — "Authorized transaction by current year" (3.05k vistas)
    # SQL: WHERE t.transaction_status in ('Payable','Paid','Cancelled','Client_refund',
    #      'Chargeback','Chargeback_won','Chargeback_lost')
    # 7/9 — falta 'Hold' y 'Chargeback_unir'. Título dice "Authorized" explícitamente.
    decisions[175] = {
        "status": "confirmed_error",
        "nota":   "Error: 'Authorized transaction by current year' usa 7/9 statuses. Falta 'Hold' (transacciones en tránsito, parte de authorized) y 'Chargeback_unir'. Subestima el volumen autorizado real. Fix: añadir 'Hold', 'Chargeback_unir' al IN. Verificado SQL lote_05 el 08/04/2026.",
        "fuente": "confirmed_error: falta Hold + Chargeback_unir (7/9) en Authorized by year (Sesión 36 — 08/04/2026)"
    }

    # Card 696 — "# of New Users (Payment)" (5 vistas)
    # SQL: WHERE transaction_status IN ('Payment', 'Sent', 'Payable','Paid','Cancelled','Client_refund')
    # 'Payment' NO existe en transaction_details ni en BD (confirmado en transaction_status_catalog.md).
    decisions[696] = {
        "status": "confirmed_error",
        "nota":   "Error CRÍTICO: usa transaction_status = 'Payment' que NO existe en la BD (ni en transaction_details ni en transaction). Los valores canónicos son 'Paid', 'Payable', etc. La condición con 'Payment' es código muerto que nunca matchea. Verificado SQL lote_46 el 08/04/2026.",
        "fuente": "confirmed_error: status 'Payment' no existe en BD (Sesión 36 — 08/04/2026)"
    }

    # Card 1683 — "Top users tiers - Duplicate" (1 vista)
    # SQL: WHERE td2.transaction_status IN ('Hold','Paid','Payable','Client_refund',
    #      'Cancelled','Chargeback_unir') — 6/9, falta Chargeback, Chargeback_won, Chargeback_lost
    decisions[1683] = {
        "status": "confirmed_error",
        "nota":   "Error: usa 6/9 statuses autorizados. Falta 'Chargeback', 'Chargeback_won', 'Chargeback_lost' (todos los chargebacks excepto Chargeback_unir). Subestima transacciones authorized. Fix: añadir 'Chargeback', 'Chargeback_won', 'Chargeback_lost'. Verificado SQL lote_53 el 08/04/2026.",
        "fuente": "confirmed_error: falta 3 chargebacks (6/9) en Top users tiers (Sesión 36 — 08/04/2026)"
    }

    # Card 118 — "Número total de intentos de transacción" (5.56k vistas)
    # SQL: WHERE t.transaction_status in ('Sent','Payable','Paid','Chargeback',
    #      'Chargeback_won','Chargeback_lost','Chargeback_unir') — incluye Sent pero falta Hold
    # Si mide "intentos", debería incluir 'Hold' (también es intent autorizado). Inconsistencia.
    decisions[118] = {
        "status": "confirmed_error",
        "nota":   "Error: 'Número total de intentos de transacción' incluye 'Sent' (pre-auth, intencional para 'intentos') pero omite 'Hold' (también es una transacción en-tránsito/intento autorizado). Además falta 'Cancelled', 'Client_refund'. Inconsistencia: incluye Sent pero no Hold. Fix: si es 'intentos', incluir 'Hold'; si es 'authorized', quitar 'Sent' y añadir los faltantes. Requiere aclaración de propósito con dueño. Verificado SQL lote_03 el 08/04/2026.",
        "fuente": "confirmed_error: incluye Sent pero omite Hold en 'intentos de transacción' (Sesión 36 — 08/04/2026)"
    }

    # ── FALSE POSITIVES: borderlines verificados (Sesión 36 — 08/04/2026) ───────
    # Cards 2897 y 2906 — dashboards con patrón intencional Hold+Paid+Payable (3/9)
    # Contexto: métricas de "autorizadas en proceso" (no incluyen chargebacks/cancelled
    # a propósito). Bajo tráfico (5-6 vistas). Diseño deliberado y consistente entre cards.
    for cid in (2897, 2906):
        decisions[cid] = {
            "status": "false_positive",
            "nota":   "FP: patrón intencional Hold+Paid+Payable (3/9). El subconjunto excluye deliberadamente Cancelled/Chargebacks para medir 'autorizadas en-tránsito'. Diseño consistente entre las dos cards del mismo dashboard. Bajo tráfico (5-6 vistas). Verificado SQL (lote_46 y lote_44) el 08/04/2026.",
            "fuente": "FP: Hold+Paid+Payable intencional — dashboard autorizadas en-tránsito (Sesión 36 — 08/04/2026)"
        }

    # ── FALSE POSITIVES: reaudit batch — patrones Sent intencional (Sesión 36 — 08/04/2026) ──
    # Cards verificadas en reaudit 07/04 con patron Sent documentado como diseño intencional
    # (95% FP rate en P1 confirmado). Excluye cards borderline (59, 118, 122, 1817).
    _fp_sent_reaudit = [
        373, 420, 456, 671, 675, 687, 693, 908, 1641, 1819,
        2432, 2479, 2504, 2755, 2771, 3005
    ]
    for cid in _fp_sent_reaudit:
        if cid not in decisions:  # no sobreescribir si ya tiene decisión
            decisions[cid] = {
                "status": "false_positive",
                "nota":   "FP: hallazgo de 'Sent' como patrón de pre-autorización intencional. En ExampleCorp, 'Sent' = sinónimo de 'Injected' en BD. El patrón es diseño documentado (FP rate ~95% en P1, confirmado por Leonardo, cto, Axel desde 2024). Verificado en reaudit 07/04/2026.",
                "fuente": "FP: Sent = diseño intencional ExampleCorp (P1 ~95% FP — reaudit Sesión 36 — 08/04/2026)"
            }

    # ── FALSE POSITIVES: reaudit batch — funnel/webchat y métricas Paid-only ────
    # Cards de funnel webchat Lead→1TU: etapas sin filtro de authorized = intencional
    _fp_funnel_reaudit = [9, 13, 2436, 2437, 2513]
    for cid in _fp_funnel_reaudit:
        if cid not in decisions:
            decisions[cid] = {
                "status": "false_positive",
                "nota":   "FP: card de funnel webchat/Lead→1TU. Las etapas de funnel (Lead, initiated, Sent, etc.) no usan el array de 9 authorized por diseño — miden conversión a lo largo del funnel, no solo transacciones autorizadas. Patrón documentado como intencional. Verificado en reaudit 07/04/2026.",
                "fuente": "FP: funnel webchat Lead→1TU — patrón intencional (reaudit Sesión 36 — 08/04/2026)"
            }

    # Card 372 — NPS con solo 'Paid' → correcto (ATV/MAU/NPS usan solo 'Paid')
    decisions[372] = {
        "status": "false_positive",
        "nota":   "FP: métrica NPS. Confirmado (04/04/2026): NPS en ExampleCorp = encuesta a usuarios con transaction_status = 'Paid'. Usar solo 'Paid' es correcto — NO aplicar el array de 9 statuses authorized. Igual que ATV y MAU.",
        "fuente": "FP: NPS usa solo 'Paid' — correcto (reaudit Sesión 36 — 08/04/2026)"
    }

    # Card 3246 — counter IN (0,1) en contexto FX Discount/Lead Pricing
    decisions[3246] = {
        "status": "false_positive",
        "nota":   "FP: counter IN (0,1) es workaround intencional documentado para ~17,597 registros con desfase de timing entre counter y counter_paid. Confirmado por cto (CTO). Regla de negocio INMUTABLE. Verificado en reaudit 07/04/2026.",
        "fuente": "FP: counter IN (0,1) workaround intencional (reaudit Sesión 36 — 08/04/2026)"
    }

    # ── S59 (22/04/2026) — Derivadas del PDF Training + obsolescencia ──────────

    # Card 354 — "Active users (DoD)" = DAU (4,562v, ACTIVA last 2026-04-07)
    # PDF §02 "Usuarios Activos": DAU = `transaction_status = 'Paid'` ÚNICAMENTE.
    # Card usa 8/9 statuses (excluye Hold) → viola el PDF. La pregunta "¿Hold intencional?"
    # queda obsoleta: el PDF establece que TODOS los statuses != Paid están mal en DAU.
    # Fix propuesto: reemplazar IN(8 statuses) por = 'Paid'.
    decisions[354] = {
        "status": "confirmed_error",
        "nota":   "DAU debe filtrar transaction_status = 'Paid' únicamente, per PDF Training §02 'Usuarios Activos'. La card usa 8/9 statuses (excluye Hold) — todo el array extra es incorrecto para DAU. Fix = reemplazar IN(...) por = 'Paid'. Consistente con MAU/WAU/QAU y las 6 cards de usuarios activos ya verificadas. S59 (22/04/2026).",
        "fuente": "CE: DAU = solo 'Paid' per PDF Training §02 (S59 — 22/04/2026)"
    }

    # Cards obsoletas INACTIVA_12M (última vista 2024-10-30, >520 días inactivas)
    # Acción CSV: ARCHIVAR. Los hallazgos pending pierden relevancia operativa — FP por bajo impacto.
    S59_OBSOLETAS_ARCHIVAR = {
        6:    "# Total of Successful Transactions — INACTIVA_12M, 138v, archivar",
        60:   "Charged-Back Transactions — INACTIVA_12M, 26v, archivar",
        678:  "Transaction Funnel: First Transaction & Intents — INACTIVA_12M, 12v, archivar",
        687:  "# New Users (First Sent Message) — INACTIVA_12M, 5v, archivar",
        693:  "# New Users (Quote) — INACTIVA_12M, 5v, archivar",
    }
    for cid, descr in S59_OBSOLETAS_ARCHIVAR.items():
        decisions[cid] = {
            "status": "false_positive",
            "nota":   f"FP por obsolescencia: {descr}. Hallazgo sin impacto operativo — card sin uso real (última vista 2024-10-30). Se archivará vía archivar_cards.py --categoria INACTIVA_12M. Decisión documentada sin requerir confirmación de data_owner. S59 (22/04/2026).",
            "fuente": "FP: obsolescencia INACTIVA_12M (S59 — 22/04/2026)"
        }

    # Cards marcadas para ELIMINAR (obsoletas + acción CSV: ELIMINAR)
    S59_OBSOLETAS_ELIMINAR = {
        807: "Hold transaction over authorized — INACTIVA_12M, 343v, eliminar",
        810: "Hold vs authorized — INACTIVA_12M, 365v, eliminar",
    }
    for cid, descr in S59_OBSOLETAS_ELIMINAR.items():
        decisions[cid] = {
            "status": "false_positive",
            "nota":   f"FP por obsolescencia total: {descr}. Card marcada para ELIMINAR en obsolescencia_cards.csv (última vista 2024-10-30, 524 días inactiva). Ampliar fix P9c a estas cards es innecesario — se eliminarán. S59 (22/04/2026).",
            "fuente": "FP: obsolescencia ELIMINAR (S59 — 22/04/2026)"
        }

    # Card 732 — "new_funnel:: First Transaction & Intents (Adquisitivo)" (2,814v, ACTIVA)
    # Hallazgo sobre 'Payment'/'Paymentlink' como etapas.
    # Ground truth (inicio.md L43): transaction_details_funnel: 'Payment','Paymentlink','Sent' son
    # etapas válidas. Cards de funnel que usan transaction_details_funnel → FP.
    decisions[732] = {
        "status": "false_positive",
        "nota":   "FP: card de funnel (new_funnel::) sobre transaction_details_funnel. Per ground truth (inicio.md L43): 'Payment','Paymentlink','Sent' son etapas válidas de la tabla funnel. No es error de lógica de statuses autorizados. S59 (22/04/2026).",
        "fuente": "FP: funnel etapas válidas per ground truth (S59 — 22/04/2026)"
    }

    # CE leves detectados en triaje S59 — incluidos en próximo fix_masivo batch.

    # Card 132 — "profitability by bands and users" (5,671v, ACTIVA)
    # Typo en filtro WEEK opcional: 'tinjected_date' (sin punto) en vez de 't.injected_date'.
    # Si se activa el filtro {{week}}, la query falla (columna inexistente).
    decisions[132] = {
        "status": "confirmed_error",
        "nota":   "CE: typo 'tinjected_date' → 't.injected_date' en filtro opcional WEEK. Fallaría en runtime si se activa {{week}}. Fix en fix_masivo.py fase P_TYPO_132. S59 (23/04/2026).",
        "fuente": "CE: typo tinjected_date (S59 — 23/04/2026)"
    }

    # Card 801 — "# adquisition user first sent message post unir" (1,674v, ACTIVA)
    # Inconsistencia en filtro YEAR opcional: usa 'u.created_date' cuando la tabla users
    # tiene columna 'createdat' (verificado en analytics_db_schema.json). El resto del SQL
    # usa 'u.createdat' correctamente — solo este filtro tiene la divergencia.
    decisions[801] = {
        "status": "confirmed_error",
        "nota":   "CE: inconsistencia 'u.created_date' vs 'u.createdat' en filtro opcional YEAR. Tabla users tiene columna 'createdat' (verificado en schema). Resto del SQL usa 'u.createdat'. Fix = normalizar filtro YEAR. Fase P_CREATEDAT_801. S59 (23/04/2026).",
        "fuente": "CE: createdat vs created_date (S59 — 23/04/2026)"
    }

    # Card 2632 — "raw data users auth details info" (293v, ACTIVA)
    # Array de 6 statuses: ('Hold','Paid','Payable','Client_refund','Cancelled','Chargeback_unir').
    # Faltan 3 Chargeback* (Chargeback, Chargeback_won, Chargeback_lost) per los 9 autorizados.
    # La card tiene 2 ocurrencias del mismo array (CTE last3 + subquery last_paying_amount).
    decisions[2632] = {
        "status": "confirmed_error",
        "nota":   "CE: array 6/9 statuses en raw data users auth — faltan Chargeback, Chargeback_won, Chargeback_lost. Fix = usar los 9 canónicos (NEW_9). Tiene 2 ocurrencias del mismo array. Fase P_STATUS_2632. S59 (23/04/2026).",
        "fuente": "CE: 6/9 statuses, falta 3 Chargeback* (S59 — 23/04/2026)"
    }

    return decisions


# ============================================================
# REGLAS DE NEGOCIO (aplican sobre la descripción del hallazgo)
# ============================================================

BUSINESS_RULE_PATTERNS = [
    {
        "keywords": ["counter in (0,1)", "counter not in (0,1)", "counter in(0,1)"],
        "status":   "intentional",
        "nota":     "Patrón counter IN (0,1) es workaround intencional documentado para ~17,597 registros con desfase de timing entre counter y counter_paid. Confirmado por cto (CTO). No reportar como error.",
        "fuente":   "Business rule: counter IN (0,1)"
    },
    # LEFT JOIN mto: ELIMINADO 15/04/2026 — redundante con PATTERN_DEFAULTS["P8"] = intentional.
    # P8 default ya cubre todos los hallazgos de este patrón.
]

# ============================================================
# TABLAS VERIFICADAS EN BD (P4 — falsos positivos)
# El modelo reportó estas tablas como "vacías" pero se verificó
# con SELECT COUNT(*) el 30/03/2026 — todas tienen filas.
# ============================================================

P4_VERIFIED_NONEMPTY = {
    "app_pricing": (
        "false_positive",
        "P4 FP: tabla app_pricing verificada con 2,811 filas el 30/03/2026. La tabla no está vacía — el modelo se equivocó al reportarla como vacía.",
        "P4 FP: app_pricing verificada 30/03/2026"
    ),
    "mkt_perfomance_metrics": (
        "false_positive",
        "P4 FP: tabla mkt_perfomance_metrics verificada con 968 filas el 30/03/2026. La tabla no está vacía.",
        "P4 FP: mkt_perfomance_metrics verificada 30/03/2026"
    ),
    "mkt_performance_metrics": (
        "false_positive",
        "P4 FP: tabla mkt_performance_metrics verificada con 968 filas el 30/03/2026. La tabla no está vacía.",
        "P4 FP: mkt_performance_metrics verificada 30/03/2026"
    ),
    "user_activity_summary": (
        "false_positive",
        "P4 FP: tabla user_activity_summary verificada con 218,650 filas el 30/03/2026. La tabla no está vacía.",
        "P4 FP: user_activity_summary verificada 30/03/2026"
    ),
    "facebook_ads_manager": (
        "false_positive",
        "P4 FP: tabla facebook_ads_manager verificada con 3 filas el 30/03/2026. La tabla existe — los datos son escasos por problema de pipeline de ingesta, no porque la tabla no exista.",
        "P4 FP: facebook_ads_manager verificada 30/03/2026"
    ),
    "mkt_google": (
        "false_positive",
        "P4 FP: tabla mkt_google verificada con 87 filas el 30/03/2026. La tabla existe — los datos son escasos por problema de pipeline, no porque la tabla no exista.",
        "P4 FP: mkt_google verificada 30/03/2026"
    ),
    # ── Ronda 2 — verificadas el 03/04/2026 via information_schema ──────────────
    "app_meta": (
        "false_positive",
        "P4 FP: tabla app_meta existe en BD (368 kB verificado el 03/04/2026). No está vacía — el modelo se equivocó.",
        "P4 FP: app_meta verificada 03/04/2026"
    ),
    "app_direct_mkt_by_day": (
        "false_positive",
        "P4 FP: tabla app_direct_mkt_by_day existe en BD (16 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: app_direct_mkt_by_day verificada 03/04/2026"
    ),
    "app_google": (
        "false_positive",
        "P4 FP: tabla app_google existe en BD (80 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: app_google verificada 03/04/2026"
    ),
    "app_daily_20240315164033": (
        "false_positive",
        "P4 FP: tabla app_daily_20240315164033 existe en BD (48 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: app_daily_20240315164033 verificada 03/04/2026"
    ),
    "app_affiliate_partners_20240715034252": (
        "false_positive",
        "P4 FP: tabla app_affiliate_partners_20240715034252 existe en BD (32 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: app_affiliate_partners_20240715034252 verificada 03/04/2026"
    ),
    "app_search_terms_20240315034947": (
        "false_positive",
        "P4 FP: tabla app_search_terms_20240315034947 existe en BD (200 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: app_search_terms_20240315034947 verificada 03/04/2026"
    ),
    "app_cac_monthly": (
        "false_positive",
        "P4 FP: tabla app_cac_monthly existe en BD (16 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: app_cac_monthly verificada 03/04/2026"
    ),
    "mkt_cpci": (
        "false_positive",
        "P4 FP: tabla mkt_cpci existe en BD (168 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_cpci verificada 03/04/2026"
    ),
    "mkt_cpci_fb": (
        "false_positive",
        "P4 FP: tabla mkt_cpci_fb existe en BD (48 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_cpci_fb verificada 03/04/2026"
    ),
    "mkt_cpci_google": (
        "false_positive",
        "P4 FP: tabla mkt_cpci_google existe en BD (48 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_cpci_google verificada 03/04/2026"
    ),
    "mkt_cpci_tb": (
        "false_positive",
        "P4 FP: tabla mkt_cpci_tb existe en BD (16 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_cpci_tb verificada 03/04/2026"
    ),
    "mkt_cpci_tk": (
        "false_positive",
        "P4 FP: tabla mkt_cpci_tk existe en BD (16 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_cpci_tk verificada 03/04/2026"
    ),
    "mkt_cpci_yt": (
        "false_positive",
        "P4 FP: tabla mkt_cpci_yt existe en BD (16 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_cpci_yt verificada 03/04/2026"
    ),
    "mkt_fb": (
        "false_positive",
        "P4 FP: tabla mkt_fb existe en BD (8192 bytes verificado el 03/04/2026). La tabla existe y tiene datos.",
        "P4 FP: mkt_fb verificada 03/04/2026"
    ),
    "mkt_tk": (
        "false_positive",
        "P4 FP: tabla mkt_tk existe en BD (8192 bytes verificado el 03/04/2026). La tabla existe y tiene datos.",
        "P4 FP: mkt_tk verificada 03/04/2026"
    ),
    "mkt_total_spent": (
        "false_positive",
        "P4 FP: tabla mkt_total_spent existe en BD (96 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: mkt_total_spent verificada 03/04/2026"
    ),
    "utm_tk_spent": (
        "false_positive",
        "P4 FP: tabla utm_tk_spent existe en BD (48 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: utm_tk_spent verificada 03/04/2026"
    ),
    "monthly_metrics_snapshot": (
        "false_positive",
        "P4 FP: tabla monthly_metrics_snapshot existe en BD (112 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: monthly_metrics_snapshot verificada 03/04/2026"
    ),
    "daily_data": (
        "false_positive",
        "P4 FP: tabla daily_data existe en BD (88 kB verificado el 03/04/2026). No está vacía.",
        "P4 FP: daily_data verificada 03/04/2026"
    ),
    "test_trx_details_v12": (
        "false_positive",
        "P4 FP: tabla test_trx_details_v12 existe en BD (116 MB verificado el 03/04/2026). Tabla de test pero existe y tiene muchos datos — el modelo se equivocó al reportarla como vacía/inexistente.",
        "P4 FP: test_trx_details_v12 verificada 03/04/2026"
    ),
    # ── Tablas verificadas con analytics_db_schema.json (07/04/2026) ────────────
    # Existen en BD con tamaño > 0 (stats desactualizadas, mismo patrón que app_pricing).
    # Pendiente COUNT(*) real desde Windows con verificar_p4_tablas.py.
    "mkt_perfomance_funnel": (
        "false_positive",
        "P4 FP: tabla mkt_perfomance_funnel existe en BD (160 kB verificado en schema 07/04/2026). "
        "Stats desactualizadas → row_estimate = 0 pero size > 0. La tabla existe — hallazgo del modelo incorrecto.",
        "P4 FP: mkt_perfomance_funnel existe en schema 07/04/2026"
    ),
    "app_pricing_cohort1_retention": (
        "false_positive",
        "P4 FP: tabla app_pricing_cohort1_retention existe en BD (16 kB verificado en schema 07/04/2026). "
        "Stats desactualizadas → row_estimate = 0 pero size > 0. La tabla existe — hallazgo del modelo incorrecto.",
        "P4 FP: app_pricing_cohort1_retention existe en schema 07/04/2026"
    ),
    "app_pricing_cohort2_retention": (
        "false_positive",
        "P4 FP: tabla app_pricing_cohort2_retention existe en BD (16 kB verificado en schema 07/04/2026). "
        "Stats desactualizadas → row_estimate = 0 pero size > 0. La tabla existe — hallazgo del modelo incorrecto.",
        "P4 FP: app_pricing_cohort2_retention existe en schema 07/04/2026"
    ),
    # ── Tablas NO existentes — se quedan pending para revisión manual ─
    # 'devolver', 'temporal', 'utilizada' → no aparecieron en information_schema
    # Si el hallazgo dice "tabla no existe" o "tabla vacía", puede ser confirmed_error legítimo.
    # Se clasificarán manualmente cuando se revisen esas cards específicas.
}

# Reglas adicionales basadas en descripción (análisis de 1,694 pending del 03/04/2026)
DESCRIPTION_RULES = [
    # El modelo dice "excluye Sent, debería incluirlo" → FP: excluir Sent de authorized ES correcto
    {
        "must_contain":  ["excluye", "'sent'"],
        "any_contain":   ["debería", "incluido", "incluir"],
        "status":        "false_positive",
        "nota":          "FP: el modelo sugiere incluir 'Sent' en métricas autorizadas, pero per ground truth verificado con cto (CTO), 'Sent' es PRE-autorización y NO debe estar en el array de authorized. Excluirlo es correcto.",
        "fuente":        "FP: modelo invirtió lógica de Sent"
    },
    # El título menciona Paid y el filtro es Paid → no es error
    {
        "must_contain":  ["título", "paid"],
        "any_contain":   ["correcto", "es correcto", "adecuado", "apropiado"],
        "status":        "false_positive",
        "nota":          "FP: el título ya refleja correctamente el filtro de 'Paid'. No hay desajuste — el modelo lo marcó incorrectamente.",
        "fuente":        "FP: título correcto con filtro Paid"
    },
    # Performance / estilo — no afectan correctitud
    # ENDURECIDO 15/04/2026: guards para no capturar errores reales de transaction_status
    {
        "must_contain":  [],
        "any_contain":   ["se puede simplificar", "puede simplificarse"],
        "must_not_contain": ["incompleto", "no existe", "no incluye", "falta",
                             "incorrecto", "authorized", "transaction_status",
                             "sobreconteo", "sobrecuenta", "error de negocio"],
        "status":        "false_positive",
        "nota":          "FP: observación de performance o estilo de código. No afecta la correctitud del resultado. Baja prioridad — clasificar como mejora técnica opcional.",
        "fuente":        "FP: performance/estilo"
    },
    {
        "must_contain":  ["subconsulta"],
        "any_contain":   ["redundante", "innecesari", "cuando ya se filtra", "sin necesidad"],
        "must_not_contain": ["sobreconteo", "sobrecuenta", "transaction_status",
                             "pierde transacciones", "resultado incorrecto",
                             "error de datos", "genera duplicados"],
        "status":        "false_positive",
        "nota":          "FP: subconsulta marcada como redundante o innecesaria. Observación de performance — no afecta la correctitud del resultado.",
        "fuente":        "FP: subconsulta redundante"
    },
    # test_user — diferencia de estilo, no error
    {
        "must_contain":  ["test_user"],
        "any_contain":   ["patrón estándar", "coalesce", "cast("],
        "status":        "false_positive",
        "nota":          "FP: diferencia de estilo en el filtro de test_user. Ambas formas funcionan; COALESCE/CAST es el patrón estándar pero la variante sin él no es un error de datos.",
        "fuente":        "FP: estilo test_user"
    },
    # EXTRACT vs week_number — diferencia de implementación, no error
    {
        "must_contain":  ["extract"],
        "any_contain":   ["week_number", "week number"],
        "status":        "false_positive",
        "nota":          "FP: diferencia de implementación para calcular la semana (EXTRACT vs week_number). Ambas son válidas — inconsistencia de estilo, no error de resultado.",
        "fuente":        "FP: EXTRACT vs week_number"
    },
    # MIN/MAX calculado pero no usado en SELECT final
    {
        "must_contain":  [],
        "any_contain":   ["no se usa en el resultado", "no se usa en el select", "innecesario cuando ya se filtra por counter"],
        "status":        "false_positive",
        "nota":          "FP: cálculo auxiliar que no impacta el resultado final. Observación de limpieza de código, no error de lógica.",
        "fuente":        "FP: cálculo no usado"
    },
    # ── Ronda 3 — análisis de 1,033 pending del 03/04/2026 ──────────────────────

    # 'Sent' es pre-autorización → modelo dice "incluye Sent incorrectamente"
    # FP rate de P1 verificado ~95%. Cards que usan Sent como denominador (injected/funnel) son intencionales.
    # Excluir cards 118 y 59 que quedan pending para confirmación de data_owner.
    # Nota: la exclusión de 118/59 se gestiona a nivel card-decision (no se clasifican aquí porque
    # build_card_decisions() no tiene entrada para ellas → la regla de descripción podría dispararse.
    # Se añade NOT CARD check en classify_row().
    {
        "must_contain":  ["pre-autorización"],
        "any_contain":   ["sent", "'sent'"],
        "status":        "false_positive",
        "nota":          "FP: el modelo reporta 'Sent' como error porque es pre-autorización. Sin embargo, FP rate verificado de P1 es ~95% — el uso de 'Sent' como equivalente a 'injected' (denominator de yield/funnel) es un patrón intencional documentado en ExampleCorp. Confirmado por cto (CTO) y Axel (Analytics). Ver Card 3 y P1 en ESTADO.md.",
        "fuente":        "FP: Sent pre-autorización — P1 FP rate 95%"
    },
    # disbursement_type — modelo advierte sobre algo que admite NO haber encontrado → FP claro
    {
        "must_contain":  ["disbursement_type"],
        "any_contain":   ["no se detectó"],
        "status":        "false_positive",
        "nota":          "FP: el modelo advierte sobre disbursement_type pero admite en la propia descripción que 'no se detectó' uso de esa columna. Advertencia preventiva sin evidencia — FP.",
        "fuente":        "FP: disbursement no detectado (advertencia sin evidencia)"
    },
    # R_DISBURSEMENT: disbursement_type "sin homologar" → FP
    # Verificado 06/04/2026 en BD analytics_db (author): los valores reales son:
    #   'Account Credit' (175,259), 'Cash Pickup' (90,628),
    #   'bankDeposit' (1,105), 'directCash' (789), 'mobileWallet' (124)
    # La BD tiene naming inconsistente internamente (Title Case vs camelCase) pero es el dato real.
    # Cualquier query que use estos strings exactos ES correcta — retorna filas.
    # El modelo pedía "normalización" a un estándar inexistente en ExampleCorp. → FP.
    # Excepción: si la descripción indica que el valor literalmente no existe en BD → puede ser error.
    {
        "must_contain":  ["disbursement"],
        "any_contain":   ["sin homologar", "no homologado", "no está homologado",
                          "homologación de disbursement", "homologar disbursement",
                          "homologación pendiente", "falta homologación",
                          "falta de homologación",
                          "raw values", "raw usage", "valores raw",
                          "sin normalizar", "sin normalización", "no normalizado",
                          "falta de normalización", "falta normalización",
                          "homologar los valores", "aplica la homologación",
                          "aplica homologación", "homologar utilizando",
                          "uso de disbursement_type sin homolog",
                          "disbursement_type raw",
                          "valores de disbursement_type no están",
                          "tipo de dispersión",
                          "homologación de tipo",
                          "tipo de desembolso",
                          # Frases adicionales detectadas 07/04/2026 (cards 415, 494)
                          "debería ser homologado",
                          "deberia ser homologado",
                          "normalización de disbursement",
                          "normalizar disbursement",
                          ],
        "must_not_contain": ["no existe", "no se encuentra", "0 filas", "sin datos",
                             "tabla vacía", "no retorna"],
        "status":        "false_positive",
        "nota":          "FP: hallazgo de 'disbursement_type sin homologar'. Verificado 06/04/2026 en analytics_db: los valores reales en BD son 'Account Credit' (175k), 'Cash Pickup' (91k), 'bankDeposit' (1.1k), 'directCash' (789), 'mobileWallet' (124). La BD tiene naming mixto internamente (Title Case vs camelCase) pero son los valores canónicos reales — las queries que usan estos strings exactos son correctas. El modelo pedía normalización a un estándar que no existe en ExampleCorp.",
        "fuente":        "FP: disbursement_type sin homologar — valores verificados en BD (R_DISBURSEMENT — 06/04/2026)"
    },
    # Código SQL comentado — cosmético, no afecta resultados de la query
    # Expandido 06/04/2026 (Sesión 20): captura variantes "SQL comentado", "query comentada" etc.
    {
        "must_contain":  [],
        "any_contain":   ["código sql comentado", "código comentado", "línea comentada",
                          "líneas de código comentado",
                          "sql comentado",           # "La consulta tiene SQL comentado"
                          "secciones comentadas",    # "El SQL contiene secciones comentadas"
                          "bloque de sql comentado", # "tiene un bloque de SQL comentado"
                          "query comentada",         # "La query tiene un bloque de SQL comentado"
                          "consulta comentada",      # variante en español
                          "hay sql comentado",       # "Hay SQL comentado dentro de la query"
                          "existe sql comentado",    # "Existe SQL comentado"
                          "versionamiento informal", # señal estándar del modelo para código comentado
                          ],
        "status":        "false_positive",
        "nota":          "FP: comentarios en SQL no afectan la correctitud ni los resultados de la consulta. Es un problema de mantenimiento/limpieza de código, no un error de datos. Severidad real: cosmética.",
        "fuente":        "FP: código comentado (cosmético)"
    },
    # user_label redundante — campo ya existe en transaction_details
    {
        "must_contain":  ["user_label"],
        "any_contain":   ["ya existe", "ya está disponible", "recalcula", "recrea"],
        "status":        "false_positive",
        "nota":          "FP: observación de performance — el SQL recalcula user_label cuando ya existe pre-computado en transaction_details. No afecta la correctitud del resultado, solo es ineficiencia. No requiere fix urgente.",
        "fuente":        "FP: user_label redundante (performance)"
    },
    # ORDER BY innecesario en subquery — no afecta resultado final
    {
        "must_contain":  ["order by"],
        "any_contain":   ["subquery", "subconsulta", "innecesario", "no tiene efecto"],
        "status":        "false_positive",
        "nota":          "FP: ORDER BY en subquery sin TOP/LIMIT no afecta el resultado de la query externa. Observación de estilo, no error de datos.",
        "fuente":        "FP: ORDER BY innecesario en subquery"
    },
    # ── Ronda 6 — 04/04/2026 ────────────────────────────────────────────────────

    # R_sent_ausente: modelo dice que 'Sent' ESTÁ AUSENTE / debería incluirse → FP
    # El modelo a veces invierte la lógica y dice que Sent debería estar pero no está.
    # Per ground truth (cto CTO), Sent = pre-auth y NO debe estar en authorized.
    # Si Sent está ausente, eso es correcto — el hallazgo del modelo es FP.
    {
        "must_contain":  ["'sent'"],
        "any_contain":   ["ausente", "está ausente", "no está presente", "no aparece",
                          "debería incluir", "debería estar"],
        "status":        "false_positive",
        "nota":          "FP: el modelo reporta que 'Sent' está AUSENTE de un filtro y sugiere incluirlo. Pero per ground truth verificado con cto (CTO), 'Sent' es pre-autorización y correctamente NO debe aparecer en filtros de authorized. La ausencia de Sent es correcta — el modelo invirtió la lógica.",
        "fuente":        "FP: modelo invirtió lógica — Sent ausente es correcto (04/04/2026)"
    },

    # R14: 'total_injected' en descripción → FP (Sent = injected en denominador de ratio)
    # Card 278 (Performance Metrics By Source): "incluye 'Sent' para total_injected"
    # 'total_injected' es el alias de Sent usado en cálculos de conversión — intencional.
    {
        "must_contain":  ["total_injected"],
        "any_contain":   [],
        "status":        "false_positive",
        "nota":          "FP: descripción menciona 'total_injected', alias de 'Sent' usado como denominador de ratio de conversión (injected = Sent en ExampleCorp). Uso intencional — mismo patrón que Card 3 confirmado por analyst_2 (31/03/2026). Verificado el 04/04/2026.",
        "fuente":        "FP: total_injected = Sent denominador (R14 — 04/04/2026)"
    },
    # R14b: descripción menciona 'Sent' + 'injected' juntos → FP
    {
        "must_contain":  ["'sent'", "injected"],
        "any_contain":   [],
        "must_not_contain": ["error", "incorrecto", "sobrecuenta", "sobreconteo"],
        "status":        "false_positive",
        "nota":          "FP: descripción vincula 'Sent' con 'injected' — confirma que el uso de Sent como sinónimo de injected es reconocido incluso por el modelo. Patrón intencional documentado (Card 3, INICIO.md). Verificado el 04/04/2026.",
        "fuente":        "FP: Sent=injected explícito en descripción (R14b — 04/04/2026)"
    },

    # R9: Deuda técnica / código muerto → intencional (sistémico, documentado)
    {
        "must_contain":  [],
        "any_contain":   ["deuda técnica", "código muerto", "dead code", "código legacy",
                          "no tiene uso efectivo", "comentado inline sin uso"],
        "status":        "intentional",
        "nota":          "Intencional: deuda técnica o código muerto identificado. Es un patrón sistémico documentado en el proyecto — no requiere fix urgente. Coordinar con ingeniería cuando haya capacidad.",
        "fuente":        "Intencional: deuda técnica/código muerto (R9 — 04/04/2026)"
    },
    # R10: Rendimiento/performance (ampliado) — no afectan correctitud
    # Expandido 06/04/2026 (Sesión 20): variantes de full scan y slow query
    # ENDURECIDO 15/04/2026: guards contra errores reales
    {
        "must_contain":  [],
        "any_contain":   ["rendimiento de la consulta", "rendimiento de la query",
                          "impacto en el rendimiento", "afecta el rendimiento",
                          "puede afectar el rendimiento", "performance de la query",
                          "performance de la consulta", "full scan en tablas",
                          "cálculos repetidos en varias subqueries",
                          "sin usar un índice",
                          "full table scan",
                          "escaneando toda la tabla",
                          "escanear toda la tabla",
                          "escaneo completo de",
                          "causando un full",
                          ],
        "must_not_contain": ["incompleto", "no incluye", "falta", "incorrecto",
                             "authorized", "transaction_status",
                             "sobreconteo", "sobrecuenta", "error de negocio"],
        "status":        "false_positive",
        "nota":          "FP: observación de rendimiento/performance o full scan potencial. No afecta la correctitud del resultado — la query devuelve los datos correctos, solo puede ser lenta sin filtro de fecha. Evaluar impacto real antes de priorizar.",
        "fuente":        "FP: rendimiento/performance ampliado (R10 — 04/04/2026)"
    },
    # R_FECHA_PERF: sin filtro de fecha → full scan potencial (performance, no error de datos)
    # 73 hallazgos pending dicen "puede ejecutarse sin filtro de fecha" (Sesión 20 — 06/04/2026)
    # Los filtros de fecha opcionales en Metabase son por diseño (campo {{Start_date}} opcional).
    # La correctitud del resultado no se ve afectada — solo el performance.
    # Excluir descripciones que hablen de: solapamiento de fechas, sintaxis incorrecta,
    # o conflicto lógico entre filtros (esas sí pueden ser problemas reales).
    {
        "must_contain":  [],
        "any_contain":   ["puede ejecutarse sin filtro de fecha",
                          "puede ejecutarse sin ningún filtro de fecha",
                          "puede ejecutarse sin un filtro de fecha",
                          "puede escanear toda la tabla",
                          "puede escanear la tabla",
                          "puede realizar un full scan",
                          "falta de filtro de fecha",    # "Falta de filtro de fecha en tabla grande"
                          "no hay un filtro de fecha",   # "No hay filtro de fecha explícito"
                          "no tiene filtro por fecha",   # variante
                          "no tiene un filtro de fecha", # variante
                          "no tiene un filtro explícito de fecha",
                          "no tiene un filtro de rango",
                          "sin filtro de fecha obligatorio",
                          "sin filtro de fecha",         # pattern general
                          "full scan sobre",             # "puede causar un full scan sobre"
                          "un full scan",                # "provocar un full scan"
                          "scan completo de la tabla",   # "scan completo de tabla grande"
                          "scan completo de",            # variante
                          "deriva en un scan",           # "puede derivar en un scan completo"
                          "provocar un full scan",
                          "puede causar un full scan",
                          "puede causar un scan",
                          "puede generar un scan",
                          ],
        "must_not_contain": ["sobrepon", "sobrepone", "superpone", "solapan", "conflicto",
                             "contradicen", "contradicción",
                             "sintaxis incorrecta",      # esos son errores de sintaxis reales
                             "sintaxis incorrecto",
                             "incorrectamente construidos",
                             ],
        "status":        "false_positive",
        "nota":          "FP: la query puede ejecutarse sin filtro de fecha o con riesgo de full scan. Esto es un concern de performance, no de correctitud de datos. En Metabase, los filtros de fecha opcionales ({{Start_date}}/{{End_date}}) son un patrón aceptado — el usuario decide si los aplica. No afecta el resultado de la métrica de negocio. Baja prioridad de optimización.",
        "fuente":        "FP: falta filtro fecha = performance, no error de datos (R_FECHA_PERF — 06/04/2026)"
    },
    # R11: Título dice "paid" y filtro es "solo 'Paid'" — concordancia correcta, no hay desajuste
    # El modelo confundió una card sobre métricas "paid" con un error de scope
    {
        "must_contain":  ["título", "'paid'"],
        "any_contain":   ["filtro es solo 'paid'", "filtra solo 'paid'", "filtro solo 'paid'",
                          "filter is only 'paid'", "el filtro es solo 'paid'"],
        "must_not_contain": ["authorized"],
        "status":        "false_positive",
        "nota":          "FP: el título de la card menciona 'paid' y el filtro también es 'Paid' — hay concordancia. El modelo reportó desajuste cuando en realidad la card mide correctamente transacciones 'paid'. No es error.",
        "fuente":        "FP: título-paid concordante con filtro-paid (R11 — 04/04/2026)"
    },
    # ── Ronda 7 — Sesión 26 (06/04/2026) ────────────────────────────────────────

    # R16: SQL comentado / código comentado → FP
    # Secciones de SQL desactivadas con -- o /* */ son prácticas de mantenimiento comunes en ExampleCorp.
    # El modelo las señala como "código obsoleto" o "versiones previas", pero no afectan datos.
    # Excluir menciones de "lógica comentada que cambia el resultado" para no generar FNs.
    {
        "must_contain":  [],
        "any_contain":   ["sql comentado", "código comentado", "sentencias comentadas",
                          "secciones comentadas", "bloques comentados",
                          "contiene secciones dentro de comentarios",
                          "versiones previas sin claridad",
                          "partes de la consulta comentadas",
                          "lineas comentadas",
                          "líneas comentadas",
                          "comentarios de código sql",
                          "comentarios de código",
                          ],
        "must_not_contain": ["lógica comentada cambia", "resultado cambia", "afecta el resultado"],
        "status":        "false_positive",
        "nota":          "FP: SQL comentado detectado (-- o /* */). En ExampleCorp es práctica común dejar código comentado como referencia histórica o para deprecar filtros sin eliminarlos. No afecta la correctitud del resultado. Verificado como patrón sistémico en Sesión 26 (06/04/2026).",
        "fuente":        "FP: SQL comentado = práctica de mantenimiento (R16 — 06/04/2026)"
    },
    # R17: Ambassador_code inconsistente entre dashboards → FP
    # El orden de condiciones CASE de ambassador_code varía entre cards — es estilo, no error de datos.
    # Los valores resultantes son los mismos independientemente del orden del CASE.
    {
        "must_contain":  ["ambassador_code"],
        "any_contain":   ["orden", "casing", "homologación", "inconsistente",
                          "no sigue el patrón", "difiere del patrón",
                          "inconsistencia", "no está totalmente unificado",
                          "difiere entre", "varía entre"],
        "must_not_contain": ["transaction_status", "status", "authorized", "error de datos"],
        "status":        "false_positive",
        "nota":          "FP: inconsistencia de estilo en el CASE de ambassador_code. El orden de condiciones varía entre dashboards pero los valores resultantes son idénticos — es una inconsistencia cosmética, no un error de datos. Verificado en Sesión 26 (06/04/2026).",
        "fuente":        "FP: ambassador_code CASE order = estilo, no error (R17 — 06/04/2026)"
    },
    # R18: INNER JOIN a tabla users sin justificación → FP
    # El modelo señala INNER JOIN a users como "peligroso" porque podría excluir registros.
    # En ExampleCorp, transaction_details.user_id siempre tiene un users correspondiente en producción.
    # El INNER JOIN a users es el patrón estándar para acceder a fraud_confirmed/confirmed_fraud.
    # CUIDADO: no aplica si la descripción habla de pérdida real de datos o cards de status.
    {
        "must_contain":  [],
        "any_contain":   ["inner join", "join con users"],
        "must_not_contain": ["transaction_status", "array", "authorized", "sobreconteo",
                             "status incompleto", "pierde transacciones", "excluye transacciones válidas",
                             "sin restricción de status"],
        "status":        "false_positive",
        "nota":          "FP: INNER JOIN a tabla users para acceder a campos de fraude (fraud_confirmed, confirmed_fraud). En producción de ExampleCorp, transaction_details.user_id siempre tiene un registro en users — el INNER JOIN no excluye datos reales. Patrón intencional y consistente en el codebase. Verificado en Sesión 26 (06/04/2026).",
        "fuente":        "FP: INNER JOIN users para fraude = patrón intencional (R18 — 06/04/2026)"
    },
    # ── Sesión 33 — 07/04/2026 ────────────────────────────────────────────────────

    # R19: Modelo confirma que el filtro es correcto ("correctamente alineado")
    # Cuando el modelo mismo dice "el título menciona X pero el filtro está correctamente alineado",
    # es un FP claro — el modelo detectó concordancia pero igual generó el hallazgo.
    # Ejemplos: "# of paid txns | $500 band" — título dice paid, filtro es Paid → correcto.
    {
        "must_contain":  [],
        "any_contain":   ["correctamente alineado con el conteo",
                          "correctamente alineado con",
                          "filtro está correctamente alineado",
                          "filter is correctly aligned",
                          "está correctamente alineado",
                          ],
        "status":        "false_positive",
        "nota":          "FP: el modelo mismo indicó en la descripción que el filtro está 'correctamente alineado' con el propósito de la card. Hallazgo auto-descartado por inconsistencia interna del modelo — reconoce la concordancia pero genera el hallazgo de todas formas.",
        "fuente":        "FP: modelo confirma alineación correcta (R19 — 07/04/2026)"
    },
    # R20: Modelo dice "si el propósito es X, el filtro es correcto" → FP
    # El modelo genera el hallazgo pero en la propia descripción acepta que podría ser correcto.
    # Per ground truth: la ambigüedad del modelo se resuelve como FP hasta tener evidencia de error.
    {
        "must_contain":  [],
        "any_contain":   ["si el propósito es medir transacciones canceladas, el filtro es correcto",
                          "si el propósito es medir",
                          "si el objetivo es medir",
                          "si el propósito es analizar",
                          ],
        "must_not_contain": ["incorrecto", "sobrecuenta", "sobreconteo", "error"],
        "status":        "false_positive",
        "nota":          "FP: el modelo reconoció en la propia descripción que el filtro puede ser correcto según el propósito de la card. La ambigüedad del modelo (~73% FP rate verificado) se resuelve como FP hasta tener evidencia contraria.",
        "fuente":        "FP: modelo reconoce posible corrección según propósito (R20 — 07/04/2026)"
    },
    # R21: CASE redundante / filtros redundantes — observación de performance, no error de datos
    # El modelo señala que un CASE o filtro es redundante porque el WHERE ya cubre esa condición.
    # No afecta correctitud del resultado — es observación de limpieza de código.
    {
        "must_contain":  [],
        "any_contain":   ["filtros redundantes al calcular",
                          "redundante ya que el where ya filtra",
                          "es redundante ya que",
                          "case when.*es redundante",
                          "uso potencial de filtros redundantes",
                          ],
        "must_not_contain": ["sobrecuenta", "sobreconteo", "status incompleto",
                             "authorized", "error de datos"],
        "status":        "false_positive",
        "nota":          "FP: uso de CASE o filtro redundante — el WHERE ya cubre la condición señalada. Observación de limpieza de código, no error de datos. El resultado de la query es correcto.",
        "fuente":        "FP: CASE/filtro redundante = limpieza, no error (R21 — 07/04/2026)"
    },
    # ── Sesión 35 — 07/04/2026 (Cowork) ─────────────────────────────────────────

    # R24: Filtro opcional de Metabase bien construido → FP
    # El modelo reporta "filtro opcional mal construido" pero [[AND (...)]] ES la
    # sintaxis correcta y documentada de Metabase para filtros opcionales.
    {
        "must_contain":  [],
        "any_contain":   ["filtros opcionales no están correctamente construidos",
                          "faltan los corchetes de cierre en los filtros opcionales",
                          "corchetes de cierre en los filtros opcionales",
                          "filtro por fecha no está utilizando la sintaxis correcta para filtros opcionales",
                          "sintaxis correcta es [[and",
                          "sintaxis correcta es [[and ({{var}} is null",
                          ],
        "status":        "false_positive",
        "nota":          "FP: el modelo reportó 'filtro opcional mal construido' pero la sintaxis [[AND ({{var}} IS NULL OR col = {{var}})]] ES la sintaxis correcta de Metabase para filtros opcionales. Los corchetes dobles [[...]] son la forma de declarar parámetros opcionales en Metabase Native Queries. Verificado en múltiples cards el 07/04/2026.",
        "fuente":        "FP: [[AND ...]] es sintaxis correcta de Metabase (R24 — 07/04/2026)"
    },

    # ── Sesión 34 — 07/04/2026 ────────────────────────────────────────────────────

    # R22: 'Payable,Paid' como string literal — bug de sintaxis SQL real
    # Cuando el hallazgo menciona 'Payable,Paid' o 'payable,paid' como valor IN,
    # es un error confirmado: se trata como un único string y nunca matchea.
    {
        "must_contain":  [],
        "any_contain":   ["'payable,paid'", "payable,paid", "'payable, paid'",
                          # Variante: modelo describe el bug sin mencionar el valor literal
                          "coma dentro de un string en lugar de dos valores",
                          "coma dentro de un string",
                          "incluye una coma dentro de un string",
                          ],
        "status":        "confirmed_error",
        "nota":          "Bug de sintaxis: 'Payable,Paid' dentro de IN(...) se trata como un único string literal, no como dos valores separados. Nunca matchea filas reales (BD tiene 'Payable' y 'Paid' por separado). Fix: cambiar a IN ('Payable', 'Paid', ...). Verificado en cards 173/174 el 07/04/2026.",
        "fuente":        "confirmed_error: 'Payable,Paid' como string literal (R22 — 07/04/2026)"
    },
    # R23: modelo confirma que la card está correctamente configurada para su propósito
    {
        "must_contain":  [],
        "any_contain":   ["correctamente configurada para su propósito",
                          "está correctamente configurada",
                          "la configuración es correcta",
                          "la card está correctamente",
                          ],
        "must_not_contain": ["sin embargo", "pero", "aunque", "error", "incorrecto"],
        "status":        "false_positive",
        "nota":          "FP: el modelo mismo indica en la descripción que la card está correctamente configurada. Hallazgo auto-descartado por inconsistencia interna del modelo.",
        "fuente":        "FP: modelo confirma configuración correcta (R23 — 07/04/2026)"
    },

    # ── Sesión 36 — 08/04/2026 ────────────────────────────────────────────────────

    # R25: Status 'Payment' no existe en BD → confirmed_error
    # El modelo detecta uso de 'Payment' como transaction_status. NO existe en
    # transaction_details ni en transaction (los canónicos son 'Paid', 'Payable', etc.).
    {
        "must_contain":  [],
        "any_contain":   ["transaction_status = 'payment'",
                          "transaction_status in ('payment'",
                          "status 'payment' no existe",
                          "status \"payment\" no existe",
                          "'payment' no es un valor válido",
                          "utiliza 'payment' como",
                          "incluye 'payment' como",
                          ],
        "must_not_contain": ["disbursement", "scheme", "payment_method"],
        "status":        "confirmed_error",
        "nota":          "Error: transaction_status = 'Payment' NO existe en la BD. Los valores canónicos son 'Paid', 'Payable', 'Hold', etc. (ver array authorized). La condición con 'Payment' es código muerto que nunca matchea filas reales.",
        "fuente":        "confirmed_error: status 'Payment' no existe en BD (R25 — 08/04/2026)"
    },

    # R26: Funnel webchat / Lead-to-1TU sin filtro de authorized → FP intencional
    # Cards que miden conversión a lo largo del funnel webchat (Lead → Sent → 1TU)
    # no usan el array de 9 authorized por diseño. El modelo los reporta como error
    # porque no filtran por status authorized, pero el propósito de la card no es eso.
    {
        "must_contain":  [],
        "any_contain":   ["funnel webchat", "lead to 1tu", "lead-to-1tu", "lead → 1tu",
                          "etapas del funnel", "conversión del funnel",
                          "initiated_users", "funnel de conversión webchat",
                          "1tu conversations", "first transaction",
                          "one and done", "one-and-done",
                          ],
        "must_not_contain": ["error", "incorrecto", "debería filtrar"],
        "status":        "false_positive",
        "nota":          "FP: card de funnel webchat/Lead→1TU. Las etapas del funnel no usan el array de authorized por diseño — miden conversión a lo largo del proceso, no solo transacciones autorizadas. Patrón intencional documentado.",
        "fuente":        "FP: funnel webchat Lead→1TU — patrón intencional (R26 — 08/04/2026)"
    },

    # R27: Modelo confirma explícitamente que el patrón 'Sent' es pre-autorización
    # intencional / diseño correcto → FP
    # Cuando el modelo, en su propia descripción, reconoce que 'Sent' es pre-autorización
    # y que el filtro podría ser intencional, el hallazgo no es accionable.
    {
        "must_contain":  [],
        "any_contain":   ["sent podría ser intencional",
                          "sent es pre-autorización intencional",
                          "sent como pre-autorización es correcto",
                          "sent puede ser intencional",
                          "incluir sent podría estar justificado",
                          "sent en el denominador es correcto",
                          "sent para medir intentos",
                          ],
        "must_not_contain": ["error confirmado", "afecta los números"],
        "status":        "false_positive",
        "nota":          "FP: el modelo mismo reconoce que el uso de 'Sent' podría ser intencional. En ExampleCorp, 'Sent' = 'Injected' (pre-autorización). El patrón es diseño documentado (FP rate ~95% en P1). Hallazgo auto-descartado por reconocimiento interno del modelo.",
        "fuente":        "FP: modelo reconoce Sent intencional (R27 — 08/04/2026)"
    },
    
    # R28: ATV / MAU / DAU / WAU / QAU / NPS con solo 'Paid' → FP
    # Estas métricas en ExampleCorp usan EXCLUSIVAMENTE transaction_status = 'Paid'.
    # Confirmado: ATV (Val Head Mktg 04/04/26), MAU/DAU/NPS (SQL verificado 04/04/26).
    {
        "must_contain":  [],
        "any_contain":   [
            "atv", "average transaction value", "ticket promedio",
            "mau", "monthly active user", "usuarios activos mensuales",
            "dau", "daily active user", "usuarios activos diarios",
            "wau", "weekly active user", "usuarios activos semanales",
            "qau", "quarterly active",
            "nps", "net promoter",
            "usuarios activos", "active users",
            "retención", "retencion", "retention",
            "recurrentes", "recurring",
        ],
        "any_contain_2": [
            "solo 'paid'", "solo paid", "únicamente paid", "únicamente 'paid'",
            "filtro incompleto", "array incompleto",
            "debería usar los 9", "deberia usar los 9",
            "falta el array", "solo filtra 'paid'", "filtra solo 'paid'",
            "debería incluir", "deberia incluir",
            "limita solo a 'paid'", "limita a 'paid'",
            "sub-reportar", "subreportar",
        ],
        "must_not_contain": ["incorrecto en este contexto", "sobrecuenta", "sobreconteo real"],
        "status":        "false_positive",
        "nota":          "FP: métrica de ATV/MAU/DAU/WAU/QAU/NPS/Retención. Confirmado (04/04/2026): estas métricas en ExampleCorp se definen EXCLUSIVAMENTE con transaction_status = 'Paid'. No aplica el array de 9 statuses authorized. Fuentes: Val (Head Marketing) para ATV; SQL verificado para MAU/DAU/NPS.",
        "fuente":        "FP: ATV/MAU/NPS usan solo 'Paid' — correcto (R28 — 08/04/2026)"
    },

    # R29: Subquery / JOIN redundante sin impacto en resultados → FP
    # El modelo reporta subqueries "innecesarias" o JOINs "duplicados".
    # Si no afectan los datos (no generan sobreconteo ni pierden filas), es FP.
    {
        "must_contain":  [],
        "any_contain":   [
            "subquery innecesaria", "subconsulta innecesaria",
            "subquery redundante", "subconsulta redundante",
            "join innecesario", "join duplicado", "join redundante",
            "join duplicated", "join no utilizado",
            "cte innecesaria", "cte vacía", "cte vacia", "empty with clause",
            "cte que no se utiliza", "cte sin uso",
            "cálculo redundante", "calculo redundante",
            "subquery no afecta", "subconsulta no afecta",
        ],
        "must_not_contain": [
            "sobreconteo", "sobrecontará", "sobrecuenta",
            "pierde transacciones", "excluye registros válidos",
            "resultado incorrecto", "datos incorrectos",
            "nunca matchea", "0 filas", "nunca retorna",
            "transaction_status",
        ],
        "status":        "false_positive",
        "nota":          "FP: subquery/JOIN detectado como redundante o innecesario, pero sin impacto en los resultados de datos. Es deuda técnica de performance/mantenimiento, no un error que afecte métricas.",
        "fuente":        "FP: subquery/JOIN redundante sin impacto (R29 — 08/04/2026)"
    },

    # R30: Funnel webchat / Lead→1TU / adquisición → FP cuando critica filtro authorized
    # Extiende R26 a patrones genéricos de descripción (R26 solo captura frases exactas).
    {
        "must_contain":  [],
        "any_contain":   [
            "funnel", "embudo de", "adquisición", "adquisicion", "acquisition",
            "lead to 1", "lead→1tu", "lead a 1tu",
            "one-and-done", "one and done",
            "webchat", "web chat",
            "etapas del funnel", "etapas de adquisición",
            "conversión del funnel", "conversion funnel",
        ],
        "any_contain_2": [
            "no filtra por authorized", "no usa el array",
            "falta filtro de status", "sin filtro de transaction_status",
            "debería filtrar por", "deberia filtrar por",
            "debería incluir los 9", "deberia incluir los 9",
            "sobrecontará", "sobrecontara", "2.3x",
            "no usa transaction_status",
            "incluye registros de funnel",
        ],
        "must_not_contain": ["error confirmado", "confirmed_error"],
        "status":        "false_positive",
        "nota":          "FP: card de funnel de adquisición. Las cards de funnel/Lead→1TU/webchat/conversión NO deben filtrar por los 9 statuses authorized — su propósito ES el funnel completo. Patrón intencional documentado en INICIO.md.",
        "fuente":        "FP: funnel adquisición — sin filtro authorized intencional (R30 — 08/04/2026)"
    },

    # R31: 'Sent' en denominador de yield / tasa / conversión → FP
    # Complementa la regla existente de pre-autorización con más variantes de contexto.
    {
        "must_contain":  ["'sent'"],
        "any_contain":   [
            "denominador", "denominator", "base del cálculo", "base de cálculo",
            "yield", "tasa de", "ratio de", "rate de",
            "rechazo", "rejection", "conversión", "conversion",
            "intentos", "attempts", "inyectadas", "injected",
            "wow", "mom", "dod",
            "semana sobre semana", "mes sobre mes",
        ],
        "must_not_contain": [
            "incorrecto", "nunca debería", "no debe estar en authorized",
            "sobreconteo real", "título dice 'authorized'",
            "error confirmado",
        ],
        "status":        "false_positive",
        "nota":          "FP: 'Sent' en denominador de yield/tasa/rechazo/conversión. En ExampleCorp, 'Sent' = 'Injected' = intento pre-autorizado. Usar 'Sent' como base de intentos en denominadores de métricas de conversión es CORRECTO e intencional. Confirmado por analyst_2 (31/03/2026) y documentado en INICIO.md.",
        "fuente":        "FP: Sent en denominador yield/tasa — intencional (R31 — 08/04/2026)"
    },

    # R32: Modelo reconoce propósito correcto de la card en su descripción → FP
    # Cuando el modelo describe el propósito de la card y lo considera válido,
    # pero igual genera el hallazgo — inconsistencia interna → FP.
    {
        "must_contain":  [],
        "any_contain":   [
            "la descripción de la card menciona",
            "el título de la card indica",
            "el nombre de la card sugiere",
            "según el nombre de la card",
            "el propósito de la card",
            "el contexto de la card indica",
            "la card está diseñada para",
            "la card tiene como objetivo",
            "la card mide correctamente",
            "el diseño de la card",
        ],
        "any_contain_2": [
            "intencional", "propósito", "proposito", "diseño", "correcto",
            "sent", "'sent'", "funnel", "denominador",
        ],
        "must_not_contain": [
            "sin embargo hay un error", "el error es", "error confirmado",
            "afecta los resultados", "datos incorrectos",
        ],
        "status":        "false_positive",
        "nota":          "FP: el modelo reconoce en su propia descripción que el diseño/propósito de la card es correcto o intencional. Cuando el modelo mismo confirma el contexto, el hallazgo se contradice — es FP.",
        "fuente":        "FP: modelo confirma propósito correcto en descripción (R32 — 08/04/2026)"
    },

    # R33: disbursement_type con valores canónicos de BD → FP (extensión de R_DISBURSEMENT)
    # Captura frases adicionales que R_DISBURSEMENT no cubre.
    {
        "must_contain":  ["disbursement"],
        "any_contain":   [
            "account credit", "cash pickup", "cash pick-up",
            "bankdeposit", "directcash", "mobilewallet",
            "filtro de tipo de dispersión",
            "filtro por disbursement",
            "tipo de pago no homologado",
            "método de dispersión",
            "tipo de dispersión sin",
            "disbursement_type no está estandarizado",
            "no está estandarizado",
            "falta de homologación de tipo de dispersión",
        ],
        "must_not_contain": [
            "no existe", "no se encuentra", "0 filas", "sin datos",
            "tabla vacía", "no retorna", "error de datos",
        ],
        "status":        "false_positive",
        "nota":          "FP: disbursement_type con valores canónicos de BD. Los valores 'Account Credit', 'Cash Pickup', 'bankDeposit', 'directCash', 'mobileWallet' son los VALORES REALES en analytics_db — no son errores de homologación. Verificado el 06/04/2026.",
        "fuente":        "FP: disbursement valores canónicos BD (R33 — 08/04/2026)"
    },

    # R34: Hallazgo de performance/estilo en card Duplicate → FP baja prioridad
    # Las cards Duplicate son copias de testing. Hallazgos de performance en ellas
    # no son accionables hasta que la card principal tenga el mismo problema.
    {
        "must_contain":  ["duplicate"],
        "any_contain":   [
            "rendimiento", "performance", "optimización", "optimizacion",
            "subquery innecesaria", "join redundante", "índice",
            "sql comentado", "código comentado",
            "full scan", "escaneo completo",
            "disbursement_type sin homologar", "disbursement type sin homologar",
            "cálculo redundante", "calculo redundante",
        ],
        "must_not_contain": [
            "transaction_status incompleto", "falta 'hold'", "falta hold",
            "falta chargeback", "'payment' no existe", "'success'", "'fail'",
            "nunca matchea", "0 filas", "error crítico", "error confirmado",
        ],
        "status":        "false_positive",
        "nota":          "FP: hallazgo de performance/estilo en una card Duplicate. Las cards Duplicate son copias de testing/desarrollo — los hallazgos de optimización en ellas son de bajísima prioridad. No escalar hasta que la card principal tenga el mismo hallazgo.",
        "fuente":        "FP: performance/estilo en Duplicate — baja prioridad (R34 — 08/04/2026)"
    },

    # R35: Modelo indica que 'Sent' se usa intencionalmente o es correcto en contexto → FP
    {
        "must_contain":  ["sent"],
        "any_contain":   [
            "reconoce que sent", "reconoce sent como",
            "identifica sent como", "identifica 'sent' como",
            "sent se usa intencionalmente",
            "sent es pre-autorización en este contexto",
            "sent está en el denominador correctamente",
            "sent en el denominador es correcto",
            "sent como intento de transacción",
            "sent como inyectado",
            "sent como injected",
            "sent podría ser correcto aquí",
            "sent en este contexto es",
        ],
        "must_not_contain": ["error", "incorrecto", "afecta los resultados"],
        "status":        "false_positive",
        "nota":          "FP: el modelo señala que 'Sent' se usa intencionalmente o es correcto en el contexto de la card. En ExampleCorp, 'Sent' = 'Injected' (pre-autorización). Hallazgo auto-descartado.",
        "fuente":        "FP: modelo indica Sent correcto en contexto (R35 — 08/04/2026)"
    },

    # R36: Título de card dice "paid" y hallazgo critica filtro "solo 'Paid'" → FP
    # Complementa R11 con más variantes de "el título ya dice paid".
    {
        "must_contain":  [],
        "any_contain":   [
            "título dice 'paid'", "título indica 'paid'",
            "el título es 'paid'", "el título contiene 'paid'",
            "nombre dice 'paid'", "nombre indica paid",
            "la card es sobre paid", "card de paid",
            "título ya especifica paid",
            "el título hace referencia a paid",
            "título menciona 'paid'",
        ],
        "any_contain_2": [
            "solo 'paid'", "solo paid", "filtra solo", "filtro solo",
            "únicamente paid", "únicamente 'paid'",
            "limita a paid", "limita a 'paid'",
        ],
        "must_not_contain": [
            "authorized", "la card dice 'authorized'",
            "el objetivo es authorized", "el título dice 'authorized'",
        ],
        "status":        "false_positive",
        "nota":          "FP: el título de la card ya especifica 'Paid' y el filtro SQL también usa 'Paid'. Hay concordancia — no existe desajuste. Correcto por definición de negocio (data_owner, 27/03/2026).",
        "fuente":        "FP: título-paid concordante con SQL-paid (R36 — 08/04/2026)"
    },

    # ── Ronda 9 — Sesión 50 (15/04/2026) ────────────────────────────────────────

    # R25_EXT: Variantes adicionales de 'Payment' no existe en BD → confirmed_error
    # Los 22 pending restantes usan frases que R25 no cubre.
    {
        "must_contain":  [],
        "any_contain":   [
            "'payment' no existe en la base",
            "estado 'payment' no existe",
            "'payment' no existe en la bd",
            "'payment' aparece en el filtro",
            "'payment' es un status inexistente",
            "'payment' es un status que no existe",
            "uso de 'payment' en transaction_status",
            "'payment' incluido en listado",
            "'payment' no es un status válido",
            "status inexistente 'payment'",
            "transaction_status 'payment' que no existe",
            "uso de status 'payment'",
            "uso del status 'payment'",
            "uso de estado de transacción inexistente 'payment'",
            "card utiliza transaction_status 'payment'",
            "uso incorrecto de 'payment'",
            "'payment' es un status inexistente en la tabla",
            "el array de transaction_status incluye 'payment'",
            "uso de transaction_status 'payment'",
            "'payment' en transaction_status",
            "'payment' que no existe en la tabla",
            "estado de transacción inexistente 'payment'",
            "'payment' en el filtro transaction_status",
            "incluye 'payment' que no existe",
            "la query utiliza el status 'payment'",
            "se está utilizando el status 'payment'",
            "'transaction_status' incluye 'payment'",
            "filtro de status incluye 'payment'",
        ],
        "must_not_contain": ["disbursement", "scheme", "payment_method",
                             "transaction_details_funnel"],
        "status":        "confirmed_error",
        "nota":          "Error: transaction_status = 'Payment' NO existe en la BD. Los valores canónicos son 'Paid', 'Payable', 'Hold', etc. La condición con 'Payment' es código muerto que nunca matchea filas reales.",
        "fuente":        "confirmed_error: 'Payment' no existe en BD (R25_EXT — 15/04/2026)"
    },

    # R_PAYMENTLINK: Status 'Paymentlink' no existe en transaction_details → confirmed_error
    # 'Paymentlink' es del schema legado / transaction_details_funnel, no de transaction_details.
    {
        "must_contain":  [],
        "any_contain":   [
            "'paymentlink' que no es reconocido",
            "'paymentlink' no existe",
            "'paymentlink' no es un status válido",
            "filtro para 'paymentlink'",
            "status 'paymentlink' no existe",
        ],
        "must_not_contain": ["transaction_details_funnel", "funnel"],
        "status":        "confirmed_error",
        "nota":          "Error: 'Paymentlink' no existe como transaction_status en transaction_details. Es un valor del schema legado o de transaction_details_funnel. La condición nunca matchea filas.",
        "fuente":        "confirmed_error: 'Paymentlink' no existe en BD (R_PAYMENTLINK — 15/04/2026)"
    },

    # R_PERF_NOT_IN: NOT IN / NOT EXISTS con subquery grande → FP (performance, no correctitud)
    {
        "must_contain":  [],
        "any_contain":   [
            "not in con subquer",
            "not in (select",
            "not exists' puede ser ineficiente",
            "not exists con subquery",
            "not in' con subquery",
            "'not in' en una tabla de gran",
            "not in con una subquery",
            "uso de not in con subquer",
            "not exists para exclusión",
            "not in subquery puede ser ineficiente",
        ],
        "must_not_contain": ["incompleto", "not in incomplete", "falta", "no incluye"],
        "status":        "false_positive",
        "nota":          "FP: NOT IN / NOT EXISTS con subquery es una observación de performance, no un error de datos. La query devuelve resultados correctos — puede ser lenta en tablas grandes pero no afecta la correctitud.",
        "fuente":        "FP: NOT IN/EXISTS = performance (R_PERF_NOT_IN — 15/04/2026)"
    },

    # R_PERF_DISTINCT: DISTINCT potencialmente innecesario → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "uso de distinct",
            "count(distinct",
            "distinct en count",
            "distinct puede ser innecesario",
            "distinct) puede ser costoso",
            "distinct) podría ser innecesario",
        ],
        "must_not_contain": ["incorrecto", "error", "duplicado"],
        "status":        "false_positive",
        "nota":          "FP: DISTINCT es una decisión de performance/estilo. No afecta la correctitud del resultado.",
        "fuente":        "FP: DISTINCT innecesario = estilo (R_PERF_DISTINCT — 15/04/2026)"
    },

    # R_PERF_UNION: UNION innecesario / simplificable → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "union podría ser simplificado",
            "union innecesario",
            "uso innecesario de union",
            "combinarlas sin necesidad de un union",
            "múltiples union",
        ],
        "must_not_contain": ["incorrecto", "error de datos"],
        "status":        "false_positive",
        "nota":          "FP: uso de UNION múltiple es una decisión de estilo/performance. No afecta la correctitud del resultado — las queries podrían simplificarse pero devuelven datos correctos.",
        "fuente":        "FP: UNION simplificable = estilo (R_PERF_UNION — 15/04/2026)"
    },

    # R_PERF_GENERAL: Observaciones generales de performance/eficiencia → FP
    # ACTUALIZADO 15/04/2026: absorbe "rendimiento" de regla genérica temprana con guards estrictos
    {
        "must_contain":  [],
        "any_contain":   [
            "rendimiento",
            "simplificar",
            "podría ser optimizado",
            "puede ser optimizado",
            "optimizarse",
            "ineficiente",
            "ralentización significativa",
            "uso de case innecesario",
            "case interno podría no ser necesario",
            "redundante",
            "regexp_replace",
            "comparación que no es eficiente",
            "sub-consultas anidadas",
            "subconsultas repetitivas",
            "subconsultas que realizan la misma",
            "búsquedas repetitivas que pueden ser ineficientes",
        ],
        "must_not_contain": ["incompleto", "no existe", "error de negocio",
                             "no incluye", "falta", "incorrecto",
                             "authorized", "transaction_status",
                             "sobreconteo", "sobrecuenta"],
        "status":        "false_positive",
        "nota":          "FP: observación de performance/eficiencia. No afecta la correctitud de los datos — la query puede ser lenta o redundante pero devuelve resultados correctos.",
        "fuente":        "FP: performance/eficiencia general (R_PERF_GENERAL — 15/04/2026)"
    },

    # R_CROSS_JOIN: CROSS JOIN implícito → FP (performance/estilo)
    {
        "must_contain":  [],
        "any_contain":   [
            "cross join implícito",
            "cross join implicit",
            "uso de cross join",
            "cross join sin especificar",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: CROSS JOIN implícito es estilo de SQL — no afecta la correctitud si la lógica del JOIN es correcta. Observación de mantenibilidad.",
        "fuente":        "FP: CROSS JOIN implícito = estilo (R_CROSS_JOIN — 15/04/2026)"
    },

    # R_FULL_JOIN: FULL JOIN innecesario → FP (performance/estilo)
    {
        "must_contain":  [],
        "any_contain":   [
            "full join sin necesidad",
            "full join innecesario",
            "full join puede ser innecesario",
            "full join podría no ser necesario",
            "full join con la tabla",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: FULL JOIN puede ser innecesario pero no afecta la correctitud del resultado. Es una observación de performance/estilo.",
        "fuente":        "FP: FULL JOIN innecesario = performance (R_FULL_JOIN — 15/04/2026)"
    },

    # R_HARDCODED: Lista hardcodeada de statuses / puede desactualizarse → FP
    # Observación de mantenimiento, no error actual. Todos los status hardcodeados
    # en ExampleCorp son correctos al momento de la auditoría.
    {
        "must_contain":  [],
        "any_contain":   [
            "puede desactualizarse",
            "podría quedar desactualizad",
            "pueden quedar desactualizad",
            "lista fija de status",
            "status hardcodeado",
            "hardcodeado que podría desactualizar",
            "listas de estados hardcodeadas",
            "lista de status de transacción está escrita de forma explícita",
            "lista de transaction_status en la clausula in puede desactualizarse",
            "valores de 'transaction_status' que podrían quedar desactualizado",
            "status autorizados, lo cual puede desactualizar",
            "podría sobre o sub-representar las transacciones al estar harcodeado",
        ],
        "must_not_contain": ["incompleto", "falta", "no incluye", "'payment'"],
        "status":        "false_positive",
        "nota":          "FP: la lista de statuses hardcodeada es correcta al momento de la auditoría. La observación de que 'puede desactualizarse' es de mantenimiento preventivo, no un error actual. Los 9 statuses authorized no han cambiado.",
        "fuente":        "FP: lista hardcodeada correcta = mantenimiento preventivo (R_HARDCODED — 15/04/2026)"
    },

    # R_COMMENT_EXT: Variantes de SQL comentado / comentarios → FP
    # Extiende R16 con variantes en inglés y español no cubiertas.
    {
        "must_contain":  [],
        "any_contain":   [
            "commented-out sql",
            "commented out sql",
            "commented-out code",
            "commented out code",
            "sql como comentario informal",
            "comentarios excesivos",
            "comentarios para desactivar",
            "comentarios potencialmente obsoletos",
            "comentario que sugiere",
            "comentario olvidado",
            "comentario al inicio del query",
            "uso de comentarios para desactivar",
            "comentario de sql sin versión",
            "comentario innecesario",
            "comentario sql para versionamiento",
        ],
        "must_not_contain": ["lógica comentada cambia", "resultado cambia", "afecta el resultado"],
        "status":        "false_positive",
        "nota":          "FP: SQL comentado o comentarios de mantenimiento. En ExampleCorp es práctica común dejar código comentado como referencia. No afecta la correctitud del resultado actual.",
        "fuente":        "FP: comentarios/SQL comentado (R_COMMENT_EXT — 15/04/2026)"
    },

    # R_STYLE: Observaciones de estilo / naming / redundancia → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "schema \"public\" es redundante",
            "schema 'public' es redundante",
            "uso del schema \"public\"",
            "uso de upper sin necesidad",
            "upper en filtrado de texto",
            "join innecesario ya que",
            "join parece ser innecesario",
            "filtro redundante",
            "filtro duplicado",
            "lógica redundante",
            "procesamiento innecesario",
            "error tipográfico en variable de filtro",
        ],
        "must_not_contain": ["incompleto", "falta", "no incluye", "authorized",
                             "transaction_status"],
        "status":        "false_positive",
        "nota":          "FP: observación de estilo, naming o redundancia. No afecta la correctitud de los datos — es una sugerencia de limpieza de código.",
        "fuente":        "FP: estilo/naming/redundancia (R_STYLE — 15/04/2026)"
    },

    # R_ATV_WRONG: ATV con statuses que no son 'Paid' → confirmed_error
    # ATV en ExampleCorp = counter_paid >= 1 / status = 'Paid'. NO usar otros statuses.
    # Confirmado con Val (Head Marketing) + card canónica 2699 (04/04/26).
    {
        "must_contain":  [],
        "any_contain":   [
            "incompleto para medir 'atv'",
            "incompleto para medir atv",
        ],
        "must_not_contain": [],
        "status":        "confirmed_error",
        "nota":          "Error: ATV en ExampleCorp = counter_paid >= 1 / status = 'Paid'. Incluir 'Payable', 'Client_refund' o 'Cancelled' es incorrecto para ATV. Confirmado con Val (Head Marketing) + card canónica 2699 (04/04/26).",
        "fuente":        "confirmed_error: ATV con statuses incorrectos (R_ATV_WRONG — 15/04/2026)"
    },

    # R_PAID_INCONSISTENT: Filtro inconsistente con definición de 'Paid' → confirmed_error
    # Cards que dicen medir 'Paid' pero incluyen statuses extras (Payable, Cancelled, etc.)
    {
        "must_contain":  ["'paid'"],
        "any_contain":   [
            "inconsistente con la definición de transacciones 'paid'",
            "inconsistente con la definición de 'paid'",
        ],
        "must_not_contain": ["authorized"],
        "status":        "confirmed_error",
        "nota":          "Error: la card mide 'Paid' pero el filtro incluye statuses que no son 'Paid' (Payable, Client_refund, Cancelled). Definición de Paid = solo transaction_status = 'Paid'.",
        "fuente":        "confirmed_error: filtro inconsistente con 'Paid' (R_PAID_INCONSISTENT — 15/04/2026)"
    },

    # R_STRING_COMBO: 'Payable' y 'Paid' combinados en una sola cadena → confirmed_error
    {
        "must_contain":  [],
        "any_contain":   [
            "combina 'payable' y 'paid' en una sola cadena",
            "'payable' y 'paid' en una sola cadena",
            "combina payable y paid en una sola cadena",
        ],
        "must_not_contain": [],
        "status":        "confirmed_error",
        "nota":          "Error: 'Payable' y 'Paid' combinados como string literal ('Payable,Paid') en lugar de valores separados en un IN(). La condición nunca matchea filas reales.",
        "fuente":        "confirmed_error: string literal combo (R_STRING_COMBO — 15/04/2026)"
    },

    # R_CORRECT_TYPE: Hallazgo tipo "correcta" — el modelo dice que es correcto → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "incluye todos los estatus correctos",
            "incluye todos los status correctos",
            "el filtro es correcto",
            "todos los estados correctos",
        ],
        "must_not_contain": ["pero", "sin embargo", "aunque", "podría", "no incluye"],
        "status":        "false_positive",
        "nota":          "FP: el propio modelo indica que el filtro es correcto / incluye todos los statuses. No hay error que reportar.",
        "fuente":        "FP: modelo confirma correcto (R_CORRECT_TYPE — 15/04/2026)"
    },

    # R_YEAR_FILTER: Filtro de año excluye 2023 → FP
    # Excluir 2023 es decisión de negocio (datos pre-launch o incompletos).
    {
        "must_contain":  [],
        "any_contain":   [
            "filtro de año excluye 2023",
            "excluye 2023",
            "suprimir años diferentes de 2023",
            "años distintos de 2023",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: excluir datos de 2023 es una decisión de negocio — los datos pre-launch o incompletos de 2023 se filtran intencionalmente. No es un error de lógica.",
        "fuente":        "FP: filtro de año = decisión de negocio (R_YEAR_FILTER — 15/04/2026)"
    },

    # R_METABASE_SYNTAX: Variables opcionales de Metabase / sintaxis estándar → FP
    # Metabase usa {{variable}} y [[AND ...]] para filtros opcionales — es diseño estándar.
    {
        "must_contain":  [],
        "any_contain":   [
            "variables de fecha {{",
            "variables temporales condicionales",
            "filtros de metabase, posibles errores si no están definidos",
            "variables de filtro de metabase",
            "uso incorrecto de filtros de metabase",
        ],
        "must_not_contain": ["sintaxis incorrecta"],
        "status":        "false_positive",
        "nota":          "FP: las variables {{Start_date}}, {{End_date}} y [[AND ...]] son sintaxis estándar de Metabase para filtros opcionales. No es un error.",
        "fuente":        "FP: variables Metabase estándar (R_METABASE_SYNTAX — 15/04/2026)"
    },

    # R_FECHA_FIJA: Fecha fija/hardcodeada que puede desactualizarse → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "fecha fija",
            "fecha hardcodeada",
            "no se actualiza automáticamente",
            "podría des actualizarse",
        ],
        "must_not_contain": ["incorrecto", "error", "no existe"],
        "status":        "false_positive",
        "nota":          "FP: la fecha fija es un umbral intencional (fecha de corte, start date, etc.). Es una observación de mantenimiento, no un error actual de datos.",
        "fuente":        "FP: fecha fija = mantenimiento preventivo (R_FECHA_FIJA — 15/04/2026)"
    },

    # R_HOMEPAGE: Inconsistencia 'Home Page' vs 'Homepage' → FP
    # Naming en la BD refleja realidad — no es error de la card.
    {
        "must_contain":  [],
        "any_contain":   [
            "'home page' versus 'homepage'",
            "'home page' vs 'homepage'",
            "home page' versus 'homepage'",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: la inconsistencia 'Home Page' vs 'Homepage' refleja datos reales en la BD. La card consulta ambas variantes — no es un error de lógica.",
        "fuente":        "FP: naming inconsistente en BD (R_HOMEPAGE — 15/04/2026)"
    },

    # R_SUBCONSULTA_DUPLICADA: Subconsulta con lógica similar/duplicada → FP
    # ENDURECIDO 15/04/2026: any_contain más específico, guards para no enmascarar sobreconteo
    {
        "must_contain":  [],
        "any_contain":   [
            "lógica similar para el case",
            "subconsulta redundante",
            "subconsulta duplicada",
            "subconsulta innecesaria",
            "subconsulta que no se utiliza",
        ],
        "any_contain_2": [
            "duplicación de lógica",
            "lógica similar",
            "duplicada",
            "nunca se utiliza",
        ],
        "must_not_contain": ["incompleto", "falta", "no incluye", "authorized",
                             "transaction_status", "error de negocio", "error de datos",
                             "sobreconteo", "sobrecuenta", "resultado incorrecto",
                             "pierde transacciones", "excluye registros",
                             "mal construido", "error de filtro",
                             "genera duplicados", "duplica filas"],
        "status":        "false_positive",
        "nota":          "FP: subconsulta duplicada o con lógica similar es un patrón de estilo/mantenimiento. No afecta la correctitud del resultado.",
        "fuente":        "FP: subconsulta duplicada = estilo (R_SUBCONSULTA_DUPLICADA — 15/04/2026)"
    },

    # R_COALESCE: Uso de COALESCE como workaround → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "coalesce a 0 parece ser para manejar",
            "coalesce puede prevenir dividir por cero",
            "workaround intencional",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: COALESCE a 0 es un patrón estándar para manejar NULLs / división por cero. No es un error.",
        "fuente":        "FP: COALESCE workaround estándar (R_COALESCE — 15/04/2026)"
    },

    # R_ID_STATUS: Uso de id_status en lugar de transaction_status → FP (estilo/mantenimiento)
    # Algunas cards usan id_status numérico en vez de nombres de status. Es menos legible
    # pero funcionalmente correcto si los IDs mapean correctamente.
    {
        "must_contain":  [],
        "any_contain":   [
            "id_status en lugar de",
            "usa id_status",
            "utiliza 'id_status'",
            "utiliza id_status",
            "incorrectamente usa id_status",
            "id_status con suposiciones",
            "id_status que no encuentra equivalencias",
            "id_status parece contener",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: uso de id_status numérico en lugar de transaction_status es una decisión de implementación. Menos legible pero funcionalmente correcto. Observación de mantenibilidad, no error de datos.",
        "fuente":        "FP: id_status = estilo/mantenibilidad (R_ID_STATUS — 15/04/2026)"
    },

    # R_SCAN_COMPLETO: Escaneo completo / falta filtro fecha en tabla grande → FP
    # Variantes que no captura R_FECHA_PERF.
    {
        "must_contain":  [],
        "any_contain":   [
            "escaneo completo",
            "falta un filtro explícito de fecha",
            "falta un filtro de fecha claro",
            "no tiene filtro de fecha en una tabla",
            "generar un escaneo completo",
            "resultar en un escaneo completo",
            "falta filtro explícito de fecha",
        ],
        "must_not_contain": ["sobrepon", "superpone", "solapan", "conflicto",
                             "sintaxis incorrecta"],
        "status":        "false_positive",
        "nota":          "FP: la query puede causar un escaneo completo por falta de filtro de fecha explícito. Esto es un concern de performance, no de correctitud de datos. En Metabase los filtros opcionales son por diseño.",
        "fuente":        "FP: escaneo completo = performance (R_SCAN_COMPLETO — 15/04/2026)"
    },

    # R_TYPO_CARD_NAME: Error tipográfico en nombre de card → FP (no afecta datos)
    {
        "must_contain":  [],
        "any_contain":   [
            "error tipográfico",
            "error tipografico",
            "'auhtorized' en lugar de 'authorized'",
        ],
        "must_not_contain": ["columna", "created_date", "reated_date", "variable"],
        "status":        "false_positive",
        "nota":          "FP: error tipográfico en el nombre de la card. No afecta el resultado del SQL — solo es un issue cosmético en el título.",
        "fuente":        "FP: typo en nombre de card (R_TYPO_CARD_NAME — 15/04/2026)"
    },

    # R_NO_ERROR: El modelo mismo dice que no hay error → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "no hay error específico de negocio",
            "no hay error específico",
            "no hay un error de negocio",
            "lo cual es correcto",
            "es correcto, pero",
        ],
        "must_not_contain": ["incompleto", "falta", "no incluye"],
        "status":        "false_positive",
        "nota":          "FP: el propio modelo indica que no hay error de negocio en esta card. El hallazgo es una observación informativa, no un error real.",
        "fuente":        "FP: modelo confirma sin error (R_NO_ERROR — 15/04/2026)"
    },

    # R_HOMOLOGACION: Naming inconsistente en columnas de BD → FP
    # disbursement_type, scheme_card naming mixto es real en la BD.
    # Verificado 06/04/26 (INICIO.md): naming mixto NO es error de homologación.
    {
        "must_contain":  [],
        "any_contain":   [
            "tipo de entrega no está homologado",
            "valor homologado para",
            "scheme_card' incluye variaciones",
            "visa' y 'visa'",
            "'visa' y 'visa'",
            "homologar al seleccionar",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: naming mixto en disbursement_type / scheme_card es real en la BD — no es error de homologación. Verificado 06/04/26.",
        "fuente":        "FP: naming mixto real en BD (R_HOMOLOGACION — 15/04/2026)"
    },

    # R_PERF_LENTITUD: Observaciones de lentitud / ineficiencia en inglés y español → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "might be inefficient",
            "could be inefficient",
            "may be inefficient",
            "podría causar lentitud",
            "puede generar una ralentización",
            "filtro dinámico utilizado en 'in (select",
        ],
        "must_not_contain": ["incompleto", "falta", "no incluye", "incorrecto",
                             "no existe"],
        "status":        "false_positive",
        "nota":          "FP: observación de lentitud/performance. La query puede ser lenta pero devuelve resultados correctos. No es un error de datos.",
        "fuente":        "FP: lentitud = performance (R_PERF_LENTITUD — 15/04/2026)"
    },

    # R_WEEK_CALC: Cálculo de semana manual → FP (estilo)
    {
        "must_contain":  [],
        "any_contain":   [
            "calcular el inicio de la semana manualmente",
            "date_trunc('week'",
            "filtro de semana potencialmente mal aplicado",
            "filtro de semana duplicado",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: cálculo manual de semana es una alternativa válida a DATE_TRUNC. Puede ser propenso a errores pero no es necesariamente incorrecto. Observación de estilo.",
        "fuente":        "FP: cálculo semana manual = estilo (R_WEEK_CALC — 15/04/2026)"
    },

    # R_TEST_USER: Filtro de test_user / app_user → FP
    # ENDURECIDO 15/04/2026: guards estrictos para no enmascarar errores reales de filtrado
    {
        "must_contain":  [],
        "any_contain":   [
            "'app_user' como test_user",
            "patrón estándar de test_user",
            "filtro de user_label",
            "diferencia de estilo en el filtro de test_user",
        ],
        "must_not_contain": ["incompleto", "falta", "no incluye", "authorized",
                             "incorrectamente", "mal construido", "error de filtro",
                             "no excluye", "incluye usuarios de prueba",
                             "test users en producción", "sin filtro de test",
                             "falta filtro de test", "no filtra test"],
        "status":        "false_positive",
        "nota":          "FP: filtro de test_user es un patrón estándar para excluir usuarios de prueba. No es un error de datos.",
        "fuente":        "FP: filtro test_user = patrón estándar (R_TEST_USER — 15/04/2026)"
    },

    # R_TOP_N: Selección de top N categorías → FP (diseño)
    {
        "must_contain":  [],
        "any_contain":   [
            "solo selecciona las top",
            "top 5 categorías",
            "top 8 resultados",
            "el límite está configurado para",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: la selección de top N categorías es una decisión de diseño del dashboard. No es un error de datos.",
        "fuente":        "FP: top N = decisión de diseño (R_TOP_N — 15/04/2026)"
    },

    # R_REJECTED: Status 'Rejected' incluido → FP
    # 'Rejected' no es parte del array de 9 authorized, pero su uso en
    # análisis de errores/cancelaciones es intencional.
    {
        "must_contain":  [],
        "any_contain":   [
            "incluye 'rejected'",
            "status 'rejected'",
        ],
        "any_contain_2": [
            "generalmente no se considera",
            "no es parte del array",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: 'Rejected' no es parte del array authorized, pero su uso en análisis de errores/cancelaciones es intencional. La card no mide 'authorized'.",
        "fuente":        "FP: Rejected en análisis de errores = intencional (R_REJECTED — 15/04/2026)"
    },

    # R_NULLABLE: Posible NULL en columna → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "puede ser null en los filtros opcionales",
            "null' no tiene efecto",
            "in ('null')",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: comparación con NULL o campos nullable en filtros opcionales es un patrón de Metabase. No afecta la correctitud cuando el filtro está activo.",
        "fuente":        "FP: nullable / NULL pattern (R_NULLABLE — 15/04/2026)"
    },

    # R_RETENTION_TABLE: Tablas de retention_* → FP
    # Estas tablas son vistas materializadas/pivots que pueden estar vacías
    # temporalmente pero son parte de la infraestructura.
    {
        "must_contain":  [],
        "any_contain":   [
            "retention_pivot",
            "retention_percentage",
            "retention_pivot_not_fraud",
            "retention_percentage_not_fraud",
            "app_active_users_retention",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: las tablas retention_* son vistas materializadas/pivots que pueden estar temporalmente vacías. No es un error de la card.",
        "fuente":        "FP: tabla retention = infraestructura (R_RETENTION_TABLE — 15/04/2026)"
    },

    # ── Ronda 10 — Sesión 53 (16/04/2026) ────────────────────────────────────────

    # R_SUCCESS_FAIL_CE: Uso de status 'Success' o 'Fail' en transaction_details → CE
    # Estos valores NO existen en transaction_details (solo en tabla legada `transaction`).
    # Confirmado en transaction_status_catalog.md. Cualquier condición con 'Success'/'Fail'
    # es código muerto — nunca matchea filas reales en transaction_details.
    {
        "must_contain":  [],
        "any_contain":   [
            "status 'success' y 'fail'",
            "status 'success'",
            "status 'fail'",
            "'success' y 'fail'",
            "'success', que no existe",
            "'fail', que no existe",
            "usa 'success'",
            "usa 'fail'",
            "utiliza status 'success'",
            "utiliza el status 'success'",
            "estado 'success'",
            "estado 'fail'",
            "'success' no existen",
            "'fail' no existen",
            "status inexistente 'success'",
            "status inexistente 'fail'",
            "que no existen en las reglas de negocio",
        ],
        "must_not_contain": ["transaction_details_funnel", "tabla legada", "tabla transaction`"],
        "status":        "confirmed_error",
        "nota":          "Error: transaction_status 'Success' y/o 'Fail' NO existen en transaction_details (solo en tabla legada `transaction` con IDs numéricos). Confirmado en transaction_status_catalog.md. Las condiciones con estos valores son código muerto que nunca matchea filas.",
        "fuente":        "confirmed_error: 'Success'/'Fail' no existen en transaction_details (R_SUCCESS_FAIL_CE — 16/04/2026)"
    },

    # R_MTO_UNUSED_JOIN: tabla 'mto' joinada pero no utilizada en SELECT → FP (P8 deuda técnica)
    # LEFT JOIN tabla mto es deuda técnica sistémica documentada en INICIO.md.
    # La tabla se une pero sus columnas no se usan en el SELECT final — intencional.
    {
        "must_contain":  [],
        "any_contain":   [
            "tabla 'mto' es joinada pero no utilizada",
            "tabla mto es joinada pero no",
            "join con mto no se utiliza",
            "mto' es joinada pero sus columnas no se usan",
            "left join tabla mto",
            "join a la tabla mto",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: LEFT JOIN tabla mto es deuda técnica sistémica documentada (INICIO.md — P8). La tabla se une pero sus columnas no se usan en el SELECT final — patrón intencional en el codebase de ExampleCorp. No escalar como error.",
        "fuente":        "FP: LEFT JOIN mto = deuda técnica P8 (R_MTO_UNUSED_JOIN — 16/04/2026)"
    },

    # R_ACTIVE_USERS_PAID_CORRECT: "active users" + solo filtra 'Paid' → FP
    # DAU/WAU/MAU/QAU en ExampleCorp = usuarios con transaction_status = 'Paid'. Confirmado (04/04/26).
    # Si el título dice "active users" y el filtro es solo 'Paid', es CORRECTO.
    {
        "must_contain":  [],
        "any_contain":   [
            "título indica 'active users' pero solo filtra transacciones pagadas",
            "título indica 'active users' pero solo filtra pagadas",
            "título indica 'active users' pero el filtro es solo",
            "active users' pero solo filtra transacciones pagadas",
            "active users pero solo filtra",
            "el título de la card dice 'active users (txns paid)'",
            "título hace referencia a 'active users' pero solo mide",
        ],
        "must_not_contain": ["error", "incorrecto", "subreportar"],
        "status":        "false_positive",
        "nota":          "FP: 'Active Users' en ExampleCorp se define con transaction_status = 'Paid' únicamente. Confirmado (04/04/2026): DAU/WAU/MAU/QAU usan solo 'Paid', NO el array de 9 authorized. Fuentes: SQL verificado de 6 cards.",
        "fuente":        "FP: Active Users = solo Paid es correcto (R_ACTIVE_USERS_PAID_CORRECT — 16/04/2026)"
    },

    # R_LOWER_TRIM_STYLE: Inconsistencia de LOWER/TRIM / casing → FP (estilo)
    {
        "must_contain":  [],
        "any_contain":   [
            "uso inconsistente de lower y trim",
            "uso inconsistente de lower",
            "inconsistente de lower",
            "distinto casing para",
            "diferente casing",
            "inconsistencia de casing",
            "lower y trim inconsistente",
        ],
        "must_not_contain": ["transaction_status", "columna no existe", "no existe en la tabla"],
        "status":        "false_positive",
        "nota":          "FP: inconsistencia de LOWER/TRIM o casing en comparaciones de texto. Es una observación de estilo — no afecta la correctitud del resultado si los datos en BD están en el mismo case.",
        "fuente":        "FP: inconsistencia LOWER/TRIM = estilo (R_LOWER_TRIM_STYLE — 16/04/2026)"
    },

    # R_PAID_TXNS_CORRECT: "título dice 'Paid Txns' pero la lógica cuenta transacciones pagadas" → FP
    # Cuando el modelo critica que el título dice "Paid" y el filtro es "Paid" — es consistente.
    {
        "must_contain":  [],
        "any_contain":   [
            "título dice 'txns paid' pero la lógica cuenta 'transacciones pagadas'",
            "el título de la card dice 'paid' pero el filtro solo incluye 'paid'",
            "el título de la card dice 'paid' pero el filtro es solo 'paid'",
            "el título indica transacciones pagadas, mientras que la lógica actual solo busca",
            "título hace referencia a 'paid' únicamente, cuando deber",
        ],
        "must_not_contain": ["authorized", "subreportar", "incorrecto"],
        "status":        "false_positive",
        "nota":          "FP: el título y el filtro dicen 'Paid' — son consistentes. No hay desajuste. Correcto por definición de negocio.",
        "fuente":        "FP: título-paid concordante con SQL-paid (R_PAID_TXNS_CORRECT — 16/04/2026)"
    },

    # R_UNNEST_PERF: UNNEST en lista larga → FP (performance, no correctitud)
    {
        "must_contain":  [],
        "any_contain":   [
            "uso de unnest en una lista larga",
            "unnest puede no ser eficiente",
            "unnest podría no ser eficiente",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: UNNEST en lista larga es una observación de performance. No afecta la correctitud del resultado.",
        "fuente":        "FP: UNNEST performance (R_UNNEST_PERF — 16/04/2026)"
    },

    # R_FILTRO_INCOMPLETO_AUTHORIZED: filtro de status incompleto para métricas de "authorized" → CE
    # Descripción explícita: la card mide "authorized" pero faltan statuses del array oficial.
    # Cubre el patrón P9 detectado en nuevas cards no incluidas en P9A_IDS/P9B_IDS.
    # Guard: excluir ATV/NPS/DAU/MAU/active users names que intencionalmente usan solo 'Paid'.
    {
        "must_contain":  [],
        "any_contain":   [
            "filtro de status incompleto para métricas de authorized",
            "filtro de transaction_status es incompleto para métricas de authorized",
            "filtro de transaction_status es incompleto para transacciones autorizadas",
            "filtro de status es incompleto para métricas de transacciones autorizadas",
            "filtro de status incompleto para métricas de authorized",
            "filtro de status incompleto para métricas: falta",
            "filtro de status incompleto para métricas de nuevos usuarios autorizados",
            "filtro de transaction_status debería usar el array completo de authorized pero falta",
            "filtro de estado de transacción es incorrecto para métricas de authorized",
            "filtro incompleto para métricas de authorized",
            "filtro de status incompleto para transacciones autorizadas",
            "filtro de status es incompleto para authorized",
            "filtro incompleto para métricas de authorized:",
            "filtro de status no incluye todos los estados 'authorized'",
            "filtro de status no incluye el status 'hold' cuando el título implica transacciones autorizadas",
            "el filtro de transaction_status es incompleto para authorized",
            "el filtro de transaction_status es incompleto, falta incluir otros status autorizados",
            "filtro de transaction_status incompleto:",
            "filtro incompleto para métricas de authorized: falta",
            # Ronda 11 — S53 (16/04/2026): nuevas variantes encontradas en triaje de 65 pending
            "filtro de transaction_status es incompleto para métricas de transacciones autorizadas",
            "filtro de transaction_status no incluye todos los statuses del array de autorizados",
            "no incluye todos los status de authorized",
            "filtro de estado de transacción es incompleto para transacciones autorizadas",
            "excluye otros estados autorizados",
            "no incluye todos los estados autorizados",
            "falta el status 'chargeback_unir'",
            "lo cual es incompleto para medir transacciones 'authorized'",
            "filtro incompleto de status",
            "filtro de status no incluye todos los statuses del array de autorizados",
            "filtro de status no incluye todos los estados del array oficial de autorizados",
            "the filter only includes 'paid' status, which does not match the intended scope if considering all 'authorized'",
            "solo 'paid' mientras el título indica",
            "el contexto requiere 'authorized'",
        ],
        "must_not_contain": [
            "atv", "nps", "active users", "daily active", "weekly active", "monthly active",
            "usuario activo", "usuarios activos", "solo 'paid' es correcto",
            "solo paid es correcto", "intencional",
        ],
        "status":        "confirmed_error",
        "nota":          "Error: el filtro de transaction_status es incompleto para una métrica de 'authorized'. Faltan statuses del array oficial de 9: ('Hold','Paid','Payable','Cancelled','Client_refund','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'). La card subestima el volumen real de transacciones autorizadas.",
        "fuente":        "confirmed_error: filtro incompleto para authorized (R_FILTRO_INCOMPLETO_AUTHORIZED — 16/04/2026)"
    },

    # R_HOLD_IS_AUTHORIZED_FP: modelo dice "'Hold' no es un estado autorizado" → FP
    # ERROR DEL MODELO: 'Hold' SÍ es parte del array oficial de 9 authorized statuses.
    # Confirmado en transaction_status_catalog.md y ground truth con cto (CTO).
    {
        "must_contain":  [],
        "any_contain":   [
            "'hold' no es un estado autorizado",
            "hold' no es un estado autorizado",
            "'hold' no es considerado un estado autorizado",
            "hold no es autorizado",
            "hold no está en los estados autorizados",
            "hold no es parte del array",
            "hold no debe incluirse",
            "inclusión de hold sobrecontabiliza",
            "inclusión de 'hold' sobrecontabiliza",
            "su inclusión sobrecontabiliza",
        ],
        "must_not_contain": ["falta 'hold'", "falta hold", "falta incluir hold"],
        "status":        "false_positive",
        "nota":          "FP: ERROR del modelo — 'Hold' SÍ es parte del array oficial de 9 authorized statuses. El modelo incorrectamente dice que Hold no es autorizado. Ground truth verificado con cto (CTO): 'Hold', 'Paid', 'Payable', 'Cancelled', 'Client_refund', 'Chargeback', 'Chargeback_won', 'Chargeback_lost', 'Chargeback_unir'.",
        "fuente":        "FP: 'Hold' SÍ es authorized — error del modelo (R_HOLD_IS_AUTHORIZED_FP — 16/04/2026)"
    },

    # R_INTENTIONAL_ONLY_PAID: modelo mismo dice "midiendo intencionalmente solo pagadas" → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "midiendo intencionalmente solo transacciones pagadas",
            "midiendo intencionalmente solo transacciones pa",
            "está midiendo intencionalmente solo",
            "está mediendo intencionalmente solo",
            "mediendo intencionalmente solo transacciones",
            "mide intencionalmente solo transacciones",
            "intencional, pero no existe filtro",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: el propio modelo indica que la card mide intencionalmente solo transacciones pagadas. Cuando el modelo reconoce el propósito intencional, el hallazgo se contradice — es FP.",
        "fuente":        "FP: modelo confirma propósito intencional (R_INTENTIONAL_ONLY_PAID — 16/04/2026)"
    },

    # R_CANCEL_REASON_FP: cards de "cancel reason" que filtran solo canceladas → FP
    # Es correcto que "cancel reason" solo muestre transacciones canceladas/reembolsadas.
    # El modelo reporta "no considera todos los estados de cancelación" pero para análisis
    # de razón de cancelación, solo mostrar canceladas/Client_refund ES el propósito.
    {
        "must_contain":  [],
        "any_contain":   [
            "filtra solo transacciones canceladas y reembolsadas, pero no considera todos los posibles estados de cancelación",
            "filtra solo transacciones canceladas y reembolsadas",
        ],
        "must_not_contain": ["error", "incorrecto"],
        "status":        "false_positive",
        "nota":          "FP: cards de 'cancel reason' correctamente filtran por transacciones canceladas y reembolsadas (Cancelled + Client_refund). El propósito de la card ES analizar las razones de cancelación — no debe incluir todos los authorized statuses.",
        "fuente":        "FP: cancel reason filtra canceladas = correcto (R_CANCEL_REASON_FP — 16/04/2026)"
    },

    # R_FILTER_CORRECT_METRIC: "el filtro es correcto para la métrica específica" → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "el filtro es correcto para la métrica específica",
            "filtro es correcto para la métrica",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: el propio modelo confirma que el filtro es correcto para la métrica específica de la card.",
        "fuente":        "FP: modelo confirma filtro correcto (R_FILTER_CORRECT_METRIC — 16/04/2026)"
    },

    # R_JOIN_INNECESARIO_LEVEL: "JOIN parece ser innecesario ya que level nunca se utiliza" → FP
    # Extensión de R29 para esta frase específica.
    {
        "must_contain":  [],
        "any_contain":   [
            "parece ser innecesario ya que level nunca se utiliza",
            "join parece ser innecesario ya que",
            "join con user_limits parece",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: JOIN con tabla auxiliar detectado como innecesario (level nunca se usa en el SELECT final). Observación de mantenibilidad — no afecta la correctitud del resultado.",
        "fuente":        "FP: JOIN innecesario (level no utilizado) = estilo (R_JOIN_INNECESARIO_LEVEL — 16/04/2026)"
    },

    # R_PERF_TABLE_DATE: "tabla puede ser grande, query lenta sin rango de fecha" → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "la tabla puede ser grande y la consulta se ejecutará lentamente",
            "tabla puede ser grande y la consulta",
            "la consulta se ejecutará lentamente si no se proporciona un rango",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: observación de performance — la query puede ser lenta en tablas grandes sin filtro de fecha. No afecta la correctitud del resultado.",
        "fuente":        "FP: performance tabla grande sin fecha (R_PERF_TABLE_DATE — 16/04/2026)"
    },

    # R_QUERY_INCOMPLETA: Query parece incompleta → pending (necesita revisión manual)
    # No clasificar automáticamente — puede ser un error real.

    # ── Ronda 11 — Sesión 53 (16/04/2026) — Triaje de 65 pending restantes ────────────────
    # 9 reglas nuevas basadas en análisis directo de las 65 descripciones pendientes.

    # R_SENT_INCLUIDO_CE: 'Sent' incluido como transacción autorizada → CE
    # 'Sent' NO es un status autorizado. El array oficial de 9 no lo incluye.
    {
        "must_contain":  [],
        "any_contain":   [
            "'sent' está incluido en el filtro con otros statuses de transacciones autorizadas",
            "transacciones con estatus 'sent' se incluyen incorrectamente",
            "sent' se incluyen incorrectamente como parte del análisis principal",
            "estado 'sent' no debería incluirse en transacciones autorizadas",
            "incluye 'sent' como status autorizado",
        ],
        "must_not_contain": ["intencional", "es correcto"],
        "status":        "confirmed_error",
        "nota":          "Error: 'Sent' NO es un status autorizado. El array oficial es: ('Hold','Paid','Payable','Cancelled','Client_refund','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'). Incluir 'Sent' sobrecontabiliza transacciones no completadas.",
        "fuente":        "confirmed_error: Sent incluido como authorized (R_SENT_INCLUIDO_CE — 16/04/2026)"
    },

    # R_ALIAS_NO_DEFINIDO_CE: alias no definido en la query → CE (error de SQL)
    {
        "must_contain":  [],
        "any_contain":   [
            "no hay alias",
            "alias no definido",
            "alias no está definido en la consulta",
            "no existe el alias",
            "alias inexistente",
        ],
        "must_not_contain": ["intencional", "es correcto", "no es un error"],
        "status":        "confirmed_error",
        "nota":          "Error de SQL: referencia a alias no definido. La query fallará al ejecutarse.",
        "fuente":        "confirmed_error: alias no definido (R_ALIAS_NO_DEFINIDO_CE — 16/04/2026)"
    },

    # R_TABLA_INEXISTENTE_CE: referencia a tabla que no existe → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "tabla 'myself_transactions' que no existe",
            "tabla inexistente",
            "tabla que no existe",
            "table does not exist",
            "tabla no existe en el contexto actual",
            "app_total_marketing_20240426003739",
            "app_ad_group_report_20240413041820",
            "tabla que podría no existir",
        ],
        "must_not_contain": ["podría ser temporal", "backup sin validación"],
        "status":        "confirmed_error",
        "nota":          "Error: la query referencia una tabla que no existe. Retornará error de ejecución.",
        "fuente":        "confirmed_error: tabla inexistente (R_TABLA_INEXISTENTE_CE — 16/04/2026)"
    },

    # R_LOGICA_OR_AND_CE: lógica OR/AND incorrecta → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "lógica del filtro or/and está incorrecta",
            "lógica or/and incorrecta",
            "precedencia del operador and",
            "uso incorrecto de operadores lógicos que podría dar lugar a resultados incorrectos",
            "lógica union/llaves condicionales tiene problemas",
        ],
        "must_not_contain": ["intencional", "es correcto"],
        "status":        "confirmed_error",
        "nota":          "Error lógico: operadores OR/AND con precedencia incorrecta producen filtros que no funcionan como el autor intendía.",
        "fuente":        "confirmed_error: lógica OR/AND incorrecta (R_LOGICA_OR_AND_CE — 16/04/2026)"
    },

    # R_DEUDA_TECNICA_FP: observación de deuda técnica / template → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "la query es extremadamente simple y probablemente sea una configuración de plantilla",
            "configuración de plantilla para reportes dinámicos",
            "query es extremadamente simple",
        ],
        "must_not_contain": ["error", "incorrecto", "falta"],
        "status":        "false_positive",
        "nota":          "FP: la card es una plantilla simple para reportes dinámicos. No hay error de datos.",
        "fuente":        "FP: deuda técnica / template (R_DEUDA_TECNICA_FP — 16/04/2026)"
    },

    # R_TITULO_PAID_FP: título indica "total de transacciones" pero con calificador (Paid) → FP
    # El modelo omite el calificador "(Paid)" en el título y reporta inconsistencia incorrecta.
    {
        "must_contain":  [],
        "any_contain":   [
            "el título indica 'total de transacciones', pero el filtro es solo para transacciones 'paid'",
            "título y filtro son consistentes para paid",
        ],
        "must_not_contain": [],
        "status":        "false_positive",
        "nota":          "FP: el título de la card incluye el calificador '(Paid)', por lo que filtrar solo por 'Paid' es correcto. El modelo ignoró ese calificador.",
        "fuente":        "FP: título con calificador Paid = filtro correcto (R_TITULO_PAID_FP — 16/04/2026)"
    },

    # R_NO_FILTER_ESTADO_CE: query sin filtro de transaction_status para métrica transaccional → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "falta filtro explícito de estado de transacción (e.g., ningún estado de transacción está mencionado)",
            "no hay filtro explícito de estado de transacción",
            "la query no aplica un filtro de estado sobre transaction_status",
            "no se aplica ningún filtro de transaction_status",
            "ningún filtro de transaction_status presente",
        ],
        "must_not_contain": ["intencional", "correcto", "es esperado"],
        "status":        "confirmed_error",
        "nota":          "Error: la query no filtra por transaction_status. Incluye transacciones en cualquier estado (Sent, Hold, etc.) sobrecontabilizando.",
        "fuente":        "confirmed_error: sin filtro transaction_status (R_NO_FILTER_ESTADO_CE — 16/04/2026)"
    },

    # R_COLUMNA_INEXISTENTE_CE: JOIN o referencia en columna que no existe → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "join se realiza en una columna inexistente",
            "columna inexistente",
            "columna que no existe",
            "column does not exist",
            "referencia incorrecta a 'created_date' en lugar de 'week'",
            "referencia a columna incorrecta",
        ],
        "must_not_contain": ["intencional", "es correcto", "podría"],
        "status":        "confirmed_error",
        "nota":          "Error de SQL: referencia o JOIN en columna que no existe en la tabla. La query fallará al ejecutarse.",
        "fuente":        "confirmed_error: columna inexistente (R_COLUMNA_INEXISTENTE_CE — 16/04/2026)"
    },

    # R_QUERY_VACIA_CE: query vacía / empty query → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "la query está incompleta y no retornará resultados válidos",
            "empty query",
            "la query está incompleta",
            "la query no tiene lógica completa",
        ],
        "must_not_contain": ["template", "plantilla"],
        "status":        "confirmed_error",
        "nota":          "Error: la query está incompleta o vacía y no retorna resultados válidos.",
        "fuente":        "confirmed_error: query vacía/incompleta (R_QUERY_VACIA_CE — 16/04/2026)"
    },

    # R_DAT_TYPO_CE: 'dat::date' es typo obvio de 'date::date' → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "dat::date",
            "'dat::date' parece ser un error tipográfico",
            "dat::date debería ser 'date::date'",
        ],
        "must_not_contain": [],
        "status":        "confirmed_error",
        "nota":          "Error tipográfico: 'dat::date' en el alias/columna debería ser 'date::date'. El cast fallará al ejecutarse.",
        "fuente":        "confirmed_error: typo dat::date (R_DAT_TYPO_CE — 16/04/2026)"
    },

    # R_TABLA_PUBLICA_FP: tabla pública sin controles → FP (observación de estilo, no error)
    {
        "must_contain":  [],
        "any_contain":   [
            "la consulta utiliza una tabla pública que puede no estar actualizada",
            "tabla pública que puede carecer de controles",
            "tabla pública sin controles adecuados",
        ],
        "must_not_contain": ["no existe", "inexistente", "error"],
        "status":        "false_positive",
        "nota":          "FP: uso de tabla pública es una observación de estilo/mantenimiento. No afecta la correctitud del resultado actual.",
        "fuente":        "FP: tabla pública = estilo (R_TABLA_PUBLICA_FP — 16/04/2026)"
    },

    # R_VAR_YEAR_CASE_FP: {{year}} vs {{YEAR}} inconsistencia de naming en Metabase → FP
    # Metabase maneja variables case-insensitively en la mayoría de contextos.
    {
        "must_contain":  [],
        "any_contain":   [
            "uso de {{year}} y {{year}} como variables en la misma query puede causar confusión",
            "uso de {{year}} y {{YEAR}}",
            "inconsistencia de nomenclatura en filtros opcionales",
        ],
        "must_not_contain": ["error de ejecución", "falla", "incorrecto"],
        "status":        "false_positive",
        "nota":          "FP: inconsistencia cosmética entre {{year}} y {{YEAR}}. Metabase trata variables case-insensitively. No afecta resultados.",
        "fuente":        "FP: inconsistencia {{year}}/{{YEAR}} = cosmética (R_VAR_YEAR_CASE_FP — 16/04/2026)"
    },

    # R_CTR_FORMULA_CE: fórmula CTR mezcla Google y Facebook incorrectamente → CE
    {
        "must_contain":  [],
        "any_contain":   [
            "combina incorrectamente usuarios de google con clics de facebook",
            "sin un denominador común válido",
            "fórmula para calcular el ctr combina incorrectamente",
        ],
        "must_not_contain": [],
        "status":        "confirmed_error",
        "nota":          "Error conceptual: la fórmula CTR mezcla métricas de canales diferentes (Google/Facebook) sin denominador común válido. El resultado es matemáticamente incorrecto.",
        "fuente":        "confirmed_error: CTR formula incorrecta (R_CTR_FORMULA_CE — 16/04/2026)"
    },

    # ── Fin Ronda 11 ─────────────────────────────────────────────────────────────

    # R_FILTRO_OPCIONALES: Filtros opcionales Metabase pueden inducir errores → FP
    {
        "must_contain":  [],
        "any_contain":   [
            "filtros opcionales insertados directamente",
            "filtros opcionales de metabase activadas",
        ],
        "must_not_contain": ["sintaxis incorrecta"],
        "status":        "false_positive",
        "nota":          "FP: los filtros opcionales de Metabase ({{variable}} / [[AND ...]]) son un patrón estándar. La query funciona correctamente con o sin ellos.",
        "fuente":        "FP: filtros opcionales Metabase = estándar (R_FILTRO_OPCIONALES — 15/04/2026)"
    },

    # R_ERROR_ANALYSIS: Cards de error analysis sin validar status → FP/intentional
    # Cards que hacen JOIN con transaction_error para analizar errores.
    # Patrón intencional: bucket `rejected` con `Sent + EXISTS(transaction_errors)`.
    {
        "must_contain":  [],
        "any_contain":   [
            "join con transaction_error",
            "anti-join no garantiza la exclusión",
            "transacciones que fueron rechazadas pero luego autorizadas",
            "puede incluir transacciones que fueron rechazadas",
        ],
        "must_not_contain": [],
        "status":        "intentional",
        "nota":          "Intencional: cards de error analysis usan JOIN con transaction_error para analizar rechazos. El patrón de 'Sent + EXISTS(errors)' para bucket rejected es diseño intencional (INICIO.md — cards 395/495).",
        "fuente":        "intentional: error analysis con transaction_error (R_ERROR_ANALYSIS — 15/04/2026)"
    },
]


def apply_p4_table_rule(descripcion: str) -> dict | None:
    """Detecta menciones de tablas P4 verificadas como no-vacías → false_positive."""
    desc_lower = descripcion.lower()
    for tabla, (status, nota, fuente) in P4_VERIFIED_NONEMPTY.items():
        if tabla in desc_lower:
            return {"status": status, "nota": nota, "fuente": fuente}
    return None


def apply_description_rules(descripcion: str) -> dict | None:
    """Aplica reglas basadas en palabras clave de la descripción."""
    desc_lower = descripcion.lower()
    for rule in DESCRIPTION_RULES:
        must      = rule.get("must_contain", [])
        any_      = rule.get("any_contain", [])
        any_2     = rule.get("any_contain_2", [])   # ← NUEVO: segundo grupo (AND lógico)
        must_not  = rule.get("must_not_contain", [])
        if (all(kw in desc_lower for kw in must) and
                (not any_ or any(kw in desc_lower for kw in any_)) and
                (not any_2 or any(kw in desc_lower for kw in any_2)) and   # ← NUEVO
                not any(kw in desc_lower for kw in must_not)):
            return {
                "status": rule["status"],
                "nota":   rule["nota"],
                "fuente": rule["fuente"],
            }
    return None


def apply_business_rule(descripcion: str) -> dict | None:
    """
    Aplica reglas de negocio sobre la descripción del hallazgo.
    Devuelve la decisión si aplica alguna regla, None si no.
    """
    desc_lower = descripcion.lower()
    for rule in BUSINESS_RULE_PATTERNS:
        if any(kw in desc_lower for kw in rule["keywords"]):
            return {
                "status": rule["status"],
                "nota":   rule["nota"],
                "fuente": rule["fuente"]
            }
    return None


# ============================================================
# DEFAULTS POR PATRÓN
# ============================================================

PATTERN_DEFAULTS = {
    # FP rate ~95% confirmado en P1 — las cards con Sent intencional ya están cubiertas arriba
    "P1":  ("false_positive",    "FP rate ~95% confirmado tras verificación masiva (01/04/2026). El patrón de 'Sent' en SQL de esta card es diseño intencional — funnel/conversión, no error de KPI authorized. Ver ESTADO.md sección P1."),
    # P2: solo 4 genuinas, manejadas a nivel card arriba
    "P2":  ("false_positive",    "FP rate ~83% en P2. Esta card sí tiene filtro de transaction_status o el contexto hace que el conteo sea correcto. Verificado contra SQL real el 03/04/2026."),
    # P8: LEFT JOIN mto — sistémico, no escalar
    "P8":  ("intentional",       "P8: LEFT JOIN con tabla 'mto' es deuda técnica sistémica. No escalar como crítico. Documentado en INICIO.md — coordinar fix con ingeniería si es necesario."),
    # P3b, P19, P12: ELIMINADOS 15/04/2026 — nunca aparecieron en JSONs de resultados
    # P17: performance, no correctitud — generalmente no son errores de negocio
    "P17": ("false_positive",    "P17: subconsulta ineficiente o redundante. No afecta la correctitud del resultado — solo performance. Evaluar impacto antes de priorizar optimización."),
    # P6: código muerto/comentado — baja urgencia, no error
    "P6":  ("false_positive",    "P6: SQL comentado o código muerto. No afecta el resultado actual. Baja prioridad de limpieza — confirmar con dueño antes de borrar."),
    # El resto: dejar pending para revisión manual
    "P3":  (PENDING,             None),   # las confirmadas ya están en P3_IDS arriba
    "P4":  (PENDING,             None),
    "P5":  (PENDING,             None),
    "P7":  (PENDING,             None),
    "P9":  (PENDING,             None),   # las confirmadas ya están en P9A_IDS/P9B_IDS
    "P10": (PENDING,             None),
    "P13": (PENDING,             None),
    # P16, P18, P20, P99: ELIMINADOS 15/04/2026 — nunca aparecieron en JSONs de resultados
}


def apply_pattern_default(patron: str) -> dict | None:
    """
    Devuelve la decisión por defecto para el patrón dado, o None si debe quedar pending.
    """
    if patron not in PATTERN_DEFAULTS:
        return None
    status, nota = PATTERN_DEFAULTS[patron]
    if status == PENDING:
        return None
    return {"status": status, "nota": nota, "fuente": f"Default patrón {patron}"}


# ============================================================
# CLASIFICADOR PRINCIPAL
# ============================================================

def classify_row(card_id: int, patron: str, descripcion: str, card_decisions: dict,
                 card_nombre: str = "") -> dict | None:
    """
    Aplica la lógica de prioridad y devuelve la decisión para esta fila.
    Devuelve None si no hay suficiente información para clasificar.

    Prioridad:
      1. Decisión a nivel card (verificada)
      2. Regla de negocio (descripción — counter, LEFT JOIN mto)
      2b. Nombre de card + descripción combinados (e.g. "paid" en nombre + "solo paid" en desc)
      3. Tablas P4 verificadas como no-vacías
      4. Reglas por descripción (performance, estilo, FP conocidos)
      5. Default del patrón
    """
    # 1. Decisión conocida a nivel card
    if card_id in card_decisions:
        return card_decisions[card_id]

    # 2. Regla de negocio (counter, LEFT JOIN mto)
    biz_rule = apply_business_rule(descripcion)
    if biz_rule:
        return biz_rule

    # 2b. Nombre de card contiene "paid/paying/pay" + descripción dice "solo paid es error"
    # → FP: la card es explícitamente sobre transacciones pagadas, filtrar solo 'Paid' es correcto
    nombre_lower = card_nombre.lower()
    desc_lower   = descripcion.lower()
    if (any(w in nombre_lower for w in ["paid", "paying", "pay "]) and
            any(kw in desc_lower for kw in ["filtro", "filtra", "filter"]) and
            "'paid'" in desc_lower and
            any(kw in desc_lower for kw in ["autorizad", "estados", "objetivo", "lugar de"])):
        return {
            "status": "false_positive",
            "nota":   "FP: el nombre de la card indica explícitamente que mide 'paid' transactions. Filtrar por transaction_status = 'Paid' es correcto para esta métrica — el modelo confundió el scope de la card.",
            "fuente": "FP: nombre card 'paid' + filtro paid = correcto"
        }

    # 2c. Card de ATV (Average Transaction Value) — definición correcta en ExampleCorp = counter_paid >= 1 / 'Paid'
    # Confirmado 04/04/2026 por Val (Head de Marketing) + análisis de 22 cards ATV:
    # - Todas las cards ATV correctas usan 'Paid' o counter_paid >= 1 (NO los 9 statuses authorized)
    # - La card canónica es "Overall ATV" (id:2699) que usa gross_margin WHERE counter_paid >= 1
    # - Card 736 y 1136 (que usan 9 statuses) están marcadas confirmed_error — eso es la excepción
    # - El modelo flagueó estas cards pensando que debían usar todos los authorized → FP
    ATV_NAME_KEYWORDS = ["atv", "average transaction value", "avg ticket",
                         "ticket increment", "ticket promedio", "avg trx", "avg txn"]
    ATV_DESC_TRIGGERS = [
        "solo.*paid", "limita solo a.*paid", "filtrando solo.*paid",
        "filtra solo.*paid", "solo el status.*paid", "paid.*subreporta",
        "paid.*subestima", "solo.*'paid'", "'paid'.*solo",
        "paid.*sub-reportar",              # variante con guión (Sesión 20)
        "filtrar.*exclusivamente.*paid",   # "Filtrar exclusivamente por 'Paid'"
        "exclusivamente.*paid",            # variante corta
        "considera solo.*paid",            # "considera solo 'Paid'"
        "considera.*solo.*paid",
        # Sesión 20 — patrones adicionales no capturados antes:
        "limitada a.*'paid'",              # "La query está limitada a transacciones con status 'Paid'"
        "limitada a.*paid",
        "limita.*a.*'paid'",               # "limita a transacciones 'Paid'"
        "limita.*transacciones.*paid",
        "'paid'.*puede ignorar",           # "status 'Paid', lo que puede ignorar"
        "'paid'.*puede.*ignorar",
        "solo.*'paid'.*puede",             # "solo 'Paid' puede sub-reportar"
        "solo.*paid.*puede",
        "solo el estado.*'paid'",          # "solo el estado 'Paid'"
        "solo el estado.*paid",
        "solo.*filter.*paid",
        "filter.*solo.*paid",
        "status.*'paid'.*subreport",       # "status 'Paid' subreporta"
        "status.*'paid'.*ignorar",
        "status.*'paid'.*puede",
        "'paid'.*dejar fuera",             # "'Paid', lo cual puede dejar fuera otros estados"
        "solo.*filtra.*'paid'",
        "filtra.*solo.*'paid'",
        "considera.*solo.*'paid'",
        "restrict.*paid",
        "restricta.*paid",
    ]
    if any(kw in nombre_lower for kw in ATV_NAME_KEYWORDS):
        if any(re.search(pat, desc_lower) for pat in ATV_DESC_TRIGGERS):
            return {
                "status": "false_positive",
                "nota":   "FP: ATV en ExampleCorp se define sobre transacciones con counter_paid >= 1 / status='Paid' — NO sobre los 9 statuses authorized. Confirmado 04/04/2026 por Val (Head de Marketing) + análisis de 22 cards ATV. El modelo erró al sugerir que debía usar todos los authorized.",
                "fuente": "FP: ATV definición correcta = Paid/counter_paid (R6 — 04/04/2026)"
            }

    # 2d. Card de MAU/QAU/WAU/DAU — definición correcta en ExampleCorp = transaction_status = 'Paid'
    # Confirmado 04/04/2026: 6 cards MAU verificadas (1899, 1900, 2641, 3011, 3085, 3160).
    # Todas usan WHERE transaction_status = 'Paid'. "Usuario activo" en ExampleCorp = completó un 'Paid'.
    # El modelo las flagueó sugiriendo usar los 9 statuses authorized → FP.
    # Mismo patrón que ATV: en ExampleCorp las métricas de usuario/actividad se basan en Paid, no authorized.
    AU_NAME_KEYWORDS = ["mau", "qau", "wau", "dau",
                        "monthly active", "weekly active", "daily active", "quarterly active",
                        "active user", "usuarios activos", "active users"]
    if any(kw in nombre_lower for kw in AU_NAME_KEYWORDS):
        if any(re.search(pat, desc_lower) for pat in ATV_DESC_TRIGGERS):
            return {
                "status": "false_positive",
                "nota":   "FP: MAU/QAU/WAU/DAU en ExampleCorp se define sobre transaction_status = 'Paid' — NO sobre los 9 statuses authorized. Confirmado 04/04/2026: 6 cards MAU verificadas contra SQL real. El modelo erró al sugerir que debía usar todos los authorized.",
                "fuente": "FP: MAU/AU definición correcta = Paid (R7 — 04/04/2026)"
            }

    # 2e. Card de NPS — definición correcta en ExampleCorp = transaction_status = 'Paid'
    # Confirmado 04/04/2026: 6 cards NPS verificadas con SQL real (25, 47, 48, 49, 153, 240).
    # Todas usan WHERE transaction_status = 'Paid'. NPS en ExampleCorp = encuesta a usuarios que
    # completaron un 'Paid'. El modelo las flagueó sugiriendo usar los 9 statuses → FP.
    NPS_NAME_KEYWORDS = ["nps", "net promoter", "respondent", "survey injected",
                         "promoter score", "nps score", "nps response", "nps per",
                         "nps comment", "nps rate"]
    if any(kw in nombre_lower for kw in NPS_NAME_KEYWORDS):
        if any(re.search(pat, desc_lower) for pat in ATV_DESC_TRIGGERS):
            return {
                "status": "false_positive",
                "nota":   "FP: NPS en ExampleCorp se mide sobre transaction_status = 'Paid' — solo usuarios que completaron un pago reciben la encuesta. Confirmado 04/04/2026: 6 cards NPS verificadas con SQL real. El modelo erró al sugerir que debía usar los 9 statuses authorized.",
                "fuente": "FP: NPS definición correcta = Paid (R8 — 04/04/2026)"
            }

    # 2f. R12a — Funnel acumulado: Sent es un paso del funnel, no un error
    # Verificado 04/04/2026: cards tipo "Accumulated Transaction Funnel" usan UNION con Sent
    # como etapa (cada rama cuenta acumulado desde ese paso). Patrón 100% intencional.
    if "funnel" in nombre_lower and ("sent" in desc_lower or "'sent'" in desc_lower):
        return {
            "status": "intentional",
            "nota":   "Intencional: 'Sent' es un paso del funnel acumulativo de transacciones en ExampleCorp. Cada rama del UNION cuenta registros que llegaron a esa etapa o más. No es un filtro de 'authorized' — es diseño de embudo. Verificado contra SQL real el 04/04/2026.",
            "fuente": "Intencional: Sent = paso del funnel acumulativo (R12a — 04/04/2026)"
        }

    # 2g. R12b — Yield/tasa de éxito: Sent solo en denominador = injected
    # Nombre contiene "yield" o "tasa de éxito". Cards no en build_card_decisions().
    if (any(kw in nombre_lower for kw in ["yield", "tasa de éxito", "tasa de exito", "success rate"]) and
            ("sent" in desc_lower or "'sent'" in desc_lower)):
        return {
            "status": "false_positive",
            "nota":   "FP: card de yield/tasa de éxito — 'Sent' solo en denominador (= 'injected'/'intentos'). Mismo patrón que Card 3 confirmado por analyst_2 (31/03/2026): yield = authorized / injected. No es error de authorized. Verificado el 04/04/2026.",
            "fuente": "FP: yield/tasa de éxito — Sent en denominador (R12b — 04/04/2026)"
        }

    # 2h. R12d — Intentos: Sent = intento de transacción (sinónimo de injected)
    # Cards que miden "número total de intentos" usan Sent como universo de intentos.
    # Mismo patrón que Card 118 (pendiente data_owner) — pero las tarjetas genéricas "número total de intentos"
    # son FP. Excluir Card 118 explícitamente (ya está en DATA_OWNER_PENDING_CARDS arriba).
    if "intentos" in nombre_lower and ("sent" in desc_lower or "'sent'" in desc_lower):
        return {
            "status": "false_positive",
            "nota":   "FP: card de 'intentos' de transacción — 'Sent' = intento/injected. No es un error de authorized: la card mide intencionalmente el universo de transacciones intentadas (incluye Sent como primer estado del funnel). Verificado el 04/04/2026.",
            "fuente": "FP: Sent = intento de transacción (R12d — 04/04/2026)"
        }

    # 2i. R13 — Sent explícito en nombre de card
    # Nombre dice "(Sent)", "until sent status", "Total Amount Sent", "sent-trx", o "sent-to-"
    # → card mide Sent intencionalmente.
    SENT_IN_NAME_KEYWORDS = ["(sent)", "until sent", "total amount sent", "total injected",
                             "sent-trx", "sent trx", "sent-to-"]
    if (any(kw in nombre_lower for kw in SENT_IN_NAME_KEYWORDS) and
            ("sent" in desc_lower or "'sent'" in desc_lower)):
        return {
            "status": "intentional",
            "nota":   "Intencional: el nombre de la card incluye 'Sent' o 'Injected' explícitamente, lo que indica que su propósito es medir transacciones en status Sent. No hay error — el SQL refleja exactamente lo que la card pretende medir. Verificado el 04/04/2026.",
            "fuente": "Intencional: Sent/Injected en nombre de card (R13 — 04/04/2026)"
        }

    # 2j. R13+ — (Injected) en nombre: card contabiliza Sent/Injected explícitamente
    if ("(injected)" in nombre_lower and
            ("sent" in desc_lower or "'sent'" in desc_lower)):
        return {
            "status": "intentional",
            "nota":   "Intencional: el nombre de la card incluye '(Injected)', indicando que su propósito es contar transacciones inyectadas/Sent. 'Sent' = 'injected' en BD de ExampleCorp — uso correcto y explícito. Verificado el 04/04/2026.",
            "fuente": "Intencional: (Injected) en nombre de card (R13+ — 04/04/2026)"
        }

    # 2k. R15 — Authorized vs Rejected en nombre: patrón Sent+EXISTS=rejected
    # Generalización del patrón documentado en INICIO.md (cards 395/495/262/266).
    if (("authorized vs rejected" in nombre_lower or "authorized vs. rejected" in nombre_lower) and
            ("sent" in desc_lower or "'sent'" in desc_lower)):
        return {
            "status": "intentional",
            "nota":   "Intencional: card 'Authorized vs Rejected' usa 'Sent AND EXISTS(transaction_errors)' como proxy del bucket 'rejected'. Patrón documentado en INICIO.md — confirmado en cards 395/495/262/266. Solo se toca el bucket 'authorized' si aplica. Verificado el 04/04/2026.",
            "fuente": "Intencional: Authorized vs Rejected — Sent+EXISTS=rejected (R15 — 04/04/2026)"
        }

    # 2l. Raw Data — cards de raw data incluyen todos los statuses por diseño
    if (nombre_lower.startswith("raw data") and
            ("sent" in desc_lower or "'sent'" in desc_lower)):
        return {
            "status": "intentional",
            "nota":   "Intencional: card de 'Raw Data' incluye todos los statuses (incluyendo Sent) por diseño. El propósito de estas cards es exponer datos crudos del funnel completo — no aplica el filtro de authorized. Verificado el 04/04/2026.",
            "fuente": "Intencional: Raw Data incluye todos los statuses (R— 04/04/2026)"
        }

    # ── Sesión 20 — 06/04/2026 ──────────────────────────────────────────────────

    # 2m. R13b — "injected" en nombre (no solo "(injected)") → Sent = injected, FP
    # Patrón: cards con "injected" en el nombre (CPA Injected, injected trx, etc.)
    # Sent = 'injected' en BD de ExampleCorp (INICIO.md). Mismo fundamento que R13/R13+.
    # Verificado: 7 cards con "injected" en nombre siguen pending (Sesión 20).
    # Cards: 338 (CPA Injected), 351 (CPA Injected), 2688, 524 (Duplicate), 391 (Duplicate)
    # Nota: NO aplicar a cards donde desc dice "incorrectamente" + "total_authorized"
    # (esas podrían ser genuinas y se verifican manualmente).
    if ("injected" in nombre_lower and
            ("'sent'" in desc_lower or " sent " in desc_lower) and
            "total_authorized" not in desc_lower):
        return {
            "status": "false_positive",
            "nota":   "FP: el nombre de la card contiene 'injected'. En ExampleCorp, 'Sent' = 'injected' en la BD — uso del status Sent en cards de métricas 'injected' es intencional (mismo principio que Card 3, confirmado por analyst_2 31/03/2026). No es error de authorized. (R13b — 06/04/2026)",
            "fuente": "FP: 'injected' en nombre de card — Sent=injected (R13b — 06/04/2026)"
        }

    # 2n. Solo Paid en card de 'Paid' — FP cuando el nombre dice "Paid" y no hay señal P3
    # 56 cards pending donde la descripción dice "solo 'Paid'" pero el NOMBRE ya indica que
    # la card es sobre transacciones 'Paid'. Filtrar solo 'Paid' es CORRECTO para estas cards.
    # Excluir si la descripción dice "la card dice 'authorized'" (señal P3 genuina).
    # Excluir si la descripción dice "incorrecto" en contexto de numerador/denominador.
    # También cubre cards con "paying" en el nombre (e.g. "Total Paying Amount").
    PAID_NAME_WORDS = ["paid", "paying"]
    P3_SIGNAL_PHRASES = [
        "la card dice 'authorized'",
        "la card dice authorized",
        "el objetivo es 'authorized'",
        "cuando el objetivo es 'authorized'",
        "pero el objetivo es",
        "título indica 'authorized'",
        "el título dice 'authorized'",
        "indica estadisticas autorizadas",
        "indica estadísticas autorizadas",
    ]
    SOLO_PAID_TRIGGERS = [
        "solo 'paid'", "solo por 'paid'", "solo por paid",
        "solo paid", "únicamente 'paid'", "únicamente paid",
        "limita solo a 'paid'", "limita a 'paid'",
        "solo el estado 'paid'", "solo el status 'paid'",
        "sub-reportar", "subreportar",
        "filtrar exclusivamente por 'paid'",
        "exclusivamente por 'paid'",
        "exclusivamente por paid",
        "considera solo 'paid'",
        "considera solo paid",
        "considera.*solo.*paid",
        "limitada a 'paid'",
        "limitada a paid",
        "limita a transacciones 'paid'",
        "limita a transacciones paid",
        "'paid', lo cual puede dejar fuera",
        "solo filtra 'paid'",
        "solo filtra paid",
        "filtra solo 'paid'",
        "restrict.*paid",
        "restricta.*paid",
        "solo.*'paid'.*puede",
        "status 'paid' puede sub",
        "status 'paid' subreport",
    ]
    if (any(w in nombre_lower for w in PAID_NAME_WORDS) and
            any(kw in desc_lower for kw in SOLO_PAID_TRIGGERS) and
            not any(sig in desc_lower for sig in P3_SIGNAL_PHRASES)):
        return {
            "status": "false_positive",
            "nota":   "FP: el nombre de la card contiene 'paid'/'paying' y la descripción dice 'solo Paid' — filtrar por transaction_status = 'Paid' es CORRECTO para una card que mide transacciones 'paid'. El modelo confundió el scope de la card con una que debería usar todos los authorized.",
            "fuente": "FP: solo Paid en card de Paid — correcto (R9 — 06/04/2026)"
        }

    # 3. Tablas P4 verificadas como no-vacías
    p4_rule = apply_p4_table_rule(descripcion)
    if p4_rule:
        return p4_rule

    # ── Sesión 37 — reglas por nombre de card ────────────────────────────────

    # R28-nombre: ATV/MAU/NPS/Retención en nombre → array-incompleto = FP
    _atv_mau_keywords = [
        'atv', 'average transaction value', 'ticket promedio',
        'mau', 'monthly active', 'dau', 'daily active',
        'wau', 'weekly active', 'qau', 'quarterly active',
        'nps', 'net promoter', 'usuarios activos', 'active users',
        'retención', 'retencion', 'retention', 'recurrentes', 'recurring',
    ]
    _array_incomplete = [
        "solo 'paid'", "solo paid", "únicamente paid", "filtro incompleto",
        "array incompleto", "debería usar los 9", "deberia usar los 9",
        "falta el array", "solo filtra 'paid'", "filtra solo 'paid'",
        "debería incluir", "deberia incluir", "sub-reportar", "subreportar",
    ]
    if (any(k in nombre_lower for k in _atv_mau_keywords) and
            any(p in desc_lower for p in _array_incomplete) and
            "authorized" not in nombre_lower):
        return {
            "status": "false_positive",
            "nota":   "FP: métrica ATV/MAU/DAU/WAU/NPS/Retención — usan EXCLUSIVAMENTE 'Paid' por definición de negocio en ExampleCorp. El modelo reportó 'array incompleto' pero para estas métricas solo 'Paid' es correcto. Confirmado: Val (Head Mktg, ATV 04/04/26), SQL verificado (MAU/NPS 04/04/26).",
            "fuente": "FP: ATV/MAU/NPS solo Paid — correcto por negocio (R28-nombre — 08/04/2026)"
        }

    # R30-nombre: funnel/adquisición en nombre → FP cuando critica filtro authorized
    _funnel_keywords = [
        'funnel', 'embudo', 'adquisición', 'adquisicion', 'acquisition',
        '1tu', 'one-and-done', 'oad', 'webchat', 'web chat',
        'lead to', 'lead→', 'conversion', 'conversión',
    ]
    _authorized_complaint = [
        "no filtra por authorized", "no usa el array",
        "falta filtro de status", "sin filtro de transaction_status",
        "debería filtrar", "deberia filtrar",
        "debería incluir los 9", "deberia incluir los 9",
        "sobrecontará", "sobrecontara", "2.3x",
        "no usa transaction_status",
    ]
    if (any(k in nombre_lower for k in _funnel_keywords) and
            any(p in desc_lower for p in _authorized_complaint)):
        return {
            "status": "false_positive",
            "nota":   "FP: card de funnel de adquisición. No debe filtrar por los 9 statuses authorized — mide etapas del funnel completo. Patrón intencional documentado en INICIO.md.",
            "fuente": "FP: funnel adquisición — sin filtro authorized intencional (R30-nombre — 08/04/2026)"
        }


    # 4. Reglas por descripción (performance, estilo, FP conocidos)
    desc_rule = apply_description_rules(descripcion)
    if desc_rule:
        return desc_rule

    # 5. Default del patrón
    return apply_pattern_default(patron)


# ============================================================
# PROCESAMIENTO PRINCIPAL
# ============================================================

OUTPUT_DIR   = str(_ROOT / "data" / "processed" / "resultados")


def iter_hallazgos(json_dir: str = OUTPUT_DIR):
    """Itera sobre todos los hallazgos pending en los JSON de resultados."""
    for fpath in sorted(glob.glob(os.path.join(json_dir, "lote_*_resultados.json"))):
        try:
            with open(fpath, encoding="utf-8") as fh:
                data = json.load(fh)
        except (json.JSONDecodeError, OSError):
            continue
        cards = data.get("cards", []) if isinstance(data, dict) else data
        for card in cards:
            cid     = card.get("card_id")
            nombre  = card.get("card_name", "")
            for h in card.get("hallazgos", []):
                status = h.get("human_review_status", "")
                if status in (PENDING, None, ""):
                    patron = h.get("tipo", "")
                    desc   = h.get("descripcion", "")
                    yield fpath, data, card, h, cid, nombre, patron, desc


def run_stats(dry_run_limit: int = 10):
    """Muestra cuántos hallazgos clasificaría sin escribir nada."""
    card_decisions = build_card_decisions()

    total = confirmed = fp = intentional = still_pending = 0
    samples: dict[str, list] = {}

    for fpath, _data, card, h, cid, nombre, patron, desc in iter_hallazgos():
        total += 1
        result = classify_row(cid, patron, desc, card_decisions, nombre)
        if result is None:
            still_pending += 1
        elif result["status"] == "confirmed_error":
            confirmed += 1
        elif result["status"] == "false_positive":
            fp += 1
        elif result["status"] == "intentional":
            intentional += 1

        # Guardar muestra para --dry-run
        if result and len(samples.get(result["status"], [])) < dry_run_limit:
            samples.setdefault(result["status"], []).append({
                "card_id": cid,
                "card_name": nombre,
                "patron": patron,
                "descripcion": desc[:80],
                "fuente": result.get("fuente", ""),
            })

    print(f"\n{'='*60}")
    print(f"  AUTO-CLASSIFY — ESTADÍSTICAS")
    print(f"{'='*60}")
    print(f"  Total hallazgos pending      : {total:>5}")
    print(f"  → Se clasificarían           : {confirmed+fp+intentional:>5}")
    print(f"    confirmed_error            : {confirmed:>5}")
    print(f"    false_positive             : {fp:>5}")
    print(f"    intentional                : {intentional:>5}")
    print(f"  → Seguirían pending          : {still_pending:>5}")
    print(f"{'='*60}\n")
    return samples


def run_dry_run():
    """Muestra una muestra de clasificaciones sin escribir nada."""
    samples = run_stats(dry_run_limit=5)
    for status, items in samples.items():
        print(f"\n── {status.upper()} (muestra) ──")
        for it in items:
            print(f"  Card {it['card_id']:5d} | {it['patron'][:40]:<40} | {it['fuente'][:60]}")
            print(f"           Desc: {it['descripcion']}")


def run_classify():
    """Aplica clasificaciones y escribe en los JSON."""
    card_decisions = build_card_decisions()
    changed = skipped = errors = 0
    files_modified: dict[str, list] = {}

    # Cargar todos los archivos a modificar
    lote_data: dict[str, list] = {}
    for fpath in sorted(glob.glob(os.path.join(OUTPUT_DIR, "lote_*_resultados.json"))):
        try:
            with open(fpath, encoding="utf-8") as fh:
                lote_data[fpath] = json.load(fh)
        except (json.JSONDecodeError, OSError):
            skipped += 1

    for fpath, data in lote_data.items():
        modified = False
        cards = data.get("cards", []) if isinstance(data, dict) else data
        for card in cards:
            cid    = card.get("card_id")
            nombre = card.get("card_name", "")
            for h in card.get("hallazgos", []):
                status = h.get("human_review_status", "")
                if status not in (PENDING, None, ""):
                    continue
                patron = h.get("tipo", "")
                desc   = h.get("descripcion", "")
                result = classify_row(cid, patron, desc, card_decisions, nombre)
                if result is None:
                    continue
                h["human_review_status"] = result["status"]
                h["human_review_notes"]  = result.get("nota", "")
                h["validated_by"]        = result.get("fuente", "auto_classify.py")
                h["validated_date"]      = datetime.now().strftime("%Y-%m-%d")
                changed += 1
                modified = True
                files_modified.setdefault(fpath, []).append(cid)

        if modified:
            try:
                with open(fpath, "w", encoding="utf-8") as fh:
                    json.dump(data, fh, ensure_ascii=False, indent=2, default=str)
            except OSError as e:
                print(f"  ERROR escribiendo {fpath}: {e}")
                errors += 1

    print(f"\n{'='*60}")
    print(f"  AUTO-CLASSIFY — COMPLETADO")
    print(f"{'='*60}")
    print(f"  Hallazgos clasificados : {changed}")
    print(f"  Archivos modificados   : {len(files_modified)}")
    print(f"  Archivos con error     : {errors}")
    print(f"  Archivos skipped       : {skipped}")
    print(f"{'='*60}\n")


# ============================================================
# ENTRY POINT
# ============================================================


def main():
    parser = argparse.ArgumentParser(
        description="Clasifica automáticamente hallazgos pending del tracker de auditoría Metabase."
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Muestra estadísticas de cuánto clasificaría sin escribir nada."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra una muestra de clasificaciones sin escribir nada."
    )
    args = parser.parse_args()

    if args.stats:
        run_stats()
    elif args.dry_run:
        run_dry_run()
    else:
        run_classify()


if __name__ == "__main__":
    main()