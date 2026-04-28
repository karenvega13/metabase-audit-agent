"""
fix_masivo.py — Aplica correcciones de SQL y títulos vía API de Metabase.
Corrige los siguientes tipos de problemas:
  • P9a:      Array de 4 status (sin Hold ni Chargebacks) → reemplaza por los 9 oficiales
  • P9b:      Solo falta Chargeback_unir → lo agrega al array existente
  • P9c:      Faltan Hold Y Chargeback_unir (7/9) → reemplaza por los 9
  • P3:       Título dice "authorized" pero SQL solo filtra Paid → renombra título
  • NUEVOS (re-auditoría 2026-04-10):
      P_COMMA2:    'Payable,Paid' como string único → separar en dos valores
      P_TYPO:      Typo 'reated_date' → 'created_date' (card 977)
      P_CHARGED:   Status inválidos en Charged-Back → usar los 4 reales (card 60)
      P_OR_PREC:   OR sin paréntesis → IN() / agrupación correcta (cards 6, 324)
  • S59 (2026-04-23 — derivados de PDF Training + triaje):
      P_DAU_354:     DAU debe filtrar solo 'Paid' per PDF §02 (card 354, reemplaza IN 8 → = 'Paid')
      P_TYPO_132:    Typo 'tinjected_date' → 't.injected_date' (card 132)
      P_CREATEDAT_801: 'u.created_date' → 'u.createdat' en filtro YEAR (card 801)
      P_STATUS_2632: Array 6 statuses → 9 canónicos en raw data users auth (card 2632)

IMPORTANTE:
  - Corre siempre con --dry-run primero para revisar los cambios antes de aplicar.
  - Corre desde Windows: python fix_masivo.py --dry-run
  - El script crea un backup JSON de cada card ANTES de modificarla.
  - Genera fix_masivo_log.json con el resultado de cada operación.

USO:
  python fix_masivo.py --dry-run        # muestra cambios, NO modifica Metabase
  python fix_masivo.py                  # aplica los cambios (pide confirmación)
  python fix_masivo.py --solo-p9a       # solo aplica P9a
  python fix_masivo.py --solo-p9b       # solo aplica P9b
  python fix_masivo.py --solo-p3        # solo aplica P3 (renombrar títulos)
  python fix_masivo.py --solo-nuevos    # solo aplica fixes de re-auditoría (P_COMMA2, P_TYPO, P_CHARGED, P_OR_PREC)
  python fix_masivo.py --solo-s59       # solo aplica fixes S59 (DAU #354, #132, #801, #2632)
  python fix_masivo.py --card 198       # aplica solo a una card específica
"""

import os, sys, re, json, time, argparse
from datetime import datetime
from pathlib import Path
import requests

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
from src.utils.metabase_client import METABASE_URL, get_session_token

# ─────────────────────────────────────────────
# CONFIG — editar si cambia el entorno
# ─────────────────────────────────────────────
BACKUP_DIR   = str(_ROOT / "data" / "processed" / "resultados" / "backups")
LOG_FILE     = str(_ROOT / "data" / "processed" / "resultados" / "fix_masivo_log.json")

# ─────────────────────────────────────────────
# STATUS ARRAYS
# ─────────────────────────────────────────────
STATUS_9 = {'hold','paid','payable','cancelled','client_refund',
            'chargeback','chargeback_won','chargeback_lost','chargeback_unir'}
OLD_4 = {'paid','payable','cancelled','client_refund'}

# String canónica de los 9 status para insertar en SQL
NEW_9 = "'Hold','Paid','Payable','Cancelled','Client_refund','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'"

# ─────────────────────────────────────────────
# CARDS A CORREGIR
# ─────────────────────────────────────────────

# P9a: tienen exactamente los 4 status viejos. Fix: reemplazar por los 9.
# VERIFICADO contra SQL real el 27/03/2026 — 31/31 cards confirmadas, regex probado.
# Cards 395 y 495: "Authorized vs Rejected" — el bucket 'rejected' usa Sent+errors (intencional).
#   El bucket 'authorized' con los 4 status viejo SÍ necesita fix. Confirmado.
P9A_CARDS = [
    # (card_id, nombre_descriptivo, vistas)
    (198,  "DoD (Txs Authorized) - Goal",                            5853),
    (114,  "All Time Users (Authorized transaction)",                 5600),
    (197,  "# Recurring Users (authorized transaction)",              5518),
    (238,  "By Os v2",                                               1412),
    (596,  "Trx authorized distribution conversion days",              678),
    (97,   "raw_data_account_manager",                                 431),
    (139,  "trend revenue by accumulated",                              31),
    (199,  "DoD (Txs Authorized)",                                      15),
    (451,  "WoW - Duplicate",                                           11),
    (520,  "All Time Users (Authorized transaction) - Duplicate",       10),
    (377,  "Authorized transaction by current year - Duplicate",        10),
    (399,  "# All Users (authorized transaction) - Duplicate",          10),  # 3 ocurrencias en SQL
    (437,  "ATV PER WEEK - Duplicate",                                  10),
    (634,  "# of New Users (Acq - Authorized)",                          9),
    (665,  "# of New Users (Acq - Authorized)",                          8),
    (527,  "% of txns from Recurring Users - Duplicate",                 7),
    (394,  "# Recurring Users (authorized transaction) - Duplicate",     7),
    (395,  "Ops recurring transactions Authorized vs Rejected - Dup",    7),  # bucket authorized fix
    (462,  "raw_data_trx_authorized - Duplicate",                        7),
    (493,  "# Recurring users authorized transaction last 30d - Dup",    7),
    (495,  "Ops New users transactions Authorized vs Rejected - Dup",    7),  # bucket authorized fix
    (501,  "# of successful transactions per city receiver",             7),
    (200,  "DoD (Txs Authorized)",                                       3),
    (419,  "% of transactions from Recurring Users - Duplicate",         2),
    (189,  "Percentage of trx by bands",                                 1),
    (872,  "Total Processed Volume $",                                   1),
    (874,  "Total Fee Revenue Authorized - Duplicate",                   1),
    (878,  "Total Revenue - Duplicate",                                  1),
    (880,  "Total Fx Revenue Authorized - Duplicate",                    1),
    (912,  "Users cohort 3 with 3+ trx",                                 0),
    (973,  "Users cohort4 with 3+ trx",                                  0),
]

# P9c: tienen 7/9 status — les falta Hold Y Chargeback_unir.
# Fix: reemplazar el array de 7 por el canónico de 9.
# VERIFICADO en ESTADO.md sesión 40: confirmed_error en ambas cards.
P9C_MISSING_TWO = [
    (172, "# of authorized transactions (7/9 — falta Hold+Chargeback_unir)", 0),
    (178, "Authorized txs by band (7/9 — falta Hold+Chargeback_unir)",       0),
]

# P_COMMA: falta coma entre dos quoted strings consecutivos en el ARRAY de status.
# Card 2606: ARRAY[...'Chargeback_lost''Chargeback_unir'] → agrega coma faltante.
# VERIFICADO en ESTADO.md sesión 40: confirmed_error.
P_COMMA_FIX = [
    (2606, "falta coma en ARRAY de status"),
]

# P9b: solo falta 'Chargeback_unir'. Fix: agregarlo al array existente.
# VERIFICADO contra SQL real el 27/03/2026 — 8 cards originales + 2 de re-auditoría 10/04/26.
P9B_ONLY_UNIR = [
    (277,   "Total Authorized",              0),
    (828,   "Hold MoM stats",                0),
    (829,   "Hold WoW stats",                0),
    (830,   "hold trx stats for adquire users",    0),  # counter IN(0,1) se preserva
    (831,   "hold trx stats for recurring users",  0),  # counter NOT IN(0,1) se preserva
    (1054,  "Hold MoM stats (dup)",          0),
    (820,   "Hold MoM v2",                   0),
    (2892,  "Txns Autorizadas Hoy",          0),
    # Re-auditoría 10/04/26 — CE confirmado
    (807,   "Hold transaction over authorized",    0),
    (810,   "Hold vs authorized",                  0),
]

# P3: título dice "authorized" o "auth" pero SQL solo filtra 'Paid'
# Fix: renombrar título reemplazando la palabra
# Verificado por script el 01/04/26: 83 cards totales (5 originales + 78 nuevas)
# data_owner confirmó el fix el 27/03/26: renombrar título a 'paid', NO expandir el filtro.
P3_TITLE_FIXES = [
    # ── Originales (verificadas manualmente 27/03/26) ──
    (50,   "# of authorized transactions per funding type",
           "# of paid transactions per funding type"),
    (52,   "# of authorized transactions per state of origin",
           "# of paid transactions per state of origin"),
    (1098, "# Recurring users authorized transaction last 30 days",
           "# Recurring users paid transaction last 30 days"),
    (1136, "ATV per Week first auth trx",
           "ATV per Week first paid trx"),
    (1375, "Trends authorized users by week v2",
           "Trends paid users by week v2"),

    # ── Nuevas — verificadas por SQL el 01/04/26 ──
    # Cards de alto tráfico
    (110,  "distribution of trx by  authorized users",
           "distribution of trx by  paid users"),
    (159,  "Trends  authorized users by day",
           "Trends  paid users by day"),
    (177,  "# first transaction authorized",
           "# first transaction paid"),
    (188,  "Transaction authorized  by band",
           "Transaction paid  by band"),
    (194,  "Transaction authorized  by band in first Transaction",
           "Transaction paid  by band in first Transaction"),
    (219,  "% Transactions from User authorized per Week",
           "% Transactions from User paid per Week"),
    (253,  "Distribution of Authorized Txns & ATV",
           "Distribution of Paid Txns & ATV"),
    (285,  "Ops Authorized transaction by visa",
           "Ops Paid transaction by visa"),
    (286,  "Ops Authorized transaction by Mastercard",
           "Ops Paid transaction by Mastercard"),
    (1048, "# of authorized transactions per disbursement type",
           "# of paid transactions per disbursement type"),
    (851,  "# users to authorized goal A/B Testing - tf",
           "# users to paid goal A/B Testing - tf"),
    (780,  "# users to authorized goal A/B Testing",
           "# users to paid goal A/B Testing"),
    (116,  "trends distribution of trx  authorized and paying amount",
           "trends distribution of trx  paid and paying amount"),
    (154,  "trx authorized trends",
           "trx paid trends"),
    (232,  "Transaction groups  authorized users",
           "Transaction groups  paid users"),

    # ⚠️ "Authorized vs Rejected" — misma lógica que 395/495: solo tocar el bucket authorized
    # Título refleja que el filtro de authorized es solo 'Paid'. El bucket rejected no se toca.
    (239,  "Ops Transactions authorized vs Rejected",
           "Ops Transactions paid vs Rejected"),
    (265,  "Ops Transactions Authorized vs Rejected per week",
           "Ops Transactions Paid vs Rejected per week"),
    (267,  "Ops  recurring  transactions Authorized vs Rejected  per week",
           "Ops  recurring  transactions Paid vs Rejected  per week"),
    (939,  "Ops  recurring  transactions Authorized vs Rejected  per week",
           "Ops  recurring  transactions Paid vs Rejected  per week"),
    (1101, "Ops New users transactions Authorized vs Rejected per week",
           "Ops New users transactions Paid vs Rejected per week"),
    (1106, "Ops Transactions Authorized vs Rejected per week",
           "Ops Transactions Paid vs Rejected per week"),

    # Métricas generales
    (14,   "Total # of New Users (Authorized Transactions)",
           "Total # of New Users (Paid Transactions)"),
    (16,   "Total # of All Transactions (Authorized)",
           "Total # of All Transactions (Paid)"),
    (34,   "# of Returning Users (Authorized Transactions)",
           "# of Returning Users (Paid Transactions)"),
    (38,   "Total # of All  Users (Authorized Transactions)",
           "Total # of All  Users (Paid Transactions)"),
    (39,   "Total # of Recurring Users (Authorized Transactions)",
           "Total # of Recurring Users (Paid Transactions)"),
    (353,  "CAC Authorized",
           "CAC Paid"),
    (401,  "MoM (Txs Authorized)",
           "MoM (Txs Paid)"),
    (412,  "Average Txn Value (Authorized) - Duplicate",
           "Average Txn Value (Paid) - Duplicate"),
    (438,  "WoW (Txs Authorized)",
           "WoW (Txs Paid)"),
    (529,  "DoD (Txs Authorized)",
           "DoD (Txs Paid)"),
    (528,  "Total Fee Revenue Authorized - Duplicate",
           "Total Fee Revenue Paid - Duplicate"),
    (734,  "auth users by state of origin",
           "paid users by state of origin"),
    (950,  "Adquisitions authorized users",
           "Adquisitions paid users"),
    (990,  "# Active users   authorized  transaction  last 30 days",
           "# Active users   paid  transaction  last 30 days"),
    (1011, "Transaction authorized by band without first transaction",
           "Transaction paid by band without first transaction"),

    # Weekly/Monthly/Daily Trends — familia de retención (45d / 60d / 90d ventanas)
    (1960, "Weekly Trends | Active Users (Txns Auth) | 45d",
           "Weekly Trends | Active Users (Txns Paid) | 45d"),
    (1962, "Monthly Trends | Active Users (Txns Auth) | 45d",
           "Monthly Trends | Active Users (Txns Paid) | 45d"),
    (1968, "Weekly Trends | Inactive Users (Txns Auth) | 45d",
           "Weekly Trends | Inactive Users (Txns Paid) | 45d"),
    (1970, "Monthly Trends | Inactive Users (Txns Auth) | 45d",
           "Monthly Trends | Inactive Users (Txns Paid) | 45d"),
    (1972, "Weekly Trends | Active Users (Txns Auth) | 60d",
           "Weekly Trends | Active Users (Txns Paid) | 60d"),
    (1974, "Monthly Trends | Active Users (Txns Auth) | 60d",
           "Monthly Trends | Active Users (Txns Paid) | 60d"),
    (1976, "Weekly Trends | Inactive Users (Txns Auth) | 60d",
           "Weekly Trends | Inactive Users (Txns Paid) | 60d"),
    (1978, "Monthly Trends | Inactive Users (Txns Auth) | 60d",
           "Monthly Trends | Inactive Users (Txns Paid) | 60d"),
    (1980, "Weekly Trends | Active Users (Txns Auth) | 90d",
           "Weekly Trends | Active Users (Txns Paid) | 90d"),
    (1982, "Monthly Trends | Active Users (Txns Auth) | 90d",
           "Monthly Trends | Active Users (Txns Paid) | 90d"),
    (1984, "Weekly Trends | Inactive Users (Txns Auth) | 90d",
           "Weekly Trends | Inactive Users (Txns Paid) | 90d"),
    (1986, "Monthly Trends | Inactive Users (Txns Auth) | 90d",
           "Monthly Trends | Inactive Users (Txns Paid) | 90d"),
    (1989, "Daily Trends | Active Users (Txns Auth) | 45d",
           "Daily Trends | Active Users (Txns Paid) | 45d"),
    (1993, "Daily Trends | Active Users (Txns Auth) | 60d",
           "Daily Trends | Active Users (Txns Paid) | 60d"),
    (1994, "Daily Trends | Active Users (Txns Auth) | 90d",
           "Daily Trends | Active Users (Txns Paid) | 90d"),
    (1997, "Daily Trends | Inactive Users (Txns Auth) | 45d",
           "Daily Trends | Inactive Users (Txns Paid) | 45d"),
    (1999, "Daily Trends | Inactive Users (Txns Auth) | 60d",
           "Daily Trends | Inactive Users (Txns Paid) | 60d"),
    (2001, "Daily Trends | Active Users (Txns Auth) | 90d",
           "Daily Trends | Active Users (Txns Paid) | 90d"),
    (2003, "Inactive Users Trends (Txns Auth) | Last 60d | 60d Window",
           "Inactive Users Trends (Txns Paid) | Last 60d | 60d Window"),
    (2005, "Inactive Users Trends (Txns Auth) | Last 60d | 90d Window",
           "Inactive Users Trends (Txns Paid) | Last 60d | 90d Window"),
    (2007, "Inactive Users Trends (Txns Auth) | Last 90d | 60d Window",
           "Inactive Users Trends (Txns Paid) | Last 90d | 60d Window"),
    (2009, "Inactive Users Trends (Txns Auth) | Last 90d | 90d Window",
           "Inactive Users Trends (Txns Paid) | Last 90d | 90d Window"),

    # Duplicados
    (371,  "distribution of trx by  authorized users - Duplicate",
           "distribution of trx by  paid users - Duplicate"),
    (397,  "# adquisitions authorized trx - Duplicate",
           "# adquisitions paid trx - Duplicate"),
    (464,  "DoD (Txs Authorized) - Goal - Duplicate",
           "DoD (Txs Paid) - Goal - Duplicate"),
    (466,  "Ops Authorized transaction by visa - Duplicate",
           "Ops Paid transaction by visa - Duplicate"),
    (472,  "% Transactions from User authorized per Week with user label at specific time - Duplicate",
           "% Transactions from User paid per Week with user label at specific time - Duplicate"),
    (475,  "Ops Authorized transaction by Mastercard - Duplicate",
           "Ops Paid transaction by Mastercard - Duplicate"),
    (497,  "Ops Transactions Authorized vs Rejected per week - Duplicate",
           "Ops Transactions Paid vs Rejected per week - Duplicate"),
    (519,  "# of authorized transactions per state of origin - Duplicate",
           "# of paid transactions per state of origin - Duplicate"),
    (521,  "Distribution of Authorized Txns & ATV - Duplicate",
           "Distribution of Paid Txns & ATV - Duplicate"),
    (530,  "# first transaction authorized - Duplicate",
           "# first transaction paid - Duplicate"),
    (648,  "# of New Users (Acq - Authorized) - Duplicate",
           "# of New Users (Acq - Paid) - Duplicate"),
    (865,  "DoD (Txs Authorized)",
           "DoD (Txs Paid)"),
    (869,  "MoM (Txs Authorized)",
           "MoM (Txs Paid)"),
    (870,  "Average Txn Value (Authorized) - Duplicate",
           "Average Txn Value (Paid) - Duplicate"),
    (884,  "WoW (Txs Authorized)",
           "WoW (Txs Paid)"),
    (904,  "distribution of trx by  authorized users",
           "distribution of trx by  paid users"),
    (1041, "DoD (Txs Authorized) - Goal",
           "DoD (Txs Paid) - Goal"),
    (1043, "Ops Authorized transaction by visa",
           "Ops Paid transaction by visa"),
    (1064, "Ops Authorized transaction by Mastercard",
           "Ops Paid transaction by Mastercard"),
    (1163, "# first transaction authorized",
           "# first transaction paid"),
    (1661, "# users to authorized goal A/B Testing - tf - Duplicate",
           "# users to paid goal A/B Testing - tf - Duplicate"),

    # Cards 3173/3174 — "Authorized & Paid as % of Initiated" — título ya menciona ambos
    (3173, "1TU - Txn Authorized & Paid as a % of Initiated",
           "1TU - Txn Paid as a % of Initiated"),
    (3174, "MTU - Txn Authorized & Paid as a % of Initiated",
           "MTU - Txn Paid as a % of Initiated"),
]

# ─────────────────────────────────────────────
# NUEVOS FIXES — Re-auditoría 2026-04-10
# ─────────────────────────────────────────────

# P_COMMA2: 'Payable,Paid' como string único → 'Payable','Paid' separados.
# VERIFICADO en SQL: cards 414 y 509 tienen IN('Payable,Paid','Cancelled','Client_refund')
# El string combinado NUNCA coincide con ningún status real → gross_profit filtrado = 0.
P_COMMA2_CARDS = [
    (414, "Operating Profit margin w/o first trx - Dup"),
    (509, "Operating Profit margin first trx - Dup"),
]

# P_TYPO: typo 'reated_date' → 'created_date' en card 977.
# VERIFICADO en SQL: EXTRACT(YEAR FROM td.reated_date) → falla en runtime si se usa filtro YEAR.
P_TYPO_CARDS = [
    (977, "Hold to authorized by week — typo reated_date"),
]

# P_CHARGED: statuses incorrectos en 'Charged-Back Transactions' (card 60).
# SQL actual: IN('Client_refund','Rejected') — 'Rejected' no existe, 'Client_refund' no es chargeback.
# Fix: usar los 4 statuses reales de chargeback.
P_CHARGED_CARDS = [
    (60, "Charged-Back Transactions — statuses inválidos"),
]
CHARGEBACK_4 = "'Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'"

# P_OR_PREC: OR sin paréntesis — dos cards con patrones distintos.
# Card 6:   = 'Payable' OR = 'Paid' AND transaction_number → Payable ignora el filtro de transaction_number
# Card 324: LIKE '%facebook%' OR LIKE '%FB%' AND user_label → Facebook no filtra por user_label
P_OR_PREC_CARDS = [
    (6,   "Total # of Successful Transactions — OR precedencia"),
    (324, "Total Conversations (Meta) — OR precedencia ambassador_code"),
]

# ─────────────────────────────────────────────
# S59 FIXES — derivados de PDF Training + triaje (2026-04-23)
# ─────────────────────────────────────────────

# P_DAU_354: DAU debe filtrar solo 'Paid' per PDF Training §02.
# SQL actual: transaction_status IN ('Paid','Payable','Client_refund','Cancelled','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir')
# Fix: reemplazar por transaction_status = 'Paid'. Consistente con MAU/WAU/QAU.
P_DAU_CARDS = [
    (354, "Active users (DoD) — DAU debe ser solo 'Paid' per PDF §02"),
]

# P_TYPO_132: typo 'tinjected_date' → 't.injected_date' en filtro YEAR opcional.
# SQL actual L1526: [[AND ({{week}} IS NULL OR EXTRACT(WEEK FROM tinjected_date) = {{week}})]]
P_TYPO_TINJ_CARDS = [
    (132, "profitability by bands — typo 'tinjected_date' → 't.injected_date'"),
]

# P_CREATEDAT_801: tabla users usa 'createdat' (verificado en schema).
# Inconsistencia en filtro YEAR: usa 'u.created_date' en lugar de 'u.createdat'.
P_CREATEDAT_CARDS = [
    (801, "adquisition user first sent msg post unir — u.created_date → u.createdat"),
]

# P_STATUS_2632: array de 6 statuses, faltan Chargeback/Chargeback_won/Chargeback_lost.
# SQL actual: IN ('Hold','Paid','Payable','Client_refund','Cancelled','Chargeback_unir')
# Fix: reemplazar por los 9 canónicos. La card tiene 2 ocurrencias del array.
P_STATUS_2632_CARDS = [
    (2632, "raw data users auth — falta Chargeback/Chargeback_won/Chargeback_lost"),
]

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

load_session_token = get_session_token  # alias para compatibilidad interna

def get_headers(token):
    return {"X-Metabase-Session": token, "Content-Type": "application/json"}

def get_card(card_id, headers):
    resp = requests.get(f"{METABASE_URL}/api/card/{card_id}", headers=headers)
    resp.raise_for_status()
    return resp.json()

def update_card(card_id, payload, headers):
    resp = requests.put(f"{METABASE_URL}/api/card/{card_id}",
                        headers=headers, json=payload)
    resp.raise_for_status()
    return resp.json()

def extract_sql_from_card(card_data):
    """Extrae el SQL nativo de una card de Metabase."""
    try:
        return card_data["dataset_query"]["native"]["query"]
    except (KeyError, TypeError):
        return None

def set_sql_in_card(card_data, new_sql):
    """Devuelve payload mínimo para actualizar el SQL de una card."""
    dq = card_data["dataset_query"].copy()
    dq["native"] = dq["native"].copy()
    dq["native"]["query"] = new_sql
    return {"dataset_query": dq}

def fix_sql_p9a(sql):
    """Reemplaza IN('status',...) con los 4 status viejos por el array de 9.

    Usa regex seguro que SOLO matchea listas de quoted-identifiers (letras+guiones_bajos).
    NO captura IN(SELECT...) ni IN(0,1). Verificado contra 31 SQLs + 9 test cases.
    """
    # Safe: only matches IN( 'AlphaNumeric_words', ... ) — no subqueries, no numbers
    safe_pattern = re.compile(
        r"IN\s*\(\s*'[A-Za-z_]+'\s*(?:,\s*'[A-Za-z_]+'\s*)*\)",
        re.IGNORECASE
    )
    def replacer(m):
        snippet = m.group(0)
        sl = snippet.lower()
        has_all_4 = all(f"'{s}'" in sl for s in ['paid','payable','cancelled','client_refund'])
        has_new   = any(f"'{s}'" in sl for s in ['hold','chargeback'])
        vals = re.findall(r"'[^']*'", snippet)
        if has_all_4 and not has_new and len(vals) == 4:
            return f"IN ({NEW_9})"
        return snippet
    return safe_pattern.sub(replacer, sql)

def fix_sql_7to9(sql):
    """Reemplaza un array de status que tiene exactamente 7 de los 9 oficiales
    (le faltan Hold y Chargeback_unir) por el array canónico de 9.

    Criterio de match: el snippet IN(...) contiene los 7 status conocidos
    ('Paid','Payable','Cancelled','Client_refund','Chargeback','Chargeback_won','Chargeback_lost')
    y NO contiene 'Hold' ni 'Chargeback_unir'.
    """
    EXPECTED_7 = ['paid', 'payable', 'cancelled', 'client_refund',
                  'chargeback', 'chargeback_won', 'chargeback_lost']
    safe_pattern = re.compile(
        r"IN\s*\(\s*'[A-Za-z_]+'\s*(?:,\s*'[A-Za-z_]+'\s*)*\)",
        re.IGNORECASE
    )
    def replacer(m):
        snippet = m.group(0)
        sl = snippet.lower()
        has_all_7 = all(f"'{s}'" in sl for s in EXPECTED_7)
        missing_hold = "'hold'" not in sl
        missing_unir = "'chargeback_unir'" not in sl
        vals = re.findall(r"'[^']*'", snippet)
        if has_all_7 and missing_hold and missing_unir and len(vals) == 7:
            return f"IN ({NEW_9})"
        return snippet
    return safe_pattern.sub(replacer, sql)


def fix_sql_missing_comma(sql):
    """Agrega coma faltante entre dos quoted strings consecutivos en arrays de status.
    Corrige: 'Chargeback_lost''Chargeback_unir' → 'Chargeback_lost','Chargeback_unir'
    """
    return re.sub(r"('[A-Za-z_]+')\s*('[A-Za-z_]+')", r"\1,\2", sql)


def fix_sql_add_unir(sql):
    """Agrega 'Chargeback_unir' al array de status si no está."""
    if "'Chargeback_unir'" in sql or "'chargeback_unir'" in sql:
        return sql  # ya tiene, no tocar
    # Add before the closing ) of any status array that has Chargeback_won
    def replacer(m):
        snippet = m.group(0)
        if "'Chargeback_won'" in snippet and "'Chargeback_unir'" not in snippet:
            return snippet.rstrip(')').rstrip() + ",'Chargeback_unir')"
        return snippet
    return re.sub(r"IN\s*\([^)]{10,300}\)", replacer, sql, flags=re.IGNORECASE)

def fix_sql_comma2(sql):
    """Corrige 'Payable,Paid' (string único con coma adentro) → 'Payable','Paid'.
    Patrón: IN('Payable,Paid',...) — el string combinado nunca coincide con un status real.
    """
    return re.sub(r"'(Payable),(Paid)'", r"'\1', '\2'", sql, flags=re.IGNORECASE)


def fix_sql_typo_reated(sql):
    """Corrige typo 'reated_date' → 'created_date' (card 977).
    Usa lookbehind negativo para no tocar 'created_date' existente."""
    return re.sub(r'(?<![a-z])reated_date', 'created_date', sql)


def fix_sql_chargeback_60(sql):
    """Reemplaza IN('Client_refund','Rejected') por los 4 statuses reales de chargeback.
    'Rejected' no es un status válido; 'Client_refund' es refund de ExampleCorp, no chargeback bancario.
    """
    return re.sub(
        r"IN\s*\(\s*'Client_refund'\s*,\s*'Rejected'\s*\)",
        f"IN ({CHARGEBACK_4})",
        sql, flags=re.IGNORECASE
    )


def fix_sql_or_prec_6(sql):
    """Corrige OR sin paréntesis en card 6.
    Reemplaza:
      "transaction_status" = 'Payable' OR "transaction_status" = 'Paid'
    Por:
      "transaction_status" IN ('Payable', 'Paid')
    El AND transaction_number que sigue queda correctamente unido al IN().
    """
    # Match the full qualified field name with double-quotes as it appears in the SQL
    return re.sub(
        r'"public"\."transaction_details"\."transaction_status"\s*=\s*\'Payable\''
        r'\s+or\s+'
        r'"public"\."transaction_details"\."transaction_status"\s*=\s*\'Paid\'',
        '"public"."transaction_details"."transaction_status" IN (\'Payable\', \'Paid\')',
        sql, flags=re.IGNORECASE
    )


def fix_sql_or_prec_324(sql):
    """Corrige OR sin paréntesis en card 324.
    Reemplaza:
      ambassador_code LIKE '%facebook%' or ambassador_code LIKE '%FB%'
    Por:
      (ambassador_code LIKE '%facebook%' OR ambassador_code LIKE '%FB%')
    El AND user_label que sigue queda correctamente unido al grupo.
    """
    return re.sub(
        r"(ambassador_code LIKE '%facebook%') or (ambassador_code LIKE '%FB%')",
        r"(\1 OR \2)",
        sql
    )


def _get_or_prec_fix_fn(card_id):
    """Devuelve la función de fix correcta para cada card de P_OR_PREC."""
    return {6: fix_sql_or_prec_6, 324: fix_sql_or_prec_324}.get(card_id)


def fix_sql_dau_354(sql):
    """Reemplaza IN(8 statuses, sin Hold) por = 'Paid' en card 354 (DAU).
    Patrón exacto: 'Paid','Payable','Client_refund','Cancelled','Chargeback','Chargeback_won','Chargeback_lost','Chargeback_unir'.
    DAU debe filtrar solo 'Paid' per PDF Training §02. Consistente con MAU/WAU/QAU.
    """
    return re.sub(
        r"transaction_status\s+IN\s*\(\s*"
        r"'Paid'\s*,\s*'Payable'\s*,\s*'Client_refund'\s*,\s*'Cancelled'\s*,\s*"
        r"'Chargeback'\s*,\s*'Chargeback_won'\s*,\s*'Chargeback_lost'\s*,\s*'Chargeback_unir'"
        r"\s*\)",
        "transaction_status = 'Paid'",
        sql, flags=re.IGNORECASE
    )


def fix_sql_typo_tinjected(sql):
    """Corrige typo 'tinjected_date' → 't.injected_date' (card 132).
    Lookbehind negativo evita tocar identificadores que terminan en letra/underscore/punto."""
    return re.sub(r'(?<![A-Za-z_.])tinjected_date', 't.injected_date', sql)


def fix_sql_createdat_801(sql):
    """Normaliza 'u.created_date' → 'u.createdat' en card 801.
    Tabla users usa 'createdat' (verificado en analytics_db_schema.json).
    El resto del SQL ya usa 'u.createdat' — solo el filtro YEAR opcional tiene la inconsistencia.
    """
    return re.sub(
        r"EXTRACT\(\s*YEAR\s+FROM\s+u\.created_date\s*\)",
        "EXTRACT(YEAR FROM u.createdat)",
        sql, flags=re.IGNORECASE
    )


def fix_sql_status_2632(sql):
    """Reemplaza array de 6 statuses por los 9 canónicos en card 2632.
    Array actual: 'Hold','Paid','Payable','Client_refund','Cancelled','Chargeback_unir' — faltan 3 Chargeback*.
    La card tiene 2 ocurrencias del mismo array (CTE last3 + subquery last_paying_amount).
    """
    return re.sub(
        r"IN\s*\(\s*"
        r"'Hold'\s*,\s*'Paid'\s*,\s*'Payable'\s*,\s*'Client_refund'\s*,\s*'Cancelled'\s*,\s*'Chargeback_unir'"
        r"\s*\)",
        f"IN ({NEW_9})",
        sql, flags=re.IGNORECASE
    )


def backup_card(card_id, card_data):
    """Guarda backup JSON antes de modificar."""
    Path(BACKUP_DIR).mkdir(parents=True, exist_ok=True)
    path = Path(BACKUP_DIR) / f"card_{card_id}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    path.write_text(json.dumps(card_data, indent=2, ensure_ascii=False))
    return str(path)

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def run(dry_run, only_p9a, only_p9b, only_p9c, only_comma, only_p3, only_nuevos, only_s59, target_card):
    token = load_session_token()
    headers = get_headers(token)
    log = []

    def process_sql_fix(card_id, label, fix_fn):
        print(f"\n  [{card_id}] {label}")
        try:
            card = get_card(card_id, headers)
            sql_orig = extract_sql_from_card(card)
            if sql_orig is None:
                print(f"    ⚠️  No es card nativa (no tiene SQL). Skip.")
                log.append({"card_id": card_id, "status": "skip_no_sql"})
                return

            sql_fixed = fix_fn(sql_orig)
            if sql_fixed == sql_orig:
                print(f"    ⚠️  No se encontró el patrón esperado en el SQL. Skip.")
                log.append({"card_id": card_id, "status": "skip_pattern_not_found"})
                return

            # Show diff (first changed line)
            orig_lines  = sql_orig.split('\n')
            fixed_lines = sql_fixed.split('\n')
            for i in range(min(len(orig_lines), len(fixed_lines))):
                if orig_lines[i] != fixed_lines[i]:
                    print(f"    - {orig_lines[i].strip()[:100]}")
                    print(f"    + {fixed_lines[i].strip()[:100]}")
                    break

            if dry_run:
                print(f"    [DRY-RUN] No se aplicó el cambio.")
                log.append({"card_id": card_id, "status": "dry_run_ok"})
                return

            backup_path = backup_card(card_id, card)
            print(f"    Backup: {backup_path}")
            payload = set_sql_in_card(card, sql_fixed)
            update_card(card_id, payload, headers)
            print(f"    ✅ Actualizado en Metabase.")
            log.append({"card_id": card_id, "status": "ok", "backup": backup_path})
            time.sleep(0.3)  # throttle

        except requests.HTTPError as e:
            print(f"    ❌ HTTP Error: {e.response.status_code} {e.response.text[:200]}")
            log.append({"card_id": card_id, "status": "error", "msg": str(e)})
        except Exception as e:
            print(f"    ❌ Error: {e}")
            log.append({"card_id": card_id, "status": "error", "msg": str(e)})

    def process_title_fix(card_id, old_name, new_name):
        print(f"\n  [{card_id}] '{old_name}' → '{new_name}'")
        try:
            card = get_card(card_id, headers)
            if dry_run:
                print(f"    [DRY-RUN] No se aplicó el cambio.")
                log.append({"card_id": card_id, "status": "dry_run_ok"})
                return
            backup_path = backup_card(card_id, card)
            print(f"    Backup: {backup_path}")
            update_card(card_id, {"name": new_name}, headers)
            print(f"    ✅ Título actualizado.")
            log.append({"card_id": card_id, "status": "ok", "backup": backup_path})
            time.sleep(0.3)
        except Exception as e:
            print(f"    ❌ Error: {e}")
            log.append({"card_id": card_id, "status": "error", "msg": str(e)})

    mode = "DRY-RUN" if dry_run else "EJECUCIÓN REAL"
    print(f"\n{'='*60}")
    print(f" fix_masivo.py — {mode}")
    print(f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")

    if not dry_run:
        print("\n⚠️  ADVERTENCIA: Esto modificará cards en Metabase.")
        print("Los backups se guardarán en resultados/backups/")
        ans = input("¿Continuar? (escribe 'si' para confirmar): ")
        if ans.strip().lower() != 'si':
            print("Cancelado.")
            return

    any_solo = only_p9a or only_p9b or only_p9c or only_comma or only_p3 or only_nuevos or only_s59

    # ── P9a ──
    if not only_p9b and not only_p9c and not only_comma and not only_p3 and not only_nuevos and not only_s59:
        print(f"\n{'─'*50}")
        print(f"P9a — Array viejo 4 status → reemplazar por 9 ({len(P9A_CARDS)} cards)")
        print(f"{'─'*50}")
        for card_id, label, _ in P9A_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_p9a)

    # ── P9b ──
    if not only_p9a and not only_p9c and not only_comma and not only_p3 and not only_nuevos and not only_s59:
        print(f"\n{'─'*50}")
        print(f"P9b — Agregar Chargeback_unir ({len(P9B_ONLY_UNIR)} cards)")
        print(f"{'─'*50}")
        for card_id, label, _ in P9B_ONLY_UNIR:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_add_unir)

    # ── P9c ──
    if not only_p9a and not only_p9b and not only_comma and not only_p3 and not only_nuevos and not only_s59:
        print(f"\n{'─'*50}")
        print(f"P9c — 7/9 statuses: agregar Hold+Chargeback_unir ({len(P9C_MISSING_TWO)} cards)")
        print(f"{'─'*50}")
        for card_id, label, _ in P9C_MISSING_TWO:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_7to9)

    # ── P_COMMA ──
    if not only_p9a and not only_p9b and not only_p9c and not only_p3 and not only_nuevos and not only_s59:
        print(f"\n{'─'*50}")
        print(f"P_COMMA — Coma faltante en ARRAY ({len(P_COMMA_FIX)} cards)")
        print(f"{'─'*50}")
        for card_id, label in P_COMMA_FIX:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_missing_comma)

    # ── P3 ──
    if not only_p9a and not only_p9b and not only_p9c and not only_comma and not only_nuevos and not only_s59:
        print(f"\n{'─'*50}")
        print(f"P3 — Renombrar títulos: 'authorized' → 'paid' ({len(P3_TITLE_FIXES)} cards)")
        print(f"{'─'*50}")
        for card_id, old_name, new_name in P3_TITLE_FIXES:
            if target_card and card_id != target_card:
                continue
            process_title_fix(card_id, old_name, new_name)

    # ── NUEVOS (re-auditoría 2026-04-10) ──
    if not only_p9a and not only_p9b and not only_p9c and not only_comma and not only_p3 and not only_s59:
        print(f"\n{'─'*50}")
        print(f"NUEVOS — Re-auditoría: P_COMMA2 + P_TYPO + P_CHARGED + P_OR_PREC")
        print(f"{'─'*50}")

        print(f"\n  P_COMMA2 — 'Payable,Paid' string único ({len(P_COMMA2_CARDS)} cards)")
        for card_id, label in P_COMMA2_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_comma2)

        print(f"\n  P_TYPO — Typo reated_date ({len(P_TYPO_CARDS)} card)")
        for card_id, label in P_TYPO_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_typo_reated)

        print(f"\n  P_CHARGED — Statuses inválidos en Charged-Back ({len(P_CHARGED_CARDS)} card)")
        for card_id, label in P_CHARGED_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_chargeback_60)

        print(f"\n  P_OR_PREC — OR sin paréntesis ({len(P_OR_PREC_CARDS)} cards)")
        for card_id, label in P_OR_PREC_CARDS:
            if target_card and card_id != target_card:
                continue
            fix_fn = _get_or_prec_fix_fn(card_id)
            if fix_fn:
                process_sql_fix(card_id, label, fix_fn)

    # ── S59 (derivados de PDF Training + triaje, 2026-04-23) ──
    if not only_p9a and not only_p9b and not only_p9c and not only_comma and not only_p3 and not only_nuevos:
        print(f"\n{'─'*50}")
        print(f"S59 — DAU #354 + 3 CE leves (#132/#801/#2632)")
        print(f"{'─'*50}")

        print(f"\n  P_DAU_354 — DAU debe ser solo 'Paid' per PDF §02 ({len(P_DAU_CARDS)} card)")
        for card_id, label in P_DAU_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_dau_354)

        print(f"\n  P_TYPO_132 — Typo tinjected_date ({len(P_TYPO_TINJ_CARDS)} card)")
        for card_id, label in P_TYPO_TINJ_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_typo_tinjected)

        print(f"\n  P_CREATEDAT_801 — u.created_date → u.createdat ({len(P_CREATEDAT_CARDS)} card)")
        for card_id, label in P_CREATEDAT_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_createdat_801)

        print(f"\n  P_STATUS_2632 — 6/9 statuses → 9 canónicos ({len(P_STATUS_2632_CARDS)} card)")
        for card_id, label in P_STATUS_2632_CARDS:
            if target_card and card_id != target_card:
                continue
            process_sql_fix(card_id, label, fix_sql_status_2632)

    # ── Summary ──
    ok   = sum(1 for x in log if x['status'] in ('ok','dry_run_ok'))
    skip = sum(1 for x in log if 'skip' in x['status'])
    err  = sum(1 for x in log if x['status'] == 'error')
    print(f"\n{'='*60}")
    print(f" RESUMEN: {ok} OK · {skip} skip · {err} errores · total {len(log)}")
    print(f"{'='*60}")

    Path(LOG_FILE).write_text(json.dumps(log, indent=2, ensure_ascii=False))
    print(f"Log guardado en {LOG_FILE}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fix masivo de cards Metabase")
    parser.add_argument("--dry-run",     action="store_true", help="Muestra cambios sin aplicar")
    parser.add_argument("--solo-p9a",    action="store_true", help="Solo aplica P9a")
    parser.add_argument("--solo-p9b",    action="store_true", help="Solo aplica P9b (incluye cards 807, 810)")
    parser.add_argument("--solo-p9c",    action="store_true", help="Solo aplica P9c (7/9, falta Hold+Chargeback_unir)")
    parser.add_argument("--solo-comma",  action="store_true", help="Solo aplica fix de coma faltante (card 2606)")
    parser.add_argument("--solo-p3",     action="store_true", help="Solo aplica P3 (títulos)")
    parser.add_argument("--solo-nuevos", action="store_true",
                        help="Solo aplica fixes de re-auditoría: P_COMMA2 (414,509), P_TYPO (977), P_CHARGED (60), P_OR_PREC (6,324)")
    parser.add_argument("--solo-s59",    action="store_true",
                        help="Solo aplica fixes S59: DAU #354 + typo #132 + createdat #801 + status #2632")
    parser.add_argument("--card",        type=int,            help="Card ID específica")
    args = parser.parse_args()
    run(
        dry_run     = args.dry_run,
        only_p9a    = args.solo_p9a,
        only_p9b    = args.solo_p9b,
        only_p9c    = args.solo_p9c,
        only_comma  = args.solo_comma,
        only_p3     = args.solo_p3,
        only_nuevos = args.solo_nuevos,
        only_s59    = args.solo_s59,
        target_card = args.card,
    )
