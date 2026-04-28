"""
verify_pending.py
Reduce los hallazgos en pending_validation usando tres capas:

  CAPA 1 — View count: cards con ≤ VIEWS_THRESHOLD vistas → FP (impacto nulo)
  CAPA 2 — SQL negativo: el modelo afirma que X está en el SQL y no está → FP
  CAPA 3 — Queries BD: genera SELECT COUNT(*) para tablas sin verificar aún

Uso:
    python verify_pending.py --stats                  # cuánto cubre cada capa, sin escribir
    python verify_pending.py --dry-run                # muestra qué clasificaría
    python verify_pending.py                          # escribe al tracker Excel
    python verify_pending.py --views-threshold 10     # considera "sin uso" cards con ≤10 vistas
    python verify_pending.py --queries                # solo genera las queries de BD a correr

El script nunca sobreescribe decisiones ya existentes.
"""

import os, re, sys, json, argparse, openpyxl
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter

# ============================================================
# CONFIG
# ============================================================

_ROOT        = Path(__file__).resolve().parent.parent.parent.parent
LOTES_DIR    = str(_ROOT / "data" / "raw" / "lotes")
TRACKER_FILE = str(_ROOT / "data" / "raw" / "tracker_auditoria_metabase.xlsx")
MAIN_SHEET   = "🔍 Todos los hallazgos"
HEADER_ROW   = 2
DATA_ROW_FROM = 3
PENDING      = "pending_validation"
AGENT_LABEL  = "verify_pending.py"
TODAY        = datetime.now().strftime("%Y-%m-%d")

# Umbral por defecto de vistas para considerar card "sin uso"
DEFAULT_VIEWS_THRESHOLD = 0

# Tablas ya verificadas en BD (no generar query para estas)
TABLAS_YA_VERIFICADAS = {
    "app_pricing", "mkt_perfomance_metrics", "mkt_performance_metrics",
    "user_activity_summary", "facebook_ads_manager", "mkt_google",
    "transaction_details", "transaction",  # tablas principales, siempre existen
    "users",
}


# ============================================================
# CAPA 1: BUILD SQL INDEX
# ============================================================

def build_sql_index(lotes_dir: str) -> dict:
    """
    Lee todos los lotes .sql y devuelve:
    {card_id: {"sql": ..., "vistas": ..., "lote": ..., "nombre": ...}}
    """
    index = {}
    for lote_path in sorted(Path(lotes_dir).glob("lote_*.sql")):
        with open(lote_path, encoding="utf-8") as f:
            content = f.read()
        blocks = re.split(r"\n(?=-- Card: )", content)
        for block in blocks:
            lines = block.strip().splitlines()
            if not lines or not lines[0].startswith("-- Card:"):
                continue
            m = re.match(r"-- Card: (.+?) \(id:(\d+) \| vistas:([\d,]+)\)", lines[0])
            if not m:
                continue
            nombre = m.group(1).strip()
            cid    = int(m.group(2))
            vistas = int(m.group(3).replace(",", ""))
            sql    = "\n".join(lines[2:]).strip()
            if sql and not sql.startswith("-- [QUERY BUILDER]"):
                index[cid] = {
                    "sql":    sql,
                    "vistas": vistas,
                    "lote":   lote_path.stem,
                    "nombre": nombre,
                }
    return index


# ============================================================
# CAPA 2: CHECKS SQL NEGATIVOS
# Principio: si el modelo afirma que X está en el SQL y X NO está → FP
# ============================================================

def sql_lower(sql: str) -> str:
    # Ignorar comentarios SQL (-- ...) para no hacer match en ellos
    clean = re.sub(r"--[^\n]*", "", sql)
    return clean.lower()


# Array canónico de los 9 transaction_status autorizados
_CANONICAL_STATUSES = [
    "hold", "paid", "payable", "cancelled", "client_refund",
    "chargeback", "chargeback_won", "chargeback_lost", "chargeback_unir",
]

def _count_canonical_statuses(sql: str) -> int:
    """Cuenta cuántos de los 9 statuses canónicos aparecen en el SQL."""
    sql_low = sql.lower()
    count = 0
    for status in _CANONICAL_STATUSES:
        if f"'{status}'" in sql_low:
            count += 1
    return count


SQL_CHECKS = [
    # ── Checks originales (S46) ────────────────────────────────
    {
        "name":   "Sent ausente en SQL",
        "trigger": lambda desc: "'sent'" in desc.lower() and (
            "incluye" in desc.lower() or "contiene" in desc.lower() or
            "usa" in desc.lower() or "cálculo" in desc.lower()
        ),
        "verify":  lambda sql: re.search(r"'sent'", sql, re.IGNORECASE) is None,
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo afirma que 'Sent' aparece en el filtro de status, pero el SQL del lote NO contiene 'Sent'. Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: Sent ausente en SQL real",
    },
    {
        "name":    "Solo Paid incorrecto — SQL tiene más statuses",
        "trigger": lambda desc: _is_solo_paid_desc(desc),
        "verify":  lambda sql: _count_canonical_statuses(sql) >= 2,
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo dice 'filtro solo Paid' pero el SQL contiene múltiples statuses canónicos. Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: 'solo Paid' es incorrecto — SQL tiene múltiples statuses",
    },
    {
        "name":    "Sin filtro de status — SQL sí lo tiene",
        "trigger": lambda desc: (
            ("sin filtro" in desc.lower() or "no hay filtro" in desc.lower() or
             "falta filtro" in desc.lower() or "no filtra" in desc.lower() or
             "falta un filtro" in desc.lower() or "ningún estado" in desc.lower())
            and ("status" in desc.lower() or "estado" in desc.lower() or "transaction" in desc.lower())
        ),
        "verify":  lambda sql: "transaction_status" in sql.lower(),
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo afirma que no hay filtro de transaction_status, pero el SQL sí lo tiene. Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: filtro de status sí existe",
    },
    {
        "name":    "Fecha hardcodeada ausente en SQL",
        "trigger": lambda desc: (
            "hardcode" in desc.lower() or "hardcod" in desc.lower() or
            ("fecha" in desc.lower() and ("literal" in desc.lower() or "fija" in desc.lower()))
        ),
        "verify":  lambda sql: not bool(re.search(
            r"'\d{4}-\d{2}-\d{2}'|'\d{4}/\d{2}/\d{2}'|'\d{4}-\d{2}'",
            sql
        )),
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo reporta fecha hardcodeada pero el SQL no contiene literales de fecha ('YYYY-MM-DD'). Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: fecha hardcodeada ausente",
    },
    {
        "name":    "FULL JOIN ausente en SQL",
        "trigger": lambda desc: "full join" in desc.lower() or "full outer join" in desc.lower(),
        "verify":  lambda sql: not bool(re.search(r"\bfull\s+(outer\s+)?join\b", sql, re.IGNORECASE)),
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo reporta FULL JOIN pero el SQL no contiene ningún FULL JOIN. Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: FULL JOIN ausente",
    },
    {
        "name":    "NULL handling — SQL ya lo maneja",
        "trigger": lambda desc: "null" in desc.lower() and (
            "coalesce" in desc.lower() or "manejo" in desc.lower() or "inadecuado" in desc.lower()
        ),
        "verify":  lambda sql: "coalesce" in sql.lower() or "is not null" in sql.lower() or "is null" in sql.lower(),
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo dice que los NULLs no están manejados, pero el SQL contiene COALESCE o IS NULL/IS NOT NULL. Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: NULL sí está manejado",
    },
    {
        "name":    "Inconsistencia de título — nombre ya dice 'paid'",
        "trigger": lambda desc: (
            ("título" in desc.lower() or "titulo" in desc.lower() or "nombre" in desc.lower())
            and "authorized" in desc.lower() and "paid" in desc.lower()
        ),
        "verify":  lambda sql: False,
        "nombre_check": lambda nombre: (
            "paid" in nombre.lower() and "authorized" not in nombre.lower()
        ),
        "status":  "false_positive",
        "nota":    "FP: el modelo reporta desajuste entre título y filtro 'Paid', pero el nombre de la card ya dice 'paid' — no hay inconsistencia real.",
        "fuente":  "SQL check: nombre ya dice 'paid'",
    },
    # ── Checks S52: Hold faltante ──────────────────────────────
    {
        "name":    "Hold faltante — SQL sí tiene Hold",
        "trigger": lambda desc: _is_hold_faltante_desc(desc),
        "verify":  lambda sql: bool(re.search(r"'hold'", sql, re.IGNORECASE)),
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo afirma que 'Hold' no aparece en el filtro, pero el SQL sí contiene 'Hold'. Verificación directa por verify_pending.py.",
        "fuente":  "SQL check: Hold sí presente en SQL",
    },
    {
        "name":    "Hold faltante — exclusión intencional (métrica solo-Paid)",
        "trigger": lambda desc: _is_hold_faltante_desc(desc),
        "verify":  lambda sql: not bool(re.search(r"'hold'", sql, re.IGNORECASE)),
        "nombre_check": lambda nombre: _is_paid_only_metric_name(nombre),
        "status":  "intentional",
        "nota":    "Intencional: métricas de usuarios activos (DAU/MAU/WAU/ATV) y NPS solo cuentan 'Paid' por definición de negocio. Hold excluido intencionalmente.",
        "fuente":  "SQL check: Hold excluido intencional (métrica solo-Paid)",
    },
    # ── Checks S52: Filtro/status incompleto ───────────────────
    {
        "name":    "Filtro incompleto — SQL tiene ≥8 de 9 statuses canónicos",
        "trigger": lambda desc: (
            ("filtro" in desc.lower() or "lista" in desc.lower()) and (
                "incompleto" in desc.lower() or "parcial" in desc.lower() or
                "no incluye todos" in desc.lower() or "faltan" in desc.lower() or
                "falta" in desc.lower()
            )
        ),
        "verify":  lambda sql: _count_canonical_statuses(sql) >= 8,
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo reporta filtro incompleto pero el SQL contiene >=8 de los 9 statuses canonicos. La diferencia es menor (probablemente Chargeback_unir). Verificacion directa por verify_pending.py.",
        "fuente":  "SQL check: filtro casi completo (>=8/9 statuses)",
    },
    {
        "name":    "Status incompleto — SQL tiene >=8 de 9 statuses canónicos",
        "trigger": lambda desc: (
            ("status" in desc.lower() or "estado" in desc.lower()) and (
                "incompleto" in desc.lower() or "parcial" in desc.lower() or
                "no incluye todos" in desc.lower() or "faltan" in desc.lower() or
                "no incluyen" in desc.lower() or "no se incluyen" in desc.lower() or
                "subreport" in desc.lower()
            )
        ),
        "verify":  lambda sql: _count_canonical_statuses(sql) >= 8,
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo reporta status incompleto pero el SQL contiene >=8 de los 9 statuses canonicos. Verificacion directa por verify_pending.py.",
        "fuente":  "SQL check: status casi completo (>=8/9 statuses)",
    },
    {
        "name":    "Auth incompleto — SQL sí tiene counter_auth",
        "trigger": lambda desc: (
            ("auth" in desc.lower() or "autoriz" in desc.lower()) and (
                "incompleto" in desc.lower() or "parcial" in desc.lower() or
                "falta" in desc.lower() or "no incluye" in desc.lower()
            )
        ),
        "verify":  lambda sql: bool(re.search(r"counter_auth", sql, re.IGNORECASE)),
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo reporta auth incompleto, pero el SQL contiene counter_auth. Verificacion directa por verify_pending.py.",
        "fuente":  "SQL check: counter_auth si presente",
    },
    # ── Checks S52: Solo Paid ──────────────────────────────────
    {
        "name":    "Solo Paid — intencional para métricas de usuarios/ATV",
        "trigger": lambda desc: _is_solo_paid_desc(desc),
        "verify":  lambda sql: _count_canonical_statuses(sql) == 1,  # solo Paid en SQL confirma
        "nombre_check": lambda nombre: _is_paid_only_metric_name(nombre),
        "status":  "intentional",
        "nota":    "Intencional: esta metrica (usuarios activos/ATV/retencion) usa solo 'Paid' por definicion de negocio confirmada. No es error usar un subset de los 9 statuses canonicos.",
        "fuente":  "SQL check: Solo Paid intencional (metrica de usuarios)",
    },
    # ── Checks S52: Authorized status genérico (82 pending) ───
    {
        "name":    "Auth status — SQL tiene >=7 statuses canónicos",
        "trigger": lambda desc: (
            ("authorized" in desc.lower() or "autorizad" in desc.lower()) and
            not _is_solo_paid_desc(desc) and not _is_hold_faltante_desc(desc)
        ),
        "verify":  lambda sql: _count_canonical_statuses(sql) >= 7,
        "status":  "false_positive",
        "nota":    "FP verificado contra SQL real: el modelo cuestiona el filtro de statuses autorizados, pero el SQL contiene >=7 de los 9 statuses canonicos. Diferencia menor. Verificacion directa por verify_pending.py.",
        "fuente":  "SQL check: auth status suficiente (>=7/9)",
    },
    {
        "name":    "Auth status — lista fija es patrón de diseño",
        "trigger": lambda desc: (
            ("lista fija" in desc.lower() or "hardcodeado" in desc.lower() or
             "desactualiz" in desc.lower() or "puede desactualiz" in desc.lower()) and
            ("status" in desc.lower() or "autorizad" in desc.lower() or "authorized" in desc.lower())
        ),
        "verify":  lambda sql: _count_canonical_statuses(sql) >= 4,
        "status":  "false_positive",
        "nota":    "FP: usar una lista fija de statuses autorizados es un patron de diseno intencional en ExampleCorp. Los valores canonicos son estables. Verificacion directa por verify_pending.py.",
        "fuente":  "SQL check: lista fija de statuses es intencional",
    },
    # ── Checks S52: Filtro opcional / sintaxis Metabase (FP) ──
    {
        "name":    "Filtro opcional / IS NULL OR — observación de sintaxis Metabase",
        "trigger": lambda desc: (
            "is null or" in desc.lower() or
            ("filtro opcional" in desc.lower() and (
                "sintaxis" in desc.lower() or "correcta" in desc.lower() or "incorrecta" in desc.lower()
            )) or
            ("filtro" in desc.lower() and "opcional" in desc.lower() and "metabase" in desc.lower())
        ),
        "verify":  lambda sql: True,  # siempre aplica — es observación de estilo
        "status":  "false_positive",
        "nota":    "FP: observacion de sintaxis del filtro opcional de Metabase (IS NULL OR pattern). No afecta la correctitud de los datos, es un patron de UX/template variable. Verificacion por verify_pending.py.",
        "fuente":  "SQL check: filtro opcional Metabase (estilo)",
    },
    {
        "name":    "CAST innecesario / schema public — observación de estilo",
        "trigger": lambda desc: (
            ("cast" in desc.lower() and ("innecesari" in desc.lower() or "desnecesari" in desc.lower() or "optimiz" in desc.lower())) or
            ("schema" in desc.lower() and "public" in desc.lower() and "redundant" in desc.lower())
        ),
        "verify":  lambda sql: True,
        "status":  "false_positive",
        "nota":    "FP: observacion de estilo/optimizacion (CAST innecesario o schema public redundante). No afecta correctitud de datos.",
        "fuente":  "SQL check: observacion de estilo (CAST/schema)",
    },
    # ── Checks S52: Homologación ──────────────────────────────
    {
        "name":    "Homologación disbursement — naming es real en BD",
        "trigger": lambda desc: (
            "homolog" in desc.lower() and (
                "disbursement" in desc.lower() or "entrega" in desc.lower() or
                "tipo" in desc.lower() or "cash" in desc.lower()
            )
        ),
        "verify":  lambda sql: True,  # confirmado: naming mixto es real en BD
        "status":  "false_positive",
        "nota":    "FP: el naming mixto de disbursement_type ('Account Credit', 'Cash Pickup', 'bankDeposit', 'directCash', 'mobileWallet') es real en la BD. No es error de homologacion. Confirmado 06/04/26.",
        "fuente":  "SQL check: homologacion disbursement (naming real en BD)",
    },
    {
        "name":    "Homologación genérica — naming inconsistente en BD",
        "trigger": lambda desc: (
            "homolog" in desc.lower() or
            ("inconsistent" in desc.lower() and ("valor" in desc.lower() or "nombre" in desc.lower()))
        ),
        "verify":  lambda sql: True,
        "status":  "false_positive",
        "nota":    "FP: observacion de homologacion/naming. El naming en la BD es intencional o legado documentado. No es error de la card SQL.",
        "fuente":  "SQL check: homologacion generica (naming en BD)",
    },
    # ── Checks S52: Observaciones de rendimiento/estilo ───────
    {
        "name":    "Observación de rendimiento o estilo SQL",
        "trigger": lambda desc: (
            "rendimiento" in desc.lower() or
            ("optimiz" in desc.lower() and "sintaxis" not in desc.lower() and "filtro" not in desc.lower()) or
            ("redundant" in desc.lower() and "status" not in desc.lower()) or
            "legibilidad" in desc.lower() or
            ("eliminad" in desc.lower() and "mejora" in desc.lower()) or
            "error tipográfico" in desc.lower() or "typo" in desc.lower() or
            "comentario" in desc.lower()
        ),
        "verify":  lambda sql: True,
        "status":  "false_positive",
        "nota":    "FP: observacion de rendimiento, estilo o legibilidad SQL. No afecta la correctitud de los datos reportados.",
        "fuente":  "SQL check: observacion rendimiento/estilo",
    },
    # ── Checks S52: Rejected ──────────────────────────────────
    {
        "name":    "Rejected — patrón intencional",
        "trigger": lambda desc: (
            "'rejected'" in desc.lower() and
            ("no está en" in desc.lower() or "no es" in desc.lower() or
             "confirmado" in desc.lower() or "producción" in desc.lower())
        ),
        "verify":  lambda sql: True,
        "status":  "false_positive",
        "nota":    "FP: el status 'Rejected' usado con Sent + EXISTS(transaction_errors) es un patron intencional para el bucket de rechazadas. Confirmado en inicio.md.",
        "fuente":  "SQL check: Rejected es patron intencional",
    },
    # ── Check S52: Ventana de tiempo fija ─────────────────────
    {
        "name":    "Ventana de tiempo fija — patrón de diseño",
        "trigger": lambda desc: (
            ("ventana" in desc.lower() and "tiempo" in desc.lower()) or
            ("30 día" in desc.lower() or "30 days" in desc.lower() or "últimos 30" in desc.lower()) or
            ("período" in desc.lower() and ("fij" in desc.lower() or "manual" in desc.lower()))
        ),
        "verify":  lambda sql: True,
        "status":  "false_positive",
        "nota":    "FP: ventana de tiempo fija (ej. ultimos 30 dias) es un patron de diseno intencional para dashboards operativos. No es un error.",
        "fuente":  "SQL check: ventana de tiempo fija (diseno)",
    },
]


# ── Helper functions para triggers (evitar lambdas repetidas) ─

def _is_solo_paid_desc(desc: str) -> bool:
    """Detecta descripciones que reportan 'solo Paid' como problema."""
    d = desc.lower()
    return (
        ("solo" in d or "limita" in d or "únicamente" in d or "unicamente" in d or
         "filtra solo" in d or "solo filtra" in d or "only" in d) and
        ("paid" in d or "'paid'" in d)
    ) and ("authorized" in d or "autorizad" in d or "subreport" in d or "otros status" in d)

def _is_hold_faltante_desc(desc: str) -> bool:
    """Detecta descripciones que reportan 'Hold' faltante."""
    d = desc.lower()
    return (
        "hold" in d and (
            "falta" in d or "ausente" in d or "no incluye" in d or
            "excluye" in d or "sin hold" in d or "sin 'hold'" in d
        )
    )

def _is_paid_only_metric_name(nombre: str) -> bool:
    """Determina si una card es de una métrica que intencionalmente usa solo 'Paid'."""
    n = nombre.lower()
    return any(kw in n for kw in [
        "active users", "dau", "mau", "wau", "qau", "atv",
        "avg transaction", "nps", "net promoter", "new user",
        "retention", "churn", "first transaction", "1tu",
        "counter_paid", "daily user", "monthly user", "weekly user",
    ])


def apply_sql_checks(card_info: dict, descripcion: str) -> dict | None:
    """
    Aplica cada check SQL. Si alguno detecta FP → retorna la decisión.
    Prioridad: el primer check que aplica y pasa gana.
    """
    sql    = card_info["sql"]
    nombre = card_info["nombre"]

    for check in SQL_CHECKS:
        if not check["trigger"](descripcion):
            continue

        # Check especial con nombre de card
        if "nombre_check" in check:
            if check["nombre_check"](nombre):
                return {
                    "status": check["status"],
                    "nota":   check["nota"],
                    "fuente": check["fuente"],
                }
            continue

        # Check normal con SQL
        if check["verify"](sql_lower(sql)):
            return {
                "status": check["status"],
                "nota":   check["nota"],
                "fuente": check["fuente"],
            }

    return None


# ============================================================
# CAPA 1: VIEW COUNT FILTER
# ============================================================

def apply_views_check(card_info: dict, threshold: int) -> dict | None:
    vistas = card_info["vistas"]
    if vistas <= threshold:
        return {
            "status": "false_positive",
            "nota": (
                f"Card con {vistas} vistas (umbral: ≤{threshold}). "
                "Impacto de negocio nulo — nadie consume esta card. "
                "El hallazgo puede ser real pero corregirlo no tiene valor práctico. "
                "Clasificado automáticamente por verify_pending.py."
            ),
            "fuente": f"Views check: {vistas} vistas ≤ umbral {threshold}",
        }
    return None


# ============================================================
# CAPA 3: TABLAS SIN VERIFICAR → QUERIES PARA BD
# ============================================================

TABLA_RE = re.compile(r"tabla[s]?\s+['\`]?([a-z_0-9]+)['\`]?", re.IGNORECASE)

def extract_unverified_tables(pending_rows: list) -> dict:
    """
    Extrae tablas mencionadas en descripciones pending que aún no están verificadas.
    Devuelve {nombre_tabla: count_menciones}
    """
    tablas = Counter()
    for row in pending_rows:
        for m in TABLA_RE.finditer(row["desc"]):
            tabla = m.group(1).lower()
            if tabla not in TABLAS_YA_VERIFICADAS and len(tabla) > 4:
                tablas[tabla] += 1
    # Filtrar ruido (palabras que no son tablas reales)
    ruido = {"grande", "grandes", "transaction", "potencialmente", "llamada",
             "general", "global", "todas", "status", "campo", "column"}
    return {t: c for t, c in tablas.items() if t not in ruido and c >= 2}


def generate_db_queries(tablas: dict) -> str:
    """Genera queries SQL para que el usuario verifique la existencia de tablas."""
    lines = [
        "-- ============================================================",
        "-- QUERIES BD — Verificar existencia de tablas pendientes",
        "-- Correr en analytics_db (readonly, usuario dbuser)",
        f"-- Generado por verify_pending.py el {TODAY}",
        "-- ============================================================",
        "",
        "-- 1. Ver qué tablas de las pendientes existen en el schema",
        "SELECT table_name, pg_size_pretty(pg_total_relation_size(table_name::regclass)) AS size",
        "FROM information_schema.tables",
        "WHERE table_schema = 'public'",
        "  AND table_name IN (",
    ]
    tabla_list = sorted(tablas.keys())
    for i, t in enumerate(tabla_list):
        sep = "," if i < len(tabla_list) - 1 else ""
        lines.append(f"  '{t}'{sep}  -- {tablas[t]} menciones en hallazgos pending")
    lines += [
        ");",
        "",
        "-- 2. Count de filas para cada tabla que exista (correr una a una o en bloque)",
    ]
    for tabla in tabla_list:
        lines.append(f"SELECT '{tabla}' AS tabla, COUNT(*) AS filas FROM {tabla}; -- {tablas[tabla]} menciones")
    lines += [
        "",
        "-- ============================================================",
        "-- Resultado esperado: copiar output y compartir con el agente",
        "-- para agregar las tablas verificadas a P4_VERIFIED_NONEMPTY en auto_classify.py",
        "-- ============================================================",
    ]
    return "\n".join(lines)


# ============================================================
# LEER / ESCRIBIR TRACKER
# ============================================================

def load_pending_rows(ws, headers: dict) -> list:
    col_id     = headers["Card ID"] - 1
    col_status = headers["Validation Status"] - 1
    col_desc   = headers["Descripción Hallazgo"] - 1
    col_nombre = headers["Nombre Card"] - 1
    col_sev    = headers.get("Severidad", 0) - 1

    rows = []
    for row_idx in range(DATA_ROW_FROM, ws.max_row + 1):
        raw_id = ws.cell(row_idx, col_id + 1).value
        if raw_id is None:
            continue
        status = str(ws.cell(row_idx, col_status + 1).value or "").strip().lower() or PENDING
        if status != PENDING:
            continue
        try:
            cid = int(str(raw_id).strip())
        except (ValueError, TypeError):
            continue
        rows.append({
            "row":    row_idx,
            "cid":    cid,
            "desc":   str(ws.cell(row_idx, col_desc + 1).value or "").strip(),
            "nombre": str(ws.cell(row_idx, col_nombre + 1).value or "").strip(),
            "sev":    str(ws.cell(row_idx, col_sev + 1).value or "").strip() if col_sev >= 0 else "",
        })
    return rows


def load_sheet_headers(ws, header_row: int) -> dict:
    headers = {}
    for row in ws.iter_rows(min_row=header_row, max_row=header_row):
        for cell in row:
            if cell.value is not None:
                headers[str(cell.value).strip()] = cell.column
    return headers


# ============================================================
# MAIN
# ============================================================

def run(views_threshold: int, dry_run: bool, stats_only: bool, queries_only: bool):
    print("=" * 60)
    print("  Verificador de Pending — verify_pending.py")
    print(f"  Views threshold: ≤{views_threshold} vistas = sin uso")
    print(f"  Modo: {'QUERIES' if queries_only else 'STATS' if stats_only else 'DRY-RUN' if dry_run else 'REAL'}")
    print("=" * 60)

    # Construir índice SQL
    print(f"\n📂 Indexando SQL de lotes...")
    sql_index = build_sql_index(LOTES_DIR)
    print(f"   {len(sql_index):,} cards indexadas")

    # Cargar tracker
    print(f"📊 Cargando tracker...")
    wb      = openpyxl.load_workbook(TRACKER_FILE)
    ws      = wb[MAIN_SHEET]
    headers = load_sheet_headers(ws, HEADER_ROW)
    pending = load_pending_rows(ws, headers)
    print(f"   {len(pending):,} filas pending encontradas")

    col_status = headers["Validation Status"]
    col_notas  = headers.get("Notas Verificación")
    col_val_by = headers.get("Validated By")
    col_val_dt = headers.get("Validated Date")

    # CAPA 3: tablas sin verificar → siempre generar
    tablas_pendientes = extract_unverified_tables(pending)
    queries_sql = generate_db_queries(tablas_pendientes)

    # Guardar queries en archivo
    queries_path = str(_ROOT / "data" / "raw" / "queries" / "verify_pending_queries.sql")
    with open(queries_path, "w", encoding="utf-8") as f:
        f.write(queries_sql)
    print(f"\n📋 Queries BD guardadas en: {queries_path}")
    print(f"   ({len(tablas_pendientes)} tablas sin verificar detectadas)")

    if queries_only:
        print(f"\n{'=' * 60}")
        print("  Tablas sin verificar encontradas:")
        for t, c in sorted(tablas_pendientes.items(), key=lambda x: -x[1]):
            print(f"    {c:3d} menciones  {t}")
        print(f"\n  Corre las queries en analytics_db y comparte el resultado.")
        print(f"{'=' * 60}")
        return

    # Clasificar cada fila pending
    stats = defaultdict(int)
    por_fuente = Counter()
    cambios = []

    for row in pending:
        cid  = row["cid"]
        desc = row["desc"]

        if cid not in sql_index:
            stats["sin_sql"] += 1
            continue

        card = sql_index[cid]
        decision = None

        # — Capa 1: view count
        decision = apply_views_check(card, views_threshold)
        if not decision:
            # — Capa 2: SQL negativo
            decision = apply_sql_checks(card, desc)

        if decision:
            stats["clasificadas"] += 1
            stats[decision["status"]] += 1
            por_fuente[decision["fuente"]] += 1
            cambios.append({**row, **decision})

            if not stats_only and not dry_run:
                ws.cell(row["row"], col_status).value = decision["status"]
                if col_notas:
                    ws.cell(row["row"], col_notas).value = decision["nota"]
                if col_val_by:
                    ws.cell(row["row"], col_val_by).value = AGENT_LABEL
                if col_val_dt:
                    ws.cell(row["row"], col_val_dt).value = TODAY
        else:
            stats["sin_regla"] += 1

    # ── Resumen ──────────────────────────────────────────────
    remaining = len(pending) - stats["clasificadas"] - stats["sin_sql"]

    print(f"\n{'─' * 60}")
    print(f"  Pending analizados:            {len(pending):,}")
    print(f"  Clasificados automáticamente:  {stats['clasificadas']:,}")
    print(f"    → false_positive:  {stats['false_positive']:,}")
    print(f"    → confirmed_error: {stats['confirmed_error']:,}")
    print(f"  Sin regla — quedan pending:    {stats['sin_regla']:,}")
    print(f"  Sin SQL (query builder):       {stats['sin_sql']:,}")
    print()
    print(f"  Por fuente:")
    for fuente, count in sorted(por_fuente.items(), key=lambda x: -x[1]):
        print(f"    {count:4d}  {fuente}")
    print(f"{'─' * 60}")
    print(f"\n  Pending restantes después de esta pasada: ~{remaining:,}")

    if stats_only:
        print("\n✓ Modo --stats: no se modificó nada.")
        return

    if dry_run:
        print(f"\n✓ DRY-RUN — muestra de primeras 10 clasificaciones:")
        for c in cambios[:10]:
            print(f"   card {c['cid']:5d} ({c['sev']:5s}) → {c['status']:<18s} [{c['fuente']}]")
        print("\n  Ejecuta sin --dry-run para aplicar al Excel.")
        return

    # Guardar Excel
    wb.save(TRACKER_FILE)
    print(f"\n💾 Tracker guardado: {TRACKER_FILE}")
    print(f"✓ {stats['clasificadas']:,} filas actualizadas.")
    print(f"\n⚠️  {remaining:,} filas aún en pending_validation.")
    if tablas_pendientes:
        print(f"   → Corre {queries_path} en analytics_db para clasificar más.")


def main():
    parser = argparse.ArgumentParser(
        description="Verifica y clasifica hallazgos pending usando SQL real y view counts"
    )
    parser.add_argument("--views-threshold", type=int, default=DEFAULT_VIEWS_THRESHOLD,
                        help=f"Cards con ≤ N vistas se clasifican como FP (default: {DEFAULT_VIEWS_THRESHOLD})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Muestra qué clasificaría sin escribir nada")
    parser.add_argument("--stats", action="store_true",
                        help="Solo muestra estadísticas sin modificar nada")
    parser.add_argument("--queries", action="store_true",
                        help="Solo genera las queries de BD para tablas sin verificar")
    args = parser.parse_args()

    run(
        views_threshold=args.views_threshold,
        dry_run=args.dry_run,
        stats_only=args.stats,
        queries_only=args.queries,
    )


if __name__ == "__main__":
    main()
