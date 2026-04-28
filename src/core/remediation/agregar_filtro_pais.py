"""
agregar_filtro_pais.py — Agrega un filtro "Country" a los dashboards principales.

Contexto:
    ExampleCorp se expande de México (id=1) a Guatemala (id=4) y eventualmente a
    El Salvador (2), Honduras (3), United States (5). data_owner pidió un filtro de
    país en los 3 dashboards principales (Main Dashboard v2, active-users-dash,
    New-users-fka-acq-dash) con arquitectura escalable.

Estado de la columna country_id:
    Hoy vive en shipments_db pero Metabase consulta analytics_db, donde la
    columna aún no se ha propagado. Por eso table_column_map en
    config/country_filter.json arranca todo en null. El script:
      - Agrega visualmente el parámetro "Country" a los 3 dashboards (entregable
        inmediato — el dropdown aparece, listo para activarse).
      - Para cada dashcard, decide si puede mappear el filtro a una columna
        real (solo si la tabla origen está en table_column_map con valor no
        null y la card es GUI/MBQL).
      - Loguea la razón de cada skip para revisión.
    Cuando Perla propague country_id a analytics_db, basta editar
    config/country_filter.json (setear las columnas) y re-ejecutar — el script
    es idempotente: el parameter ya existente se reusa, y las cards que ya
    estaban mappeadas no se duplican.

Limitaciones de scope (confirmadas con data_owner):
    - No se modifica el SQL de cards nativas (cards SQL sin template-tag
      {{country}} quedan skipped — segunda fase con fix_masivo si aplica).
    - El filtro mappea solo a cards GUI/MBQL cuya tabla origen tenga columna
      country_id mapeada en config.

Uso:
    python -m src.core.remediation.agregar_filtro_pais --dry-run
    python -m src.core.remediation.agregar_filtro_pais --dry-run --dashboard "active-users-dash"
    python -m src.core.remediation.agregar_filtro_pais --dashboard "active-users-dash"
    python -m src.core.remediation.agregar_filtro_pais         # los 3 dashboards
"""

import sys
import json
import time
import secrets
import argparse
import re
import requests
from pathlib import Path
from datetime import datetime, timezone

_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(_ROOT))
from src.utils.metabase_client import METABASE_URL, get_session_token, make_headers

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CONFIG_FILE = _ROOT / "config" / "country_filter.json"
LOG_FILE = _ROOT / "data" / "processed" / "resultados" / "agregar_filtro_pais_log.json"
BACKUP_DIR = _ROOT / "data" / "processed" / "resultados" / "backups"


# ──────────────────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────────────────

def load_config() -> dict:
    if not CONFIG_FILE.exists():
        print(f"ERROR: no existe {CONFIG_FILE.relative_to(_ROOT)}")
        sys.exit(1)
    return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))


# ──────────────────────────────────────────────────────────────────────────
# Metabase helpers
# ──────────────────────────────────────────────────────────────────────────

def api_get(session, headers, path, **kwargs):
    r = session.get(f"{METABASE_URL}{path}", headers=headers, timeout=20, **kwargs)
    if r.status_code == 401:
        print("ERROR: 401 — sesion expirada. Renueva METABASE_SESSION en config/api_key.env")
        sys.exit(1)
    return r


def api_put(session, headers, path, payload):
    return session.put(f"{METABASE_URL}{path}", headers=headers, json=payload, timeout=30)


def resolve_dashboard_id(session, headers, name: str) -> int | None:
    """Busca un dashboard por nombre exacto vía /api/search. Falla si 0 o ambiguo."""
    r = api_get(session, headers, "/api/search", params={"q": name, "models": "dashboard"})
    if r.status_code != 200:
        print(f"  WARN: search HTTP {r.status_code} — {r.text[:200]}")
        return None
    data = r.json().get("data", [])
    matches = [d for d in data if d.get("model") == "dashboard"
               and d.get("name", "").strip() == name.strip()
               and not d.get("archived", False)]
    if len(matches) == 0:
        print(f"  ERROR: no se encontró dashboard con nombre exacto '{name}'")
        return None
    if len(matches) > 1:
        ids = [m.get("id") for m in matches]
        print(f"  ERROR: nombre ambiguo '{name}' — coinciden IDs {ids}")
        return None
    return matches[0]["id"]


def get_dashboard(session, headers, dash_id):
    r = api_get(session, headers, f"/api/dashboard/{dash_id}")
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return r.json()


def get_card(session, headers, card_id, cache):
    if card_id in cache:
        return cache[card_id]
    r = api_get(session, headers, f"/api/card/{card_id}")
    if r.status_code != 200:
        cache[card_id] = None
        return None
    cache[card_id] = r.json()
    return cache[card_id]


def get_table_metadata(session, headers, table_id, cache):
    """Devuelve metadata de tabla (incluye fields). Cachea por table_id."""
    if table_id in cache:
        return cache[table_id]
    r = api_get(session, headers, f"/api/table/{table_id}/query_metadata")
    if r.status_code != 200:
        cache[table_id] = None
        return None
    cache[table_id] = r.json()
    return cache[table_id]


# ──────────────────────────────────────────────────────────────────────────
# Parameter construction
# ──────────────────────────────────────────────────────────────────────────

def gen_param_id() -> str:
    """8-char hex id, formato Metabase."""
    return secrets.token_hex(4)


def build_country_parameter(cfg: dict, existing_id: str | None = None) -> dict:
    """Construye el dict de parameter para Metabase. Si existing_id se pasa,
    se reusa (idempotencia)."""
    p = cfg["parameter"]
    catalog = cfg["country_catalog"]
    return {
        "id": existing_id or gen_param_id(),
        "name": p["name"],
        "slug": p["slug"],
        "type": p["type"],
        "default": p.get("default"),
        "values_query_type": "list",
        "values_source_type": "static-list",
        "values_source_config": {
            # [value, label] tuples — Metabase muestra label, filtra por value
            "values": [[str(c["id"]), c["name"]] for c in catalog],
        },
    }


def ensure_country_parameter(dash_data: dict, cfg: dict) -> tuple[list, str, bool]:
    """Devuelve (lista_de_parameters_actualizada, parameter_id, was_added).
    Idempotente: si ya hay un parameter con slug='country', lo reusa y refresca
    la config (catálogo, default)."""
    slug = cfg["parameter"]["slug"]
    parameters = list(dash_data.get("parameters") or [])
    for i, p in enumerate(parameters):
        if p.get("slug") == slug:
            updated = build_country_parameter(cfg, existing_id=p["id"])
            parameters[i] = updated
            return parameters, p["id"], False
    new_param = build_country_parameter(cfg)
    parameters.append(new_param)
    return parameters, new_param["id"], True


# ──────────────────────────────────────────────────────────────────────────
# Mapping logic
# ──────────────────────────────────────────────────────────────────────────

_FROM_RE = re.compile(
    r'\b(?:FROM|JOIN)\s+(?:"[^"]+"\.)*"?(?P<table>[A-Za-z_][A-Za-z0-9_]*)"?',
    re.IGNORECASE,
)


def detect_card_tables(card_data: dict, session, headers, table_meta_cache) -> list[str]:
    """Devuelve lista de nombres de tabla que la card consulta.
    - MBQL: lee dataset_query.query.source-table → resuelve nombre vía API.
    - SQL nativo: regex sobre FROM/JOIN.
    """
    if not card_data:
        return []
    dq = card_data.get("dataset_query") or {}
    qtype = dq.get("type")
    tables: list[str] = []

    if qtype == "query":
        src = (dq.get("query") or {}).get("source-table")
        if isinstance(src, int):
            meta = get_table_metadata(session, headers, src, table_meta_cache)
            if meta and meta.get("name"):
                tables.append(meta["name"])
        # source-table puede ser "card__N" → no resoluble a tabla directa
    elif qtype == "native":
        sql = (dq.get("native") or {}).get("query") or ""
        # Quita comentarios para no matchear FROM en líneas comentadas
        sql_clean = re.sub(r"--[^\n]*", "", sql)
        sql_clean = re.sub(r"/\*.*?\*/", "", sql_clean, flags=re.DOTALL)
        for m in _FROM_RE.finditer(sql_clean):
            t = m.group("table").lower()
            if t and t not in tables:
                tables.append(t)
    return tables


def find_field_id(table_meta: dict, column_name: str) -> int | None:
    if not table_meta:
        return None
    for f in table_meta.get("fields") or []:
        if (f.get("name") or "").lower() == column_name.lower():
            return f.get("id")
    return None


# ──────────────────────────────────────────────────────────────────────────
# SQL injection helpers (para cards SQL nativas en country-aware tables)
# ──────────────────────────────────────────────────────────────────────────

_SQL_RESERVED = {"ON", "WHERE", "GROUP", "ORDER", "LEFT", "RIGHT", "INNER",
                 "JOIN", "AS", "LIMIT", "HAVING", "UNION", "CROSS", "OUTER",
                 "FULL", "NATURAL", "USING", "AND", "OR", "NOT", "WITH",
                 "SELECT", "FROM"}


def strip_sql_strings_and_comments(sql: str) -> str:
    """Reemplaza strings/comentarios por blancos (preservando posiciones).
    Útil para análisis estructural sin matchear keywords dentro de strings."""
    out = list(sql)
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        # Block comment /* ... */
        if c == "/" and i + 1 < n and sql[i + 1] == "*":
            j = sql.find("*/", i + 2)
            if j == -1:
                j = n
            else:
                j += 2
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        # Line comment --
        if c == "-" and i + 1 < n and sql[i + 1] == "-":
            j = sql.find("\n", i)
            if j == -1:
                j = n
            for k in range(i, j):
                out[k] = " "
            i = j
            continue
        # Single-quoted string
        if c == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'" and (j + 1 >= n or sql[j + 1] != "'"):
                    j += 1
                    break
                if sql[j] == "'" and j + 1 < n and sql[j + 1] == "'":
                    j += 2
                    continue
                j += 1
            for k in range(i, j):
                if out[k] != "\n":
                    out[k] = " "
            i = j
            continue
        i += 1
    return "".join(out)


def find_table_alias(sql: str, table: str) -> str:
    """Devuelve el alias usado en `FROM table alias` o `JOIN table alias`.
    Si no hay alias explícito, devuelve el nombre de tabla."""
    pattern = re.compile(
        rf'\b(?:FROM|JOIN)\s+(?:"[^"]+"\.)*(?:"{re.escape(table)}"|{re.escape(table)})\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*)\b',
        re.IGNORECASE,
    )
    for m in pattern.finditer(sql):
        alias = m.group(1)
        if alias.upper() not in _SQL_RESERVED:
            return alias
    return table


def _depth_at(cleaned: str, pos: int) -> int:
    """Calcula la profundidad de paréntesis en `pos` (top-level = 0)."""
    return cleaned.count("(", 0, pos) - cleaned.count(")", 0, pos)


def _find_country_table_scopes(cleaned: str, aware_tables: list[str]) -> list[dict]:
    """Devuelve una lista de {table, alias, from_pos, from_end, depth} para cada
    FROM/JOIN de una tabla country-aware en el SQL."""
    if not aware_tables:
        return []
    # Ordenar por longitud DESC para que la alternación regex prefiera matches
    # más largos primero (evita que "transaction" matchee donde "transaction_details"
    # debería matchear).
    sorted_tables = sorted(aware_tables, key=lambda t: -len(t))
    table_alt = "|".join(re.escape(t) for t in sorted_tables)
    # Word boundary explícito al final del nombre de tabla para no matchear prefijos.
    pat = re.compile(
        r'\b(?:FROM|JOIN)\s+(?:"[^"]+"\.)*'
        r'(?:"(' + table_alt + r')"|(' + table_alt + r')\b)'
        r'(?:\s+(?:AS\s+)?([A-Za-z_][A-Za-z0-9_]*))?',
        re.IGNORECASE,
    )
    out = []
    for m in pat.finditer(cleaned):
        table = (m.group(1) or m.group(2)).lower()
        alias_cand = m.group(3)
        if alias_cand and alias_cand.upper() in _SQL_RESERVED:
            alias_cand = None
        out.append({
            "table": table,
            "alias": alias_cand or table,
            "from_pos": m.start(),
            "from_end": m.end(),
            "depth": _depth_at(cleaned, m.start()),
        })
    return out


def _scope_end_pos(cleaned: str, start_pos: int) -> int:
    """Encuentra el final del scope abierto en start_pos (donde se cierra el `(` correspondiente,
    o end-of-string si estamos en top-level)."""
    d = 0
    for i in range(start_pos, len(cleaned)):
        c = cleaned[i]
        if c == "(":
            d += 1
        elif c == ")":
            if d == 0:
                return i
            d -= 1
    return len(cleaned)


def _find_keyword_at_depth_zero(cleaned: str, start: int, end: int,
                                pattern: re.Pattern) -> re.Match | None:
    """Encuentra primer match de `pattern` en cleaned[start:end] que esté a depth 0
    relativo al rango (ignorando matches dentro de paréntesis)."""
    d = 0
    i = start
    while i < end:
        c = cleaned[i]
        if c == "(":
            d += 1
            i += 1
            continue
        if c == ")":
            d -= 1
            i += 1
            continue
        if d == 0:
            m = pattern.match(cleaned, i)
            if m and m.end() <= end:
                return m
        i += 1
    return None


_TERMINATOR_RE = re.compile(
    r"\b(GROUP\s+BY|ORDER\s+BY|LIMIT|HAVING|WINDOW|FETCH|OFFSET|UNION|INTERSECT|EXCEPT)\b",
    re.IGNORECASE,
)
_WHERE_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)
_FROM_RE_KW = re.compile(r"\bFROM\b", re.IGNORECASE)


def inject_country_clause(sql: str, aware_tables: list[str], tag_name: str,
                          marker: str) -> tuple[str, int, str]:
    """Inyecta `[[AND <alias>.country_id::text = {{country}}]]` en cada scope
    (CTE, subquery, top-level) que contiene un FROM/JOIN de una tabla
    country-aware.

    Returns (new_sql, num_injections, reason).
    - num_injections == 0 → reason explica por qué (no_country_table, etc.)
    - num_injections > 0  → reason == 'ok' (clauses agregados)
    Idempotente vía `marker`.
    """
    if marker in sql:
        return sql, 0, "already_injected"

    cleaned = strip_sql_strings_and_comments(sql)
    matches = _find_country_table_scopes(cleaned, aware_tables)
    if not matches:
        return sql, 0, "no_country_aware_from_join"

    # Agrupar por scope: usar (depth, scope_start) como llave. Para top-level,
    # usar (0, -1). Para anidados, usar (depth, posición del `(` que abre el scope).
    scopes: dict = {}
    for m in matches:
        # Encontrar el `(` que abre el scope actual
        if m["depth"] == 0:
            scope_key = (0, -1)
            scope_open = -1
        else:
            # Buscar el `(` más reciente sin cerrar antes de from_pos
            d = 0
            scope_open = -1
            for i in range(m["from_pos"] - 1, -1, -1):
                c = cleaned[i]
                if c == ")":
                    d += 1
                elif c == "(":
                    if d == 0:
                        scope_open = i
                        break
                    d -= 1
            scope_key = (m["depth"], scope_open)
        if scope_key not in scopes:
            scopes[scope_key] = {"open": scope_open, "table_match": m}

    # Para cada scope único, calcular punto de inyección
    insertions: list = []  # (pos, clause_text)
    for (depth, _open), info in scopes.items():
        m = info["table_match"]
        alias = m["alias"]
        scope_open = info["open"]

        if scope_open == -1:
            scope_start = 0
            scope_end = len(cleaned)
        else:
            scope_start = scope_open + 1
            scope_end = _scope_end_pos(cleaned, scope_start)

        clause = f"\n  [[AND {alias}.country_id::text = {{{{{tag_name}}}}}]]  {marker}"

        # WHERE en mismo scope (depth relativo al scope = 0)
        where_m = _find_keyword_at_depth_zero(cleaned, scope_start, scope_end, _WHERE_RE)
        # Terminator en mismo scope
        search_from = where_m.end() if where_m else m["from_end"]
        term_m = _find_keyword_at_depth_zero(cleaned, search_from, scope_end, _TERMINATOR_RE)

        if where_m:
            insert_pos = term_m.start() if term_m else scope_end
            insertions.append((insert_pos, clause))
        else:
            # Sin WHERE: agregar WHERE 1=1
            from_m = _find_keyword_at_depth_zero(cleaned, scope_start, scope_end, _FROM_RE_KW)
            if not from_m:
                continue  # no hay FROM en outer del scope, raro
            where_block = "\nWHERE 1=1" + clause
            insert_pos = term_m.start() if term_m else scope_end
            insertions.append((insert_pos, where_block))

    if not insertions:
        return sql, 0, "no_valid_injection_points"

    # Aplicar inyecciones de atrás hacia adelante (preserva posiciones)
    new_sql = sql
    for pos, clause in sorted(insertions, key=lambda x: -x[0]):
        new_sql = new_sql[:pos] + clause + "\n" + new_sql[pos:]

    return new_sql, len(insertions), "ok"


def add_country_template_tag(card_data: dict, tag_name: str) -> tuple[dict, bool]:
    """Agrega/actualiza el template-tag para el filtro country.
    Returns (new_card_data, was_added). Idempotente."""
    dq = card_data.get("dataset_query") or {}
    native = dict(dq.get("native") or {})
    template_tags = dict(native.get("template-tags") or {})
    if tag_name in template_tags:
        return card_data, False

    template_tags[tag_name] = {
        "id": secrets.token_hex(16),
        "name": tag_name,
        "display-name": "Country",
        "type": "text",
        "default": None,
        "required": False,
    }
    native["template-tags"] = template_tags
    new_dq = dict(dq)
    new_dq["native"] = native
    new_card = dict(card_data)
    new_card["dataset_query"] = new_dq
    return new_card, True


def update_card_sql_and_tags(session, headers, card_id: int, new_sql: str,
                             tag_name: str) -> tuple[bool, str]:
    """PUT /api/card/{id} con SQL modificado + template-tag. Re-fetch para leer
    estado actual (avoid stale)."""
    r = api_get(session, headers, f"/api/card/{card_id}")
    if r.status_code != 200:
        return False, f"http_get_{r.status_code}"
    card = r.json()
    card_with_tag, _ = add_country_template_tag(card, tag_name)
    dq = card_with_tag["dataset_query"]
    dq["native"] = dict(dq["native"])
    dq["native"]["query"] = new_sql
    payload = {"dataset_query": dq}
    rp = session.put(f"{METABASE_URL}/api/card/{card_id}",
                     headers=headers, json=payload, timeout=30)
    if rp.status_code != 200:
        return False, f"http_put_{rp.status_code}_{rp.text[:200]}"
    return True, "ok"


def backup_card(card_data: dict) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    cid = card_data.get("id")
    path = BACKUP_DIR / f"card_{cid}_filter_pais_{ts}.json"
    path.write_text(json.dumps(card_data, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


def plan_dashcard_action(parameter_id, dashcard, card_data, cfg,
                         session, headers, table_meta_cache) -> dict:
    """Decide qué hacer con un dashcard. Devuelve dict con:
        - status: 'inject_sql' | 'skip_<reason>'
        - mapping: dict | None  (parameter_mapping a agregar)
        - sql_change: dict | None  (instrucciones para PUT card)
            { 'card_id', 'new_sql', 'old_sql', 'alias', 'tag_name' }
    """
    card_id = dashcard.get("card_id")
    if card_id is None:
        return {"status": "skip_text_or_heading", "mapping": None, "sql_change": None}
    if not card_data:
        return {"status": "skip_card_unreachable", "mapping": None, "sql_change": None}
    if card_data.get("archived"):
        return {"status": "skip_archived_card", "mapping": None, "sql_change": None}

    dq = card_data.get("dataset_query") or {}
    qtype = dq.get("type")

    tables = detect_card_tables(card_data, session, headers, table_meta_cache)
    if not tables:
        return {"status": "skip_no_table_detected", "mapping": None, "sql_change": None}

    aware = {t.lower() for t in (cfg.get("country_aware_tables") or [])}
    matched_table = next((t for t in tables if t in aware), None)
    if not matched_table:
        return {"status": "skip_table_not_country_aware", "mapping": None, "sql_change": None}

    if qtype == "query":
        # MBQL: requiere field_id que solo existe cuando la columna esté en analytics_db
        return {"status": "skip_mbql_needs_column_propagation", "mapping": None, "sql_change": None}

    if qtype != "native":
        return {"status": f"skip_unknown_query_type_{qtype}", "mapping": None, "sql_change": None}

    sql = (dq.get("native") or {}).get("query") or ""
    if not sql:
        return {"status": "skip_empty_sql", "mapping": None, "sql_change": None}

    tag_name = cfg.get("sql_template_tag_name", "country")
    marker = cfg.get("sql_marker", "/* COUNTRY_FILTER_AUTO */")
    aware = cfg.get("country_aware_tables") or []
    new_sql, n_injected, reason = inject_country_clause(sql, aware, tag_name, marker)

    if reason == "already_injected":
        mapping = {
            "parameter_id": parameter_id,
            "card_id": card_id,
            "target": ["variable", ["template-tag", tag_name]],
        }
        return {"status": "already_injected_remap_only", "mapping": mapping, "sql_change": None}

    if n_injected == 0:
        return {"status": reason, "mapping": None, "sql_change": None}

    mapping = {
        "parameter_id": parameter_id,
        "card_id": card_id,
        "target": ["variable", ["template-tag", tag_name]],
    }
    return {
        "status": "inject_sql",
        "mapping": mapping,
        "sql_change": {
            "card_id": card_id,
            "new_sql": new_sql,
            "old_sql": sql,
            "n_clauses": n_injected,
            "tag_name": tag_name,
            "table": matched_table,
        },
    }


# ──────────────────────────────────────────────────────────────────────────
# Dashboard PUT
# ──────────────────────────────────────────────────────────────────────────

def clean_dashcard_for_put(dc: dict, new_mappings: list,
                           valid_tab_ids: set | None = None) -> dict:
    """Construye el dashcard payload mínimo para PUT /api/dashboard/{id}.
    Preserva tab/posición/visualización; reemplaza parameter_mappings con
    new_mappings (que ya incluye los preexistentes que queremos conservar).

    Si valid_tab_ids es no-None y el dashboard_tab_id del dashcard no está
    en ese set (tab huérfano por inconsistencia preexistente en Metabase),
    se omite — el dashcard queda sin tab para evitar FK violation en PUT.
    """
    payload = {
        "id": dc["id"],
        "card_id": dc.get("card_id"),
        "row": dc.get("row", 0),
        "col": dc.get("col", 0),
        "size_x": dc.get("size_x", 1),
        "size_y": dc.get("size_y", 1),
        "parameter_mappings": new_mappings,
        "visualization_settings": dc.get("visualization_settings", {}),
    }
    tab_id = dc.get("dashboard_tab_id")
    if tab_id is not None:
        if valid_tab_ids is None or tab_id in valid_tab_ids:
            payload["dashboard_tab_id"] = tab_id
    if dc.get("series"):
        payload["series"] = dc["series"]
    if dc.get("action_id"):
        payload["action_id"] = dc["action_id"]
    return payload


def merge_country_mapping(existing_mappings: list, parameter_id: str,
                          new_mapping: dict | None) -> list:
    """Sustituye el mapping de country (si existe) por new_mapping. Si new_mapping
    es None y ya había uno, se elimina (recomputado a skip)."""
    kept = [m for m in (existing_mappings or [])
            if m.get("parameter_id") != parameter_id]
    if new_mapping is not None:
        kept.append(new_mapping)
    return kept


# ──────────────────────────────────────────────────────────────────────────
# Backup
# ──────────────────────────────────────────────────────────────────────────

def backup_dashboard(dash_data: dict) -> Path:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"dashboard_{dash_data['id']}_filter_pais_{ts}.json"
    path.write_text(json.dumps(dash_data, ensure_ascii=False, indent=2),
                    encoding="utf-8")
    return path


# ──────────────────────────────────────────────────────────────────────────
# Procesar un dashboard
# ──────────────────────────────────────────────────────────────────────────

def process_dashboard(session, headers, dash_cfg: dict, cfg: dict,
                      dry_run: bool, table_meta_cache: dict) -> dict:
    dash_name = dash_cfg["name"]
    print(f"\n{'─' * 60}")
    print(f"  Dashboard: \"{dash_name}\"")
    print(f"{'─' * 60}")

    dash_id = dash_cfg.get("id")
    if dash_id is None:
        dash_id = resolve_dashboard_id(session, headers, dash_name)
    if dash_id is None:
        return {"name": dash_name, "status": "error_resolve_id"}

    dash_data = get_dashboard(session, headers, dash_id)
    if not dash_data:
        return {"name": dash_name, "id": dash_id, "status": "error_get"}

    print(f"  ID = {dash_id}")
    print(f"  can_write = {dash_data.get('can_write')}")
    print(f"  dashcards = {len(dash_data.get('dashcards', []))}")

    # Backup ANTES de cualquier mutación, incluso en dry-run (es barato y útil)
    backup_path = backup_dashboard(dash_data)
    print(f"  Backup → {backup_path.relative_to(_ROOT)}")

    # ── Parameter ─────────────────────────────────────────────────
    parameters, param_id, was_added = ensure_country_parameter(dash_data, cfg)
    print(f"  Parameter 'Country' → id={param_id} ({'NUEVO' if was_added else 'reutilizado'})")

    # ── Per-dashcard plan ─────────────────────────────────────────
    valid_tab_ids = {t["id"] for t in (dash_data.get("tabs") or [])}
    card_cache: dict = {}
    new_dashcards = []
    sql_changes_to_apply: list = []  # cards SQL que hay que PUTtear
    counts = {"inject_sql": 0, "already_injected_remap_only": 0}
    skip_counts: dict = {}
    orphan_tab_dashcards = 0
    per_card_log: list = []

    for dc in dash_data.get("dashcards", []):
        card_id = dc.get("card_id")
        card_data = get_card(session, headers, card_id, card_cache) if card_id else None
        plan = plan_dashcard_action(
            param_id, dc, card_data, cfg, session, headers, table_meta_cache,
        )
        status = plan["status"]
        new_pm = merge_country_mapping(dc.get("parameter_mappings"), param_id, plan["mapping"])
        tab_id = dc.get("dashboard_tab_id")
        if tab_id is not None and valid_tab_ids and tab_id not in valid_tab_ids:
            orphan_tab_dashcards += 1
        new_dashcards.append(clean_dashcard_for_put(dc, new_pm, valid_tab_ids or None))

        if plan.get("sql_change"):
            sql_changes_to_apply.append(plan["sql_change"])

        card_name = (card_data or {}).get("name") if card_data else None
        per_card_log.append({
            "dashcard_id": dc.get("id"),
            "card_id": card_id,
            "card_name": card_name,
            "result": status,
        })

        if status in counts:
            counts[status] += 1
        else:
            skip_counts[status] = skip_counts.get(status, 0) + 1

        time.sleep(0.02)

    # ── Resumen del dashboard ────────────────────────────────────
    print(f"  Inject SQL (nuevas):    {counts['inject_sql']}")
    print(f"  Ya inyectadas (remap):  {counts['already_injected_remap_only']}")
    if skip_counts:
        print(f"  Skipped:")
        for reason, n in sorted(skip_counts.items(), key=lambda x: -x[1]):
            print(f"    - {reason}: {n}")
    if orphan_tab_dashcards:
        print(f"  Orphan tab refs detectados (FK preexistente rota): {orphan_tab_dashcards} "
              f"— se omite dashboard_tab_id en esos dashcards para evitar HTTP 500")

    # ── Aplicar cambios SQL a cards (antes del PUT del dashboard) ─
    sql_results = []
    for ch in sql_changes_to_apply:
        cid = ch["card_id"]
        if dry_run:
            print(f"    [DRY] Card {cid} ({ch['table']}, {ch['n_clauses']} clause(s)) — se inyectaría")
            sql_results.append({"card_id": cid, "status": "dry_run"})
            continue
        # Backup card antes de modificar
        card_full = card_cache.get(cid) or get_card(session, headers, cid, card_cache)
        if card_full:
            backup_card(card_full)
        ok, reason = update_card_sql_and_tags(
            session, headers, cid, ch["new_sql"], ch["tag_name"],
        )
        marker = "OK" if ok else f"FAIL ({reason})"
        print(f"    Card {cid} → {marker}")
        sql_results.append({"card_id": cid, "status": "ok" if ok else "error",
                            "detail": reason if not ok else None})
        time.sleep(0.3)

    # ── PUT ──────────────────────────────────────────────────────
    # tabs DEBE ir en el payload — sin él, Metabase trata las referencias
    # dashboard_tab_id como huérfanas y rechaza con FK violation HTTP 500.
    tabs_payload = [
        {"id": t["id"], "name": t["name"], "position": t.get("position", i)}
        for i, t in enumerate(dash_data.get("tabs") or [])
    ]
    payload = {"parameters": parameters, "dashcards": new_dashcards}
    if tabs_payload:
        payload["tabs"] = tabs_payload
    if dry_run:
        print(f"  [DRY-RUN] Se haría PUT /api/dashboard/{dash_id}")
        put_status = "dry_run"
    else:
        r = api_put(session, headers, f"/api/dashboard/{dash_id}", payload)
        if r.status_code == 200:
            print(f"  OK PUT — HTTP 200")
            put_status = "ok"
        else:
            print(f"  ERROR PUT HTTP {r.status_code}: {r.text[:300]}")
            put_status = f"error_http_{r.status_code}"

    return {
        "name": dash_name,
        "id": dash_id,
        "parameter_id": param_id,
        "parameter_added": was_added,
        "backup": str(backup_path.relative_to(_ROOT)),
        "inject_sql": counts["inject_sql"],
        "already_injected_remap_only": counts["already_injected_remap_only"],
        "skipped": skip_counts,
        "orphan_tab_refs": orphan_tab_dashcards,
        "sql_results": sql_results,
        "put_status": put_status,
        "per_card": per_card_log,
    }


# ──────────────────────────────────────────────────────────────────────────
# Entry
# ──────────────────────────────────────────────────────────────────────────

def run(dry_run: bool, dashboard_filter: str | None):
    cfg = load_config()
    token = get_session_token()
    headers = make_headers(token)
    session = requests.Session()

    mode = "DRY-RUN" if dry_run else "REAL"
    print(f"\n{'=' * 60}")
    print(f"  agregar_filtro_pais.py — Modo: {mode}")
    print(f"  Catálogo: {len(cfg['country_catalog'])} países "
          f"({', '.join(c['name'] for c in cfg['country_catalog'])})")
    print(f"  Default: {cfg['parameter'].get('default')}")
    aware = cfg.get("country_aware_tables") or []
    print(f"  Country-aware tables: {aware}")
    print(f"  SQL injection target: cards SQL nativas con FROM/JOIN sobre esas tablas")
    print(f"{'=' * 60}")

    targets = cfg["dashboards"]
    if dashboard_filter:
        targets = [d for d in targets
                   if d["name"] == dashboard_filter or str(d.get("id")) == dashboard_filter]
        if not targets:
            print(f"\nERROR: --dashboard '{dashboard_filter}' no está en config/country_filter.json")
            sys.exit(1)

    table_meta_cache: dict = {}
    results = []
    for d in targets:
        results.append(process_dashboard(
            session, headers, d, cfg, dry_run, table_meta_cache,
        ))

    # ── Log persistente ─────────────────────────────────────────
    log_entry = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "config_snapshot": {
            "default": cfg["parameter"].get("default"),
            "country_catalog": cfg["country_catalog"],
            "country_aware_tables": cfg.get("country_aware_tables"),
        },
        "dashboards": results,
    }
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing: list = []
    if LOG_FILE.exists():
        try:
            existing = json.loads(LOG_FILE.read_text(encoding="utf-8"))
        except Exception:
            existing = []
    existing.append(log_entry)
    LOG_FILE.write_text(json.dumps(existing, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(f"\n  Log → {LOG_FILE.relative_to(_ROOT)}")
    if dry_run:
        print("\n  DRY-RUN OK — para aplicar real corre sin --dry-run")


def main():
    parser = argparse.ArgumentParser(
        description="Agrega filtro 'Country' a dashboards principales de Metabase",
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Simula sin hacer PUT al dashboard")
    parser.add_argument("--dashboard", metavar="NOMBRE", default=None,
                        help="Procesa solo este dashboard (nombre exacto)")
    args = parser.parse_args()

    if not args.dry_run:
        print("\nATENCION: modo REAL — se modificarán dashboards en Metabase.")
        print("Backups completos se guardan en data/processed/resultados/backups/")
        confirm = input("Escribe 'SI' para continuar: ")
        if confirm.strip().upper() != "SI":
            print("Cancelado.")
            sys.exit(0)

    run(dry_run=args.dry_run, dashboard_filter=args.dashboard)


if __name__ == "__main__":
    main()
