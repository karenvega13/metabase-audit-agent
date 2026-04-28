"""
src/core/catalog/deduplication.py — SQL Fingerprinting y Similitud Estructural.

Diseño de eficiencia para 3000+ cards:
- Exact dedup:     O(n)   — agrupación por hash MD5 (precalculado en loaders)
- Structural sim:  O(n * k) donde k = cards con tablas en común
                   Bloqueamos por tabla, descartamos tablas con >COMMON_THRESHOLD cards
                   para evitar el O(n²) del peor caso con transaction_details (~1000 cards).
"""

import re
from collections import defaultdict
from typing import Optional

# Tablas que aparecen en >80% de las queries; no son útiles como clave de bloqueo
COMMON_TABLE_THRESHOLD = 150

# Umbral de similitud de Jaccard para marcar como VARIACION
STRUCTURAL_SIMILARITY_THRESHOLD = 0.80

# Palabras reservadas SQL que pueden colarse en el regex de tablas
_SQL_KEYWORDS = frozenset(
    [
        "select", "where", "on", "and", "or", "not", "in", "exists",
        "lateral", "values", "dual", "true", "false", "null", "case",
        "when", "then", "else", "end", "as", "with", "union", "all",
        "intersect", "except", "limit", "offset", "order", "group", "by",
        "having", "distinct", "between", "like", "ilike", "is", "any",
        "some", "all", "cast", "coalesce", "nullif", "extract", "over",
        "partition", "rows", "range", "preceding", "following", "current",
    ]
)

# Columnas de fecha, CTEs comunes y aliases que el regex puede confundir con tablas
_NON_TABLE_TOKENS = frozenset(
    [
        # Columnas de fecha verificadas en analytics_db (docs/INICIO.md)
        "injected_date", "created_date", "created_at", "updated_at",
        "snapshot_month", "period_start", "period_end", "first_paid_date",
        "first_paid_at", "last_used_at",
        # Nombres de CTEs/subqueries comunes en el codebase
        "base", "base_data", "base_raw", "base_range", "base_weekly_txns",
        "final", "filtered", "filtered_data", "ranked", "dedup",
        "agg", "aggregated", "series", "counts_ranked",
        # Aliases de tablas parametrizadas (Metabase template variables)
        "reated_date",  # typo frecuente de created_date en algunos lotes
    ]
)

_TABLE_RE = re.compile(
    r"\b(?:from|join)\s+([a-z_][a-z0-9_.]*)",
    re.IGNORECASE,
)
_JOIN_RE = re.compile(
    r"\b((?:inner|left\s+outer|right\s+outer|full\s+outer|left|right|full|cross)\s+join)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Extractors
# ---------------------------------------------------------------------------

def extract_tables(sql_normalized: str) -> frozenset[str]:
    """Extrae nombres de tablas del SQL normalizado (elimina alias y keywords)."""
    tables: set[str] = set()
    for m in _TABLE_RE.finditer(sql_normalized):
        raw = m.group(1).split(".")[-1]  # strip schema prefix
        if raw not in _SQL_KEYWORDS and raw not in _NON_TABLE_TOKENS and len(raw) > 2:
            tables.add(raw)
    return frozenset(tables)


def extract_joins(sql_normalized: str) -> frozenset[str]:
    """Extrae tipos de JOIN del SQL normalizado."""
    return frozenset(
        re.sub(r"\s+", " ", m.group(1).lower().strip())
        for m in _JOIN_RE.finditer(sql_normalized)
    )


def extract_core_logic(sql_normalized: str) -> str:
    """
    Extrae el 'Core Logic': la expresión principal del SELECT.
    Se usa para agrupar variaciones bajo una Métrica Madre.
    """
    m = re.match(r"select\s+(.+?)\s+from\b", sql_normalized, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1)[:300].strip()
    return sql_normalized[:200]


def extract_where_summary(sql_normalized: str) -> str:
    """Extrae la cláusula WHERE completa para el reporte de conflictos."""
    m = re.search(r"\bwhere\b(.+?)(?:\bgroup\s+by\b|\border\s+by\b|\blimit\b|\Z)", sql_normalized, re.DOTALL)
    if m:
        return m.group(1).strip()[:400]
    return ""


# ---------------------------------------------------------------------------
# Jaccard similarity
# ---------------------------------------------------------------------------

def jaccard(set_a: frozenset, set_b: frozenset) -> float:
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


# ---------------------------------------------------------------------------
# Union-Find helpers
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self, elements):
        self._parent = {e: e for e in elements}

    def find(self, x):
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]  # path compression
            x = self._parent[x]
        return x

    def union(self, x, y):
        px, py = self.find(x), self.find(y)
        if px != py:
            self._parent[px] = py

    def groups(self) -> list[list]:
        g: dict = defaultdict(list)
        for e in self._parent:
            g[self.find(e)].append(e)
        return [v for v in g.values() if len(v) > 1]


# ---------------------------------------------------------------------------
# Main structural grouping
# ---------------------------------------------------------------------------

def enrich_cards_with_structure(cards: dict[int, dict]) -> None:
    """
    Añade in-place a cada card:
      - tables: frozenset de tablas referenciadas
      - joins: frozenset de tipos de JOIN
      - core_logic: expresión principal del SELECT
      - where_summary: cláusula WHERE resumida
    """
    for card in cards.values():
        sql = card.get("sql_normalized", "")
        card["tables"] = extract_tables(sql)
        card["joins"] = extract_joins(sql)
        card["core_logic"] = extract_core_logic(sql)
        card["where_summary"] = extract_where_summary(sql)


def build_structural_groups(
    cards: dict[int, dict],
    threshold: float = STRUCTURAL_SIMILARITY_THRESHOLD,
) -> list[list[int]]:
    """
    Agrupa cards con similitud estructural (Jaccard de tablas) >= threshold.

    Eficiencia:
      1. Construye un índice tabla→cards
      2. Descarta tablas que aparecen en demasiadas cards (COMMON_TABLE_THRESHOLD)
         para evitar el caso O(n²) con transaction_details
      3. Solo compara pares que comparten al menos una tabla poco común
      4. Union-Find para transitiva de grupos

    Retorna lista de grupos con >= 2 card_ids.
    """
    # Índice inverso: tabla → set de card_ids
    table_index: dict[str, set[int]] = defaultdict(set)
    for cid, card in cards.items():
        for tbl in card.get("tables", frozenset()):
            table_index[tbl].add(cid)

    # Filtrar tablas "comunes" que introducirían demasiados pares
    blocking_tables = {
        tbl: cids
        for tbl, cids in table_index.items()
        if len(cids) <= COMMON_TABLE_THRESHOLD
    }

    uf = UnionFind(cards.keys())
    seen_pairs: set[tuple[int, int]] = set()

    for tbl, cids in blocking_tables.items():
        cid_list = sorted(cids)
        for i in range(len(cid_list)):
            for j in range(i + 1, len(cid_list)):
                a, b = cid_list[i], cid_list[j]
                pair = (a, b) if a < b else (b, a)
                if pair in seen_pairs:
                    continue
                seen_pairs.add(pair)

                sim = jaccard(cards[a]["tables"], cards[b]["tables"])
                if sim >= threshold:
                    uf.union(a, b)

    return uf.groups()
