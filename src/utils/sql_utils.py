"""
src/utils/sql_utils.py — Utilidades de normalización y fingerprinting de SQL.

Uso:
    from src.utils.sql_utils import normalize_sql, sql_fingerprint
"""

import hashlib
import re


def normalize_sql(sql: str) -> str:
    """Elimina comentarios, colapsa espacios y pasa a minúsculas."""
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    sql = re.sub(r"\s+", " ", sql).strip().lower()
    return sql


def sql_fingerprint(sql: str) -> str:
    """MD5 truncado a 12 chars del SQL normalizado."""
    return hashlib.md5(normalize_sql(sql).encode()).hexdigest()[:12]
