"""
src/utils/db_client.py — Fábrica de conexión psycopg2 para analytics_db.

Uso:
    from src.utils.db_client import get_connection
    conn = get_connection()
"""

import os
import getpass

from src.utils.env_loader import load_env


def _get_env(key: str, prompt_text: str, secret: bool = False) -> str | None:
    """Lee una variable de os.environ o pide al usuario de forma interactiva."""
    val = os.environ.get(key)
    if val:
        return val
    if prompt_text:
        if secret:
            return getpass.getpass(f"   {prompt_text}: ").strip()
        else:
            return input(f"   {prompt_text}: ").strip()
    return None


def get_connection(readonly: bool = True):
    """
    Conecta a analytics_db. Lee credenciales de config/api_key.env / os.environ,
    y pide interactivamente si no están disponibles.

    Args:
        readonly: si True, abre la sesión en modo lectura (default).

    Returns:
        psycopg2 connection
    """
    import psycopg2  # import tardío — no requerido si el proyecto no usa BD

    load_env()  # asegurar que config/api_key.env esté cargado
    host   = _get_env("DB_HOST",     "Host de analytics_db")
    port   = _get_env("DB_PORT",     "Puerto [5432]") or "5432"
    dbname = _get_env("DB_NAME",     "Database") or "analytics_db"
    user   = _get_env("DB_USER",     "Usuario") or "dbuser"
    pwd    = _get_env("DB_PASSWORD", "Password de analytics_db", secret=True)

    conn = psycopg2.connect(
        host=host,
        port=int(port),
        dbname=dbname,
        user=user,
        password=pwd,
        connect_timeout=15,
        options="-c statement_timeout=60000",
    )
    conn.set_session(readonly=readonly, autocommit=True)
    return conn
