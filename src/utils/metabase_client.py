"""
src/utils/metabase_client.py — Constantes y helpers de autenticación para la API de Metabase.

Uso:
    from src.utils.metabase_client import METABASE_URL, get_session_token, make_headers
"""

import os
import sys

from src.utils.env_loader import load_env

METABASE_URL = "https://metabase.example.com"


def get_session_token() -> str:
    """Retorna METABASE_SESSION desde os.environ o api_key.env. Aborta si no existe."""
    token = os.environ.get("METABASE_SESSION")
    if token:
        return token
    env = load_env()
    token = env.get("METABASE_SESSION") or env.get("METABASE_TOKEN")
    if not token:
        print("ERROR: METABASE_SESSION no encontrado en variables de entorno ni en config/api_key.env")
        print("  Agrega: METABASE_SESSION=<tu_session_token> en config/api_key.env")
        sys.exit(1)
    return token


def make_headers(token: str | None = None) -> dict:
    """Devuelve los headers HTTP necesarios para la API de Metabase."""
    if token is None:
        token = get_session_token()
    return {
        "X-Metabase-Session": token,
        "Content-Type": "application/json",
    }
