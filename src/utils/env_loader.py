"""
src/utils/env_loader.py — Carga centralizada de config/api_key.env en os.environ.

Uso:
    from src.utils.env_loader import load_env, ENV_FILE
    load_env()
"""

import os
from pathlib import Path

# Ruta al archivo de credenciales: config/api_key.env en la raíz del proyecto
ENV_FILE = Path(__file__).resolve().parents[2] / "config" / "api_key.env"


def load_env(path: Path = ENV_FILE) -> dict:
    """Carga KEY=VALUE de api_key.env en os.environ. Retorna dict con las claves cargadas."""
    result = {}
    if not path.exists():
        return result
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip()
            result[key] = val
            os.environ.setdefault(key, val)
    return result
