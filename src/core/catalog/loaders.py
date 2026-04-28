"""
src/core/catalog/loaders.py — Carga de datos desde lotes, audit results, duplicados y obsolescencia.
"""

import csv
import json
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[3]
DATA_DIR = _ROOT / "data"

sys.path.insert(0, str(_ROOT))
from src.utils.sql_utils import normalize_sql, sql_fingerprint  # noqa: F401
from src.utils.env_loader import load_env  # noqa: F401


# ---------------------------------------------------------------------------
# Lote file parser
# ---------------------------------------------------------------------------

_CARD_HEADER = re.compile(
    r"--\s*Card:\s*(.+?)\s*\(id:(\d+)\s*\|\s*vistas:([\d,]+)\)\s*\n"
    r"--\s*Colecci[oó]n:\s*(.+?)\s*\n"
    r"(.*?)(?=\n--\s*Card:|\Z)",
    re.DOTALL,
)


def parse_lote_file(filepath: Path) -> list[dict]:
    """Parsea un archivo lote_*.sql y devuelve lista de dicts por card."""
    content = filepath.read_text(encoding="utf-8", errors="replace")
    cards = []
    for m in _CARD_HEADER.finditer(content):
        name, card_id, vistas_raw, collection, sql = m.groups()
        sql = sql.strip()
        # Remove trailing separator lines
        sql = re.sub(r"\n--\s*[-=]{10,}.*$", "", sql, flags=re.DOTALL).strip()
        if not sql:
            continue
        vistas = int(vistas_raw.replace(",", ""))
        cards.append(
            {
                "card_id": int(card_id),
                "card_name": name.strip(),
                "vistas": vistas,
                "collection": collection.strip(),
                "sql_raw": sql,
                "sql_normalized": normalize_sql(sql),
                "sql_fingerprint": sql_fingerprint(sql),
                "lote": filepath.stem,
            }
        )
    return cards


def load_all_cards() -> dict[int, dict]:
    """Carga todas las cards de todos los lotes. Retorna dict keyed by card_id."""
    cards: dict[int, dict] = {}
    lotes_dir = DATA_DIR / "raw" / "lotes"
    for f in sorted(lotes_dir.glob("lote_*.sql")):
        for card in parse_lote_file(f):
            # Si una card aparece en varios lotes, conservar el primero (mayor vistas en lote_01)
            cards.setdefault(card["card_id"], card)
    return cards


# ---------------------------------------------------------------------------
# Audit results loader
# ---------------------------------------------------------------------------

def load_audit_results() -> dict[int, dict]:
    """Carga todos los lote_*_resultados.json. Retorna dict keyed by card_id."""
    results: dict[int, dict] = {}
    resultados_dir = DATA_DIR / "processed" / "resultados"
    for f in sorted(resultados_dir.glob("lote_*_resultados.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for card in data.get("cards", []):
                cid = card.get("card_id")
                if cid:
                    results[cid] = card
        except Exception as exc:
            print(f"  [WARN] No se pudo leer {f.name}: {exc}", file=sys.stderr)
    # Merge re-audit results (override with latest)
    reaudit_dir = resultados_dir / "reaudit"
    if reaudit_dir.exists():
        for ts_dir in sorted(reaudit_dir.iterdir()):
            for f in sorted(ts_dir.glob("lote_*.json")):
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    for card in data.get("cards", []):
                        cid = card.get("card_id")
                        if cid:
                            results[cid] = card
                except Exception:
                    pass
    return results


# ---------------------------------------------------------------------------
# Duplicate detection loader
# ---------------------------------------------------------------------------

def load_duplicates() -> dict:
    """Carga duplicados_sql.json si existe."""
    dup_file = DATA_DIR / "processed" / "resultados" / "duplicados_sql.json"
    if dup_file.exists():
        try:
            return json.loads(dup_file.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


# ---------------------------------------------------------------------------
# Obsolescence loader
# ---------------------------------------------------------------------------

def load_obsolescence() -> dict[int, dict]:
    """Carga obsolescencia_cards.csv. Retorna dict keyed by card_id."""
    obs_file = DATA_DIR / "raw" / "obsolescencia_cards.csv"
    if not obs_file.exists():
        return {}
    results: dict[int, dict] = {}
    with open(obs_file, encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                cid = int(row.get("card_id") or 0)
                if cid:
                    results[cid] = row
            except (ValueError, TypeError):
                pass
    return results
