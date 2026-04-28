"""
detectar_duplicados.py
Detecta cards de Metabase con SQL idéntico (o casi idéntico) que no tienen
"Duplicate" en su nombre — posibles duplicados silenciosos no detectados en
la auditoría de lógica.

Metodología:
  1. Lee todos los archivos de lotes (lotes/lote_*.sql)
  2. Parsea cada card: id, nombre, colección, vistas, SQL
  3. Normaliza el SQL: minúsculas + colapsar whitespace + quitar comentarios
  4. Calcula MD5 del SQL normalizado
  5. Agrupa por hash — grupos ≥ 2 = duplicados exactos
  6. Para similitud parcial: también reporta grupos con similitud ≥ 90%
     (mismo hash de SQL sin literales de strings y números)

Outputs:
  resultados/duplicados_sql.json   → detalle completo por grupo
  resultados/duplicados_sql.csv    → formato tabular para abrir en Excel

Uso:
  python detectar_duplicados.py               # análisis completo
  python detectar_duplicados.py --min-vistas 100   # solo cards con ≥ 100 vistas
  python detectar_duplicados.py --exactos-only     # solo duplicados exactos (sin similitud parcial)
"""

import os
import re
import csv
import json
import hashlib
import argparse
from pathlib import Path
from collections import defaultdict
from datetime import datetime

_ROOT      = Path(__file__).resolve().parent.parent.parent.parent
LOTES_DIR  = str(_ROOT / "data" / "raw" / "lotes")
OUTPUT_DIR = str(_ROOT / "data" / "processed" / "resultados")

# ============================================================
# PARSING — igual que audit_agent.py
# ============================================================

def parse_lote(filepath: str) -> list:
    """
    Lee un archivo de lote y devuelve lista de cards con id, nombre,
    colección, vistas y SQL.
    """
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    cards = []
    blocks = re.split(r"\n(?=-- Card: )", content)

    for block in blocks:
        lines = block.strip().splitlines()
        if not lines or not lines[0].startswith("-- Card:"):
            continue

        # Header: -- Card: <nombre> (id:<id> | vistas:<vistas>)
        header = lines[0]
        card_match = re.match(r"-- Card: (.+?) \(id:(\d+) \| vistas:([\d,]+)\)", header)
        if not card_match:
            continue

        name   = card_match.group(1).strip()
        cid    = int(card_match.group(2))
        vistas = int(card_match.group(3).replace(",", ""))

        # Colección: segunda línea
        coleccion = ""
        if len(lines) > 1 and lines[1].startswith("-- Colección:"):
            coleccion = lines[1].replace("-- Colección:", "").strip()

        # SQL: todo lo demás (skip header + colección)
        sql_lines = []
        for line in lines[2:]:
            sql_lines.append(line)

        sql = "\n".join(sql_lines).strip()

        # Ignorar Query Builder (no tienen SQL real)
        if not sql or sql.startswith("-- [QUERY BUILDER]"):
            continue

        cards.append({
            "id":       cid,
            "name":     name,
            "vistas":   vistas,
            "coleccion": coleccion,
            "sql":      sql,
            "lote":     Path(filepath).stem,
        })

    return cards


# ============================================================
# NORMALIZACIÓN DE SQL
# ============================================================

def normalize_sql(sql: str) -> str:
    """
    Normaliza SQL para comparación:
    - Elimina líneas de comentario (-- ...)
    - Elimina comentarios de bloque (/* ... */)
    - Convierte a minúsculas
    - Colapsa whitespace (espacios, tabs, saltos de línea) en un solo espacio
    - Elimina espacios antes/después de paréntesis y comas (para canonicalizar)
    """
    # Eliminar comentarios de bloque
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    # Eliminar comentarios de línea
    sql = re.sub(r"--[^\n]*", " ", sql)
    # Minúsculas
    sql = sql.lower()
    # Colapsar whitespace
    sql = re.sub(r"\s+", " ", sql)
    # Trim
    sql = sql.strip()
    return sql


def normalize_sql_fuzzy(sql: str) -> str:
    """
    Normalización más agresiva para similitud parcial:
    - Aplica normalize_sql() primero
    - Además reemplaza literales de strings ('...') y números por placeholders
    - Útil para detectar cards que hacen lo mismo pero con fechas o valores distintos
    """
    sql = normalize_sql(sql)
    # Reemplazar strings literales
    sql = re.sub(r"'[^']*'", "'?'", sql)
    # Reemplazar números
    sql = re.sub(r"\b\d+\b", "0", sql)
    return sql


def sql_hash(sql_normalized: str) -> str:
    return hashlib.md5(sql_normalized.encode("utf-8")).hexdigest()[:12]


# ============================================================
# DETECCIÓN DE DUPLICADOS
# ============================================================

def find_duplicates(cards: list, min_vistas: int = 0) -> dict:
    """
    Retorna un dict con dos claves:
      'exactos'  → grupos con SQL normalizado idéntico (hash exacto)
      'fuzzy'    → grupos con SQL fuzzy idéntico (mismo hash sin literales/números)
                   pero hash exacto distinto (similitud ~90%)
    """
    exact_groups  = defaultdict(list)   # hash_exacto -> [cards]
    fuzzy_groups  = defaultdict(list)   # hash_fuzzy  -> [cards]

    for card in cards:
        if card["vistas"] < min_vistas:
            continue

        norm  = normalize_sql(card["sql"])
        fuzz  = normalize_sql_fuzzy(card["sql"])
        h_ex  = sql_hash(norm)
        h_fz  = sql_hash(fuzz)

        card["_hash_exacto"] = h_ex
        card["_hash_fuzzy"]  = h_fz
        card["_sql_norm"]    = norm

        exact_groups[h_ex].append(card)
        fuzzy_groups[h_fz].append(card)

    # Filtrar: solo grupos con ≥ 2 cards
    duplicados_exactos = {
        h: group for h, group in exact_groups.items() if len(group) >= 2
    }

    # Fuzzy: grupos con ≥ 2 cards Y que no sean ya un duplicado exacto completo
    # (es decir, al menos 2 hashes exactos distintos dentro del grupo fuzzy)
    duplicados_fuzzy = {}
    for h, group in fuzzy_groups.items():
        if len(group) < 2:
            continue
        hashes_exactos = {c["_hash_exacto"] for c in group}
        if len(hashes_exactos) < 2:
            continue  # son exactamente los mismos — ya capturados en exactos
        # Si hay overlap con exactos, solo incluir cards que NO sean exactas entre sí
        duplicados_fuzzy[h] = group

    return {
        "exactos": duplicados_exactos,
        "fuzzy":   duplicados_fuzzy,
    }


# ============================================================
# REPORTE
# ============================================================

def tiene_duplicate_en_nombre(cards_grupo: list) -> bool:
    """Retorna True si TODAS las cards del grupo tienen 'duplicate' en el nombre."""
    return all("duplicate" in c["name"].lower() for c in cards_grupo)


def build_report(result: dict, exactos_only: bool = False) -> dict:
    """
    Construye el reporte final con metadata por grupo.
    Filtra grupos donde TODAS las cards ya tienen 'duplicate' en el nombre
    (esos ya son conocidos).
    """
    grupos_exactos = []
    for h, group in result["exactos"].items():
        # Si todas tienen "duplicate" en el nombre, ya están etiquetadas → saltar
        if tiene_duplicate_en_nombre(group):
            continue

        # Cuántas tienen "duplicate" en el nombre
        n_con_label = sum(1 for c in group if "duplicate" in c["name"].lower())

        grupos_exactos.append({
            "hash":            h,
            "tipo":            "exacto",
            "n_cards":         len(group),
            "n_sin_label":     len(group) - n_con_label,
            "total_vistas":    sum(c["vistas"] for c in group),
            "cards": sorted([
                {
                    "id":       c["id"],
                    "name":     c["name"],
                    "vistas":   c["vistas"],
                    "coleccion": c["coleccion"],
                    "lote":     c["lote"],
                    "url":      f"https://metabase.example.com/card/{c['id']}",
                    "tiene_label_duplicate": "duplicate" in c["name"].lower(),
                }
                for c in group
            ], key=lambda x: -x["vistas"]),
            "sql_normalizado": group[0]["_sql_norm"][:300] + ("..." if len(group[0]["_sql_norm"]) > 300 else ""),
        })

    # Ordenar por total_vistas descendente (más impacto primero)
    grupos_exactos.sort(key=lambda g: -g["total_vistas"])

    grupos_fuzzy = []
    if not exactos_only:
        for h, group in result["fuzzy"].items():
            if tiene_duplicate_en_nombre(group):
                continue

            n_con_label = sum(1 for c in group if "duplicate" in c["name"].lower())
            grupos_fuzzy.append({
                "hash":            h,
                "tipo":            "fuzzy",
                "n_cards":         len(group),
                "n_sin_label":     len(group) - n_con_label,
                "total_vistas":    sum(c["vistas"] for c in group),
                "cards": sorted([
                    {
                        "id":       c["id"],
                        "name":     c["name"],
                        "vistas":   c["vistas"],
                        "coleccion": c["coleccion"],
                        "lote":     c["lote"],
                        "url":      f"https://metabase.example.com/card/{c['id']}",
                        "tiene_label_duplicate": "duplicate" in c["name"].lower(),
                    }
                    for c in group
                ], key=lambda x: -x["vistas"]),
            })

        grupos_fuzzy.sort(key=lambda g: -g["total_vistas"])

    return {
        "generado_en": datetime.now().isoformat(),
        "resumen": {
            "grupos_exactos":         len(grupos_exactos),
            "cards_en_grupos_exactos": sum(g["n_cards"] for g in grupos_exactos),
            "grupos_fuzzy":           len(grupos_fuzzy),
            "cards_en_grupos_fuzzy":  sum(g["n_cards"] for g in grupos_fuzzy),
        },
        "grupos_exactos": grupos_exactos,
        "grupos_fuzzy":   grupos_fuzzy,
    }


def save_json(report: dict, path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"  → JSON guardado: {path}")


def save_csv(report: dict, path: str):
    """
    Genera CSV con una fila por card duplicada.
    Columnas: grupo_hash, tipo, card_id, card_name, vistas, coleccion, url,
              tiene_label_duplicate, total_vistas_grupo, n_cards_grupo
    """
    rows = []
    for grupo in report["grupos_exactos"] + report["grupos_fuzzy"]:
        for card in grupo["cards"]:
            rows.append({
                "grupo_hash":            grupo["hash"],
                "tipo":                  grupo["tipo"],
                "card_id":               card["id"],
                "card_name":             card["name"],
                "vistas":                card["vistas"],
                "coleccion":             card["coleccion"],
                "url":                   card["url"],
                "tiene_label_duplicate": card["tiene_label_duplicate"],
                "total_vistas_grupo":    grupo["total_vistas"],
                "n_cards_grupo":         grupo["n_cards"],
            })

    if not rows:
        print("  → Sin duplicados encontrados para escribir en CSV.")
        return

    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"  → CSV guardado: {path}")


def print_summary(report: dict):
    res = report["resumen"]
    print("\n" + "=" * 60)
    print("RESUMEN DE DUPLICADOS SQL — EXAMPLECORP METABASE")
    print("=" * 60)
    print(f"  Duplicados exactos:  {res['grupos_exactos']} grupos · {res['cards_en_grupos_exactos']} cards")
    print(f"  Duplicados fuzzy:    {res['grupos_fuzzy']} grupos · {res['cards_en_grupos_fuzzy']} cards")
    print()

    if report["grupos_exactos"]:
        print("TOP 10 GRUPOS EXACTOS (por vistas totales):")
        print("-" * 60)
        for i, g in enumerate(report["grupos_exactos"][:10], 1):
            ids = ", ".join(str(c["id"]) for c in g["cards"])
            print(f"  [{i}] Hash {g['hash']} · {g['n_cards']} cards · {g['total_vistas']:,} vistas totales")
            print(f"      IDs: {ids}")
            for c in g["cards"]:
                label = " [ya etiquetada]" if c["tiene_label_duplicate"] else ""
                print(f"        • [{c['id']}] {c['name'][:60]} — {c['vistas']:,} vistas{label}")
            print()

    if report["grupos_fuzzy"]:
        print("TOP 5 GRUPOS FUZZY (SQL similar, literales distintos):")
        print("-" * 60)
        for i, g in enumerate(report["grupos_fuzzy"][:5], 1):
            ids = ", ".join(str(c["id"]) for c in g["cards"])
            print(f"  [{i}] Hash {g['hash']} · {g['n_cards']} cards · {g['total_vistas']:,} vistas totales")
            for c in g["cards"]:
                label = " [ya etiquetada]" if c["tiene_label_duplicate"] else ""
                print(f"        • [{c['id']}] {c['name'][:60]} — {c['vistas']:,} vistas{label}")
            print()


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="Detecta duplicados SQL en cards de Metabase")
    parser.add_argument("--min-vistas", type=int, default=0,
                        help="Solo analizar cards con ≥ N vistas (default: 0 = todas)")
    parser.add_argument("--exactos-only", action="store_true",
                        help="Solo reportar duplicados exactos (sin análisis fuzzy)")
    args = parser.parse_args()

    # ---- Leer todos los lotes ----
    lotes_dir = Path(LOTES_DIR)
    lote_files = sorted(lotes_dir.glob("lote_*.sql"))
    if not lote_files:
        print(f"ERROR: No se encontraron archivos lote_*.sql en {LOTES_DIR}")
        return

    print(f"Leyendo {len(lote_files)} archivos de lotes...")
    all_cards = []
    for lf in lote_files:
        cards = parse_lote(str(lf))
        all_cards.extend(cards)

    # Verificar IDs únicos (un card puede aparecer en varios lotes solo si hay overlap)
    seen_ids = {}
    dedup_cards = []
    for c in all_cards:
        if c["id"] not in seen_ids:
            seen_ids[c["id"]] = True
            dedup_cards.append(c)

    print(f"Cards parseadas: {len(all_cards)} (únicas: {len(dedup_cards)})")
    if args.min_vistas > 0:
        print(f"Filtrando a cards con ≥ {args.min_vistas} vistas...")

    # ---- Detectar duplicados ----
    result = find_duplicates(dedup_cards, min_vistas=args.min_vistas)

    # ---- Construir reporte ----
    report = build_report(result, exactos_only=args.exactos_only)

    # ---- Guardar outputs ----
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    json_path = os.path.join(OUTPUT_DIR, "duplicados_sql.json")
    csv_path  = os.path.join(OUTPUT_DIR, "duplicados_sql.csv")

    save_json(report, json_path)
    save_csv(report, csv_path)

    # ---- Imprimir resumen ----
    print_summary(report)

    print("\n✅ Análisis completado.")
    print(f"   Revisa {csv_path} para abrir en Excel.")


if __name__ == "__main__":
    main()
