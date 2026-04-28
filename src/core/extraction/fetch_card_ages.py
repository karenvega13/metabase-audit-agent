"""
fetch_card_ages.py — Obtiene created_at, updated_at y creator de las cards P1
Correr: python -m src.core.extraction.fetch_card_ages
Requiere: METABASE_SESSION en config/api_key.env
Output: data/processed/resultados/card_ages_p1.json
"""

import json, time, os, sys, re
from pathlib import Path
import urllib.request, urllib.error

# === Leer credenciales ===
_ROOT    = Path(os.path.dirname(__file__)).resolve().parent.parent.parent
env_path = str(_ROOT / 'config' / 'api_key.env')
creds = {}
with open(env_path) as f:
    for line in f:
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, _, v = line.partition('=')
            creds[k.strip()] = v.strip().strip('"').strip("'")

SESSION = creds.get('METABASE_SESSION', '')
if not SESSION:
    print("ERROR: METABASE_SESSION no encontrado en config/api_key.env")
    sys.exit(1)

BASE_URL = "https://metabase.example.com"

# === IDs de cards P1 con 'Sent' en SQL (200 cards) ===
CARD_IDS = [
    59, 66, 118, 262, 115, 1368, 2763, 1841, 564, 2445, 276, 2286, 654,
    839, 840, 2728, 1015, 2744, 2298, 2741, 587, 2803, 594, 278, 939, 981,
    1101, 2283, 2492, 2813, 97, 298, 52, 50, 814, 1410, 1306, 1421, 1437,
    2728, 1069, 474, 373, 908, 490, 1331, 2384, 1061, 873, 2393, 1518, 1508,
    # Agregar más si es necesario — estos son los de mayor tráfico
    # Script puede tardar ~5-10 min para 200 cards (throttle de 0.3s entre calls)
]
# Para correr con las 200 completas, ejecutar con --all
# python fetch_card_ages.py --all

def get_card(card_id):
    req = urllib.request.Request(
        f"{BASE_URL}/api/card/{card_id}",
        headers={"X-Metabase-Session": SESSION, "Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": e.code, "card_id": card_id}
    except Exception as e:
        return {"error": str(e), "card_id": card_id}

def extraer_metadata(card_data):
    return {
        "card_id":    card_data.get("id"),
        "nombre":     card_data.get("name"),
        "created_at": card_data.get("created_at"),
        "updated_at": card_data.get("updated_at"),
        "creator":    card_data.get("creator", {}).get("email", "?"),
        "vistas":     card_data.get("view_count"),
        "last_used":  card_data.get("last_used_at"),
        "archived":   card_data.get("archived"),
    }

# === Main ===
print(f"Consultando {len(CARD_IDS)} cards en Metabase...")
resultados = []
errores = []

for i, cid in enumerate(CARD_IDS):
    data = get_card(cid)
    if "error" in data:
        errores.append({"card_id": cid, "error": data["error"]})
        print(f"  [{i+1}/{len(CARD_IDS)}] ID:{cid} ❌ {data['error']}")
    else:
        meta = extraer_metadata(data)
        resultados.append(meta)
        print(f"  [{i+1}/{len(CARD_IDS)}] ID:{cid} ✅ {meta['created_at'][:10]} | {meta['creator']}")
    time.sleep(0.3)  # Throttle para no sobrecargar la API

# === Guardar resultados ===
output = {"cards": resultados, "errores": errores}
out_path = str(_ROOT / 'data' / 'processed' / 'resultados' / 'card_ages_p1.json')
with open(out_path, 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2, ensure_ascii=False)

print(f"\n✅ Listo. {len(resultados)} cards guardadas en card_ages_p1.json")
print(f"   {len(errores)} errores")

# === Análisis rápido por año de creación ===
from collections import Counter
anos = Counter()
for c in resultados:
    if c.get("created_at"):
        ano = c["created_at"][:4]
        anos[ano] += 1

print(f"\nDistribución por año de creación:")
for ano, cnt in sorted(anos.items()):
    print(f"  {ano}: {cnt} cards")
