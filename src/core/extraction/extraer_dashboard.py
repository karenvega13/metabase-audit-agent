"""
extraer_dashboard.py
Extrae todas las questions (cards) de un dashboard de Metabase y sus SQLs.
Corre desde Windows: python extraer_dashboard.py
"""

import requests
import json
import getpass
import sys

METABASE_URL = "https://metabase.example.com"
DASHBOARD_ID = int(sys.argv[1]) if len(sys.argv) > 1 else 69


def get_session_token(email, password):
    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": email, "password": password},
        timeout=15
    )
    if resp.status_code != 200:
        print(f"Error autenticando: {resp.status_code} — {resp.text}")
        sys.exit(1)
    return resp.json()["id"]


def get_dashboard(session_token, dashboard_id):
    resp = requests.get(
        f"{METABASE_URL}/api/dashboard/{dashboard_id}",
        headers={"X-Metabase-Session": session_token},
        timeout=15
    )
    resp.raise_for_status()
    return resp.json()


def get_card_sql(session_token, card_id):
    resp = requests.get(
        f"{METABASE_URL}/api/card/{card_id}",
        headers={"X-Metabase-Session": session_token},
        timeout=15
    )
    if resp.status_code != 200:
        return None
    data = resp.json()
    try:
        query_type = data.get("query_type", "")
        if query_type == "native":
            return data["dataset_query"]["native"]["query"]
        else:
            return f"[No es SQL nativo — tipo: {query_type}]"
    except Exception:
        return "[No se pudo extraer SQL]"


def main():
    print("=== Extractor de Dashboard Metabase ===")
    print(f"Dashboard ID: {DASHBOARD_ID}\n")

    email = input("Email Metabase: ").strip()
    password = getpass.getpass("Password Metabase: ")

    print("\nAutenticando...")
    token = get_session_token(email, password)
    print("✓ Autenticado\n")

    print(f"Obteniendo Dashboard {DASHBOARD_ID}...")
    dashboard = get_dashboard(token, DASHBOARD_ID)
    dashboard_name = dashboard.get("name", "Sin nombre")
    print(f"✓ Dashboard: {dashboard_name}\n")

    # Extraer todas las cards del dashboard
    dashcards = dashboard.get("dashcards", [])
    card_ids = []
    for dc in dashcards:
        card = dc.get("card", {})
        if card and card.get("id"):
            card_ids.append({
                "card_id": card["id"],
                "card_name": card.get("name", "Sin nombre"),
                "description": card.get("description", ""),
            })

    # Deduplicar por card_id
    seen = set()
    unique_cards = []
    for c in card_ids:
        if c["card_id"] not in seen:
            seen.add(c["card_id"])
            unique_cards.append(c)

    print(f"Questions encontradas: {len(unique_cards)}\n")

    # Extraer SQL de cada card
    results = []
    for i, c in enumerate(unique_cards, 1):
        cid = c["card_id"]
        name = c["card_name"]
        print(f"  [{i}/{len(unique_cards)}] Card {cid}: {name}")
        sql = get_card_sql(token, cid)
        results.append({
            "card_id": cid,
            "card_name": name,
            "description": c["description"],
            "sql": sql
        })

    # Guardar resultado
    output = {
        "dashboard_id": DASHBOARD_ID,
        "dashboard_name": dashboard_name,
        "total_questions": len(results),
        "cards": results
    }

    output_file = f"dashboard_{DASHBOARD_ID}_queries.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"\n✓ Guardado en: {output_file}")
    print(f"  Total questions extraídas: {len(results)}")

    # También guardar versión legible .txt
    txt_file = f"dashboard_{DASHBOARD_ID}_queries.txt"
    with open(txt_file, "w", encoding="utf-8") as f:
        f.write(f"Dashboard: {dashboard_name} (ID: {DASHBOARD_ID})\n")
        f.write(f"Total questions: {len(results)}\n")
        f.write("=" * 70 + "\n\n")
        for c in results:
            f.write(f"-- Card {c['card_id']}: {c['card_name']}\n")
            if c["description"]:
                f.write(f"-- Descripción: {c['description']}\n")
            f.write(f"{c['sql'] or '[Sin SQL]'}\n\n")
            f.write("-" * 70 + "\n\n")

    print(f"  Versión legible en: {txt_file}")
    print("\nListo. Comparte ambos archivos con Cowork para el análisis.")


if __name__ == "__main__":
    main()
