"""Fix copy-paste bug en business_definition de #579 y #580.

#579 (% recurring cohort2): "cohorte 1" → "cohorte 2"
#580 (% recurring cohort3): "cohorte 1" → "cohorte 3"

Las fechas hardcoded del SQL ya prueban a qué cohort corresponde cada card.
Es typo evidente, no decisión de negocio. Recertifica las cards (sale de En Revisión).
"""

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MASTER = ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
LOG = ROOT / "data" / "processed" / "diccionario" / "audit_definitions_log.jsonl"

NOW = datetime.now(timezone.utc).isoformat()

REPLACEMENTS = {
    579: ("la cohorte 1 adquirida entre el 4 de marzo", "la cohorte 2 adquirida entre el 4 de marzo"),
    580: ("dentro de la cohorte 1 adquirida entre el 10 de abril",
          "dentro de la cohorte 3 adquirida entre el 10 de abril"),
}


def main():
    master = json.loads(MASTER.read_text(encoding="utf-8"))
    touched = set()
    for metric in master["metrics"]:
        cid = metric.get("primary_card_id")
        if cid not in REPLACEMENTS or cid in touched:
            continue
        old_str, new_str = REPLACEMENTS[cid]
        bd = metric.get("business_definition", "")
        if old_str not in bd:
            raise RuntimeError(f"Card {cid}: substring '{old_str}' not found in business_definition")
        metric["business_definition"] = bd.replace(old_str, new_str)
        metric["certification_status"] = "Certificada por Agente"
        metric["definition_audit"] = {
            "status": "audited",
            "addendum_added": ["fix_copypaste_cohorte_label"],
            "audited_at": NOW,
            "previous_status": "pending_review",
            "fix_note": f"Copy-paste corregido: 'cohorte 1' → 'cohorte {2 if cid==579 else 3}'.",
        }
        touched.add(cid)

    if touched != set(REPLACEMENTS):
        raise RuntimeError(f"Cards not found: {set(REPLACEMENTS) - touched}")

    MASTER.write_text(
        json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    with LOG.open("a", encoding="utf-8") as f:
        for cid in REPLACEMENTS:
            entry = {
                "card_id": cid,
                "action": "fixed_definition",
                "tanda": 6,
                "session": "S65b",
                "audited_at": NOW,
                "fix_type": "copypaste_cohorte_label",
                "previous_status": "pending_review",
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"OK: 2 cards re-certified (#579, #580).")


if __name__ == "__main__":
    main()
