"""Apply Tanda 6 audit decisions to metrics_master.json + audit_definitions_log.jsonl.

Sesión S65 (2026-04-27). 25 cards (#126-150 del ranking).
Saldo: 3 updated · 20 flagged · 2 no_change.
"""

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
MASTER = ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
LOG = ROOT / "data" / "processed" / "diccionario" / "audit_definitions_log.jsonl"

NOW = datetime.now(timezone.utc).isoformat()

ADDENDUM_TEST_FRAUD = (
    " Por defecto incluye test_users y transacciones marcadas como fraude "
    "confirmado; pueden excluirse vía filtros del dashboard."
)
ADDENDUM_FECHA_INJECTED = (
    " El filtro temporal aplica sobre `injected_date` (fecha de inyección al sistema)."
)
ADDENDUM_FECHA_CREATED = (
    " El filtro temporal aplica sobre `created_date`."
)

# (card_id, action, payload)
# action ∈ {"updated", "flagged", "no_change"}
# payload:
#   updated   -> {"addendums": [...], "addendum_text": "..."}
#   flagged   -> {"reason": "..."}
#   no_change -> {}
DECISIONS = [
    # ---- Active users (DAU/WAU/MAU) — flagged por ground truth ----
    (354, "flagged", {"reason": (
        "DAU debe usar solo status='Paid' per ground truth cto+data_owner (inicio.md L37). "
        "SQL actual filtra 8 estados (Paid, Payable, Client_refund, Cancelled, Chargeback, "
        "Chargeback_won, Chargeback_lost, Chargeback_unir). Fix SQL ya pendiente en "
        "fix_masivo --solo-s59 con OK data_owner."
    )}),
    (355, "flagged", {"reason": (
        "WAU debe usar solo status='Paid' per ground truth cto+data_owner (inicio.md L37). "
        "SQL actual filtra 8 estados (sin Hold). Mismo bug que #354 — extender fix S59."
    )}),
    (356, "flagged", {"reason": (
        "MAU debe usar solo status='Paid' per ground truth cto+data_owner (inicio.md L37). "
        "SQL actual filtra 8 estados (sin Hold). Mismo bug que #354 — extender fix S59."
    )}),

    # ---- Heterogéneas — updated/no_change ----
    (617, "updated", {
        "addendums": ["test_fraud_default", "fecha:injected_date"],
        "addendum_text": ADDENDUM_TEST_FRAUD + ADDENDUM_FECHA_INJECTED,
    }),
    (1531, "updated", {
        "addendums": ["test_fraud_default", "fecha:created_date"],
        "addendum_text": ADDENDUM_TEST_FRAUD + ADDENDUM_FECHA_CREATED,
    }),
    (593, "updated", {
        "addendums": ["fecha:injected_date"],
        "addendum_text": ADDENDUM_FECHA_INJECTED,
    }),
    (360, "no_change", {}),

    # ---- Pricing cohorts adquisitions (app_pricing) — flagged ----
    (561, "flagged", {"reason": (
        "Pricing cohort 3 adq. (app_pricing). counter IN (0,1) incluye usuarios sin transacción "
        "(counter=0); definición dice 'se registraron Y realizaron su primera transacción'. "
        "Hardcoded date range 2024-04-10..05-13 (intencional por ser cohort fijo). "
        "Sin placeholders test/fraude."
    )}),
    (559, "flagged", {"reason": (
        "Pricing cohort 1 adq. (app_pricing). counter IN (0,1) incluye usuarios sin transacción "
        "vs definición 'se registraron Y realizaron su primera transacción'. "
        "Hardcoded 2024-01-29..03-03. Sin placeholders test/fraude."
    )}),
    (560, "flagged", {"reason": (
        "Pricing cohort 2 adq. (app_pricing). counter IN (0,1) incluye usuarios sin transacción "
        "vs definición 'se registraron Y realizaron su primera transacción'. "
        "Hardcoded 2024-03-04..04-09. Sin placeholders test/fraude."
    )}),

    # ---- Cohort ATV (app_pricing) — flagged ----
    (567, "flagged", {"reason": (
        "Cohort 1 ATV (app_pricing). transaction_status filtra solo 4 estados "
        "(Paid, Payable, Client_refund, Cancelled), incoherente con ground truth ATV "
        "(counter_paid>=1 + Paid) y con los 9 authorized. counter IN (0,1) "
        "incluye usuarios sin transacción. Sin placeholders confirmed_fraud/flagged_fraud. "
        "Hardcoded 2024-01-29..03-03."
    )}),
    (568, "flagged", {"reason": (
        "Cohort 2 ATV (app_pricing). Mismo patrón que #567: 4 estados, counter IN (0,1), "
        "sin placeholders fraud. Hardcoded 2024-03-04..04-09."
    )}),
    (569, "flagged", {"reason": (
        "Cohort 3 ATV (app_pricing). Mismo patrón que #567: 4 estados, counter IN (0,1), "
        "sin placeholders fraud. Hardcoded 2024-04-10..05-13."
    )}),

    # ---- Avg Trx per user (app_pricing) — flagged ----
    (570, "flagged", {"reason": (
        "Avg Trx/user Cohort 1 (app_pricing). transaction_status filtra solo 4 estados "
        "(Paid, Payable, Cancelled, Client_refund). Sin placeholders confirmed_fraud/flagged_fraud. "
        "Hardcoded 2024-01-29..03-03."
    )}),
    (572, "flagged", {"reason": (
        "Avg Trx/user Cohort 3 (app_pricing). Mismo patrón que #570: 4 estados, sin "
        "placeholders fraud. Hardcoded 2024-04-10..05-13."
    )}),
    (571, "flagged", {"reason": (
        "Avg Trx/user Cohort 2 (app_pricing). Mismo patrón que #570: 4 estados, sin "
        "placeholders fraud. Hardcoded 2024-03-04..04-09."
    )}),

    # ---- Pricing cohorts wow / by day / Grow ----
    (573, "flagged", {"reason": (
        "Pricing cohorts adq. wow (UNION ALL de 4 cohorts en app_pricing). counter IN (0,1) "
        "incluye usuarios sin transacción. Sin filtro transaction_status. Sin placeholders "
        "test/fraude. Hardcoded date ranges por diseño (cohorts fijos)."
    )}),
    (575, "flagged", {"reason": (
        "Grow by cohorts (app_pricing). Inconsistencia interna: solo incluye 3 cohorts "
        "(initial_cohort=0,1,2), pero #573 y #574 incluyen Cohort 4 (initial_cohort=3, "
        "2024-05-14..06-17). El cálculo de growth_percentage está incompleto. "
        "Además counter IN (0,1) y sin placeholders."
    )}),
    (574, "flagged", {"reason": (
        "Pricing cohorts adq. by day (UNION ALL de 4 cohorts en app_pricing). counter IN (0,1) "
        "incluye usuarios sin transacción. Sin transaction_status. Sin placeholders test/fraude."
    )}),

    # ---- CAC + without peaks ----
    (583, "no_change", {}),
    (584, "flagged", {"reason": (
        "DISCREPANCIA GRAVE: nombre y definición prometen 'without peaks' pero el SQL es "
        "idéntico a #559 (count distinct user_id sobre app_pricing con created_date "
        "2024-01-29..03-03 y counter IN (0,1)) — NO implementa exclusión de picos. "
        "Compárese con #586, que sí aplica adjusted_weekly_counts. Probable bug SQL."
    )}),
    (580, "flagged", {"reason": (
        "% recurring cohort 3 (app_pricing). BUG en definición: dice 'porcentaje en la "
        "cohorte 1' cuando es cohort 3 (created_date 2024-04-10..05-13). counter IN (0,1) "
        "incluye usuarios sin transacción. Sin placeholders test/fraude."
    )}),
    (586, "flagged", {"reason": (
        "Cohort 3 adq. without peaks (app_pricing). Lógica adjusted_weekly_counts sí "
        "implementada correctamente (excluye semanas 2024-04-15 y 04-22 del promedio). "
        "Pero counter IN (0,1) incluye usuarios sin transacción y sin placeholders test/fraude. "
        "Semanas excluidas hardcoded."
    )}),
    (578, "flagged", {"reason": (
        "% recurring cohort 1 (app_pricing). counter IN (0,1) incluye usuarios sin "
        "transacción. Sin transaction_status, sin placeholders test/fraude. "
        "Hardcoded 2024-01-29..03-03."
    )}),
    (579, "flagged", {"reason": (
        "% recurring cohort 2 (app_pricing). BUG en definición: dice 'porcentaje en la "
        "cohorte 1' cuando es cohort 2 (created_date 2024-03-04..04-09). counter IN (0,1) "
        "incluye usuarios sin transacción. Sin placeholders test/fraude."
    )}),
]


def main():
    master = json.loads(MASTER.read_text(encoding="utf-8"))
    decisions_by_id = {cid: (action, payload) for cid, action, payload in DECISIONS}

    touched = set()
    for metric in master["metrics"]:
        cid = metric.get("primary_card_id")
        if cid not in decisions_by_id or cid in touched:
            continue
        # Solo aplicar a la primera occurrence de cada primary_card_id
        # (cards que aparecen en múltiples lotes solo se editan una vez)
        action, payload = decisions_by_id[cid]
        touched.add(cid)

        if action == "updated":
            metric["business_definition"] = (
                metric.get("business_definition", "").rstrip()
                + payload["addendum_text"]
            )
            metric["certification_status"] = "Certificada por Agente"
            metric["definition_audit"] = {
                "status": "audited",
                "addendum_added": payload["addendums"],
                "audited_at": NOW,
            }
        elif action == "flagged":
            metric["certification_status"] = "En Revisión"
            metric["definition_audit"] = {
                "status": "pending_review",
                "reason": payload["reason"],
                "audited_at": NOW,
            }
        elif action == "no_change":
            metric["certification_status"] = "Certificada por Agente"
            metric["definition_audit"] = {
                "status": "audited",
                "addendum_added": [],
                "audited_at": NOW,
            }
        else:
            raise ValueError(f"unknown action: {action}")

    missing = [cid for cid in decisions_by_id if cid not in touched]
    if missing:
        raise RuntimeError(f"Cards not found in master: {missing}")

    MASTER.write_text(
        json.dumps(master, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Append log entries
    with LOG.open("a", encoding="utf-8") as f:
        for cid, action, payload in DECISIONS:
            entry = {
                "card_id": cid,
                "action": action,
                "tanda": 6,
                "session": "S65",
                "audited_at": NOW,
            }
            if action == "updated":
                entry["addendum_added"] = payload["addendums"]
            elif action == "flagged":
                entry["reason"] = payload["reason"]
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"OK: {len(DECISIONS)} decisions applied.")
    print(f"  updated:   {sum(1 for _, a, _ in DECISIONS if a == 'updated')}")
    print(f"  flagged:   {sum(1 for _, a, _ in DECISIONS if a == 'flagged')}")
    print(f"  no_change: {sum(1 for _, a, _ in DECISIONS if a == 'no_change')}")


if __name__ == "__main__":
    main()
