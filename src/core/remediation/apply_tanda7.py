"""Apply Tanda 7 audit decisions to metrics_master.json + audit_definitions_log.jsonl.

Sesión S65c (2026-04-27). 25 cards (#151-175 del ranking).
Saldo: 2 updated · 15 flagged · 8 no_change.
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

DECISIONS = [
    # ---- Heterogéneas: pre-agregadas o pricing — no_change ----
    (581, "no_change", {}),  # CAC cohort 1 — tablas pre-agregadas
    (582, "no_change", {}),  # CAC cohort 2 — tablas pre-agregadas
    (588, "no_change", {}),  # app_pricing_cohort3_retention pre-agregada
    (621, "no_change", {}),  # app_pricing_cohort1_retention pre-agregada
    (620, "no_change", {}),  # app_pricing_cohort2_retention pre-agregada
    (622, "no_change", {}),  # CAC cohort 3 smoothed — tablas pre-agregadas
    (143, "no_change", {}),  # retention_pivot pre-agregada
    (707, "no_change", {}),  # CAC cohort 4 — tablas pre-agregadas

    # ---- Updated: addendums estándar ----
    (597, "updated", {
        "addendums": ["fecha:created_date"],
        "addendum_text": ADDENDUM_FECHA_CREATED,
    }),  # new_accouting_data, sin placeholders test/fraud
    (843, "updated", {
        "addendums": ["test_fraud_default", "fecha:injected_date"],
        "addendum_text": ADDENDUM_TEST_FRAUD + ADDENDUM_FECHA_INJECTED,
    }),  # Monthly Paid txs and volume

    # ---- "Without peaks" sin lógica implementada (mismo bug que #584) ----
    (585, "flagged", {"reason": (
        "Cohort 2 adq. without peaks (app_pricing). DISCREPANCIA GRAVE: nombre y "
        "definición prometen 'without peaks' pero SQL es idéntico a #560 (count distinct "
        "sobre app_pricing con created_date 2024-03-04..04-09 y counter IN (0,1)) — "
        "NO implementa exclusión de picos. Mismo bug que #584. Compárese con #586/#623 "
        "que sí implementan adjusted_weekly_counts."
    )}),
    (702, "flagged", {"reason": (
        "Cohort 4 adq. without peaks (app_pricing). DISCREPANCIA GRAVE: SQL idéntico a "
        "#559/#560/#561 (sin exclusión de picos). Mismo bug que #584/#585. Hardcoded "
        "2024-05-14..06-17. Tercera card del patrón: el bug 'without peaks no implementado' "
        "afecta sistemáticamente a todos los cohorts excepto #586."
    )}),

    # ---- Cohort users with 3+ transactions ----
    (590, "flagged", {"reason": (
        "Cohort 1 users with 3+ trx (app_pricing). counter IN (0,1) en CTE cohort1_users "
        "incluye usuarios sin transacción. transaction_status filtra solo 4 estados "
        "(Paid, Payable, Client_refund, Cancelled). Sin placeholders test/fraude. "
        "Hardcoded 2024-01-29..03-03."
    )}),
    (591, "flagged", {"reason": (
        "Cohort 2 users with 3+ trx (app_pricing). Mismo patrón que #590: counter IN (0,1), "
        "4 estados, sin placeholders. Hardcoded 2024-03-04..04-09."
    )}),
    (592, "flagged", {"reason": (
        "Cohort 3 users with 3+ trx (app_pricing). counter IN (0,1) + 4 estados + "
        "sin placeholders. ADEMÁS dead code: `NOT (created_date BETWEEN '2024-02-18' "
        "AND '2024-02-24')` aplica una exclusión que cae completamente FUERA del rango "
        "del cohort 3 (2024-04-10..05-13), por tanto siempre es TRUE — copy-paste de "
        "otro cohort. Inútil pero no causa error."
    )}),

    # ---- Stickiness / Avg Trx Cohort 4 ----
    (589, "flagged", {"reason": (
        "Pricing stickiness cohort 3 (app_pricing). transaction_status filtra solo 4 "
        "estados (Paid, Payable, Client_refund, Cancelled). Sin placeholders test/fraude. "
        "Hardcoded 2024-04-10..05-13. counter=1 (no IN (0,1) — distinto del resto del bloque)."
    )}),
    (705, "flagged", {"reason": (
        "Avg Trx/user Cohort 4 (app_pricing). transaction_status filtra solo 4 estados. "
        "Sin placeholders confirmed_fraud/flagged_fraud. Hardcoded 2024-05-14..06-17. "
        "Mismo patrón que #570/#571/#572."
    )}),
    (704, "flagged", {"reason": (
        "Cohort 4 ATV (app_pricing). Mismo patrón que #567/#568/#569: 4 estados, "
        "counter IN (0,1), sin placeholders fraud. Hardcoded 2024-05-14..06-17. "
        "Incoherente con ground truth ATV (counter_paid>=1 + Paid)."
    )}),

    # ---- Hardcoded dates (cards de funnel/leads sobre transaction_details_funnel) ----
    (1099, "flagged", {"reason": (
        "Funnel returning leads/1tu Q2 (transaction_details_funnel, UNION de ~12 status "
        "buckets). Hardcoded `created_date > '2024-12-29'` — no se actualiza con el tiempo. "
        "Hardcoded `user_type_at_trx IN ('lead', '1tu')`. Comentarios `-- AND counter in (0,1) "
        "o AND counter_at_trx = 0` indican lógica incierta sobre filtro de counter."
    )}),
    (1291, "flagged", {"reason": (
        "Returning user conversations Q2 (transaction_details_funnel). Tres filtros "
        "hardcoded: `created_date > '2024-12-29'`, `EXTRACT(YEAR ...) IN (2024, 2025, 2026)`, "
        "`EXTRACT(WEEK ...) <> 52`. La card no se actualiza con cohorts de años posteriores. "
        "Definición de Returning User (≥1 Paid + ≥2 trx) sí está documentada en SQL."
    )}),
    (619, "flagged", {"reason": (
        "Funnel recurring user (transaction_details, 10 UNIONs). Hardcoded `created_date < "
        "'2024-08-01'` — la card está congelada en datos previos a Aug 2024. "
        "Sin placeholders confirmed_fraud/flagged_fraud. Sí incluye los 9 estados authorized "
        "en cada bucket. Datos antiguos: candidato a deprecación o a quitar el filtro hardcoded."
    )}),

    # ---- Filtros incompletos vs 9 authorized ----
    (604, "flagged", {"reason": (
        "Inactive users with 2+ trx. transaction_status IN (...) filtra 8 estados (sin Hold) "
        "vs los 9 authorized. Definición habla de 'transacciones realizadas', sin "
        "especificar criterio. days_since_last_transaction >= 30 hardcoded como criterio "
        "de 'inactivo' es razonable."
    )}),
    (605, "flagged", {"reason": (
        "Churn risk users (transaction_details + users_details). Múltiples issues: "
        "(1) transaction_status filtra 8 estados (sin Hold); (2) sin placeholders "
        "test/fraud (no se pueden excluir test_users); (3) en CTE user_atv el filtro "
        "WHERE + FILTER del COUNT son redundantes; (4) thresholds churn_risk hardcoded "
        "(1.10x, 1.05x ATI); (5) **expone datos personales** (full_name, phone, "
        "email_remitter, age) — chocaría con la regla de 'no señalar personas en dashboards'."
    )}),

    # ---- Hardcoded test_user filter ----
    (2308, "flagged", {"reason": (
        "Daily Trends Leads (transaction_details). Tiene `td.test_user = 'false'` "
        "**hardcoded**, no como placeholder — el dashboard no puede cambiar este filtro. "
        "Patrón inconsistente con el resto de las cards (que usan `[[AND ({{test}} IS NULL "
        "OR test_user = {{test}})]]`). CTE user_first_auth correcta con los 9 authorized. "
        "Lógica complicada con NOT(counter=1 AND status IN authorized) — vale revisar intención."
    )}),

    # ---- Cohorts wow / mvg avg con counter IN (0,1) ----
    (623, "flagged", {"reason": (
        "Pricing cohorts adq. mvg avg (app_pricing). Combina adjusted_weekly_counts "
        "(cohort 3 con exclusión de picos OK) con UNION ALL de Cohort 1, 2, 4 (sin "
        "placeholders, hardcoded date ranges). counter IN (0,1) en todos los cohorts. "
        "Sin placeholders test/fraude. A diferencia de #575, sí incluye Cohort 4."
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

    with LOG.open("a", encoding="utf-8") as f:
        for cid, action, payload in DECISIONS:
            entry = {
                "card_id": cid,
                "action": action,
                "tanda": 7,
                "session": "S65c",
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
