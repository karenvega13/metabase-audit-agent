"""
src/core/catalog/tagger.py — Clasificación, Smart Tags y niveles de certeza.
"""

# ---------------------------------------------------------------------------
# Mapeo tabla → dominio de negocio
# Basado en analytics_db_schema.json y app_context.md
# ---------------------------------------------------------------------------

_TABLE_TAGS: dict[str, list[str]] = {
    # Core transaccional
    "transaction_details":          ["#transacciones", "#pagos"],
    "transaction_details_funnel":   ["#funnel", "#conversion"],
    "transaction_details_outbound": ["#transacciones", "#pagos"],
    # Usuarios
    "users":                        ["#usuarios"],
    "users_details":                ["#usuarios"],
    "users_kyc":                    ["#usuarios", "#kyc"],
    "merchants":                    ["#merchants"],
    "merchants_details":            ["#merchants"],
    # Financiero
    "fees":                         ["#fees", "#revenue"],
    "fee_rules":                    ["#fees"],
    "fx_rates":                     ["#fx"],
    "mto":                          ["#pagos", "#mto"],
    "payouts":                      ["#payouts"],
    "settlements":                  ["#settlements"],
    # Disputas
    "chargebacks":                  ["#chargebacks", "#fraude"],
    "disputes":                     ["#chargebacks", "#fraude"],
    "refunds":                      ["#reembolsos"],
    # Geografía
    "address":                      ["#geografia"],
    "countries":                    ["#geografia"],
    "cities":                       ["#geografia"],
    # Operaciones
    "notifications":                ["#ops", "#notificaciones"],
    "webhooks":                     ["#ops", "#webhooks"],
    "api_keys":                     ["#ops"],
    "devices":                      ["#ops"],
    # Analítica
    "events":                       ["#eventos"],
    "sessions":                     ["#sesiones"],
}

# Palabras clave en nombre/SQL → tag de dominio
_KEYWORD_TAGS: dict[str, list[str]] = {
    "#ventas":       ["revenue", "volume", "venta", "sale", "tpv", "gmv", "tpv"],
    "#retencion":    ["retenci", "churn", "retention", "activ", "mtu", "mau", "dau", "wau"],
    "#lead-pricing": ["lead pricing", "counter_auth = 1", "counter_auth=1", "1ra txn", "primera transaccion", "lead price"],
    "#user-pricing": ["user pricing", "counter_auth >= 2", "counter_auth>=2", "counter_auth > 1", "user price"],
    "#1tu":          ["1tu", "first transaction user", "counter_paid = 1", "counter_paid=1"],
    "#mtu":          ["mtu", "multi-transaction", "multi transaction", "counter_paid >= 2", "counter_paid>=2"],
    "#nuevo-usuario":["nuevo usuario", "new user", "primer pago", "adquirido", "primera paid", "first paid"],
    "#recurrente":   ["recurrente", "recurring", "returning user", "usuario recurrente"],
    "#conversion":   ["funnel", "conversion", "engag", "onboard", "signup", "registro"],
    "#fraude":       ["fraud", "chargeback", "dispute", "reject", "block"],
    "#fees":         ["fee", "commission", "comisi", "spread", "yield", "margen"],
    "#fx":           ["fx", "exchange", "rate", "tipo_de_cambio", "usd", "mxn"],
    "#geografia":    ["country", "city", "estado", "region", "pais", "latam"],
    "#tiempo":       ["daily", "weekly", "monthly", "diario", "semanal", "mensual", "mtd", "ytd"],
    "#usuarios":     ["user", "usuario", "client", "cliente", "new user", "nuevo usuario"],
    "#merchants":    ["merchant", "comercio", "partner"],
    "#ops":          ["operation", "process", "pending", "hold", "queue", "latency"],
    "#payouts":      ["payout", "disbursement", "desembolso"],
    "#reembolsos":   ["refund", "reembolso", "reversa"],
    "#kpi":          ["kpi", "metric", "rate", "%", "ratio", "tasa"],
}


def generate_tags(card: dict) -> list[str]:
    """
    Genera smart tags basados en:
    1. Tablas referenciadas → dominio de negocio
    2. Nombre de la card y fragmento de SQL → palabras clave
    """
    tags: set[str] = set()

    # 1. Linaje de tablas
    for tbl in card.get("tables", frozenset()):
        for tag in _TABLE_TAGS.get(tbl, []):
            tags.add(tag)

    # 2. Nombre + SQL
    text = (
        card.get("card_name", "").lower()
        + " "
        + card.get("sql_normalized", "")[:600]
    )
    for tag, keywords in _KEYWORD_TAGS.items():
        if any(kw in text for kw in keywords):
            tags.add(tag)

    return sorted(tags)


# ---------------------------------------------------------------------------
# Certification status
# ---------------------------------------------------------------------------

def classify_certification(card: dict, audit: dict, obs: dict) -> str:
    """
    Clasifica la certeza de la métrica:

    Deprecada         → obsolescencia severa, error confirmado grave, o acción=ELIMINAR
    Certificada       → sin hallazgos graves, revisión humana positiva, card activa
    En Revisión       → cualquier otro caso (hallazgos sin resolver, pending, no revisada)
    """
    human_status = audit.get("human_review_status")
    score = audit.get("score_salud", 100)
    hallazgos = audit.get("hallazgos", [])
    uso_cat = obs.get("uso_categoria", "")
    accion = obs.get("accion", "")

    # --- Deprecada ---
    if accion in ("ELIMINAR", "ARCHIVAR"):
        return "Deprecada"
    if uso_cat in ("INACTIVA_12M", "NUNCA_USADA", "NUNCA_USADA_Y_ERRORES"):
        return "Deprecada"
    if human_status == "confirmed_error" and score < 50:
        return "Deprecada"

    # --- Certificada ---
    no_critical = not any(h.get("severidad") == "alta" for h in hallazgos)
    human_ok = human_status in ("intentional", "false_positive")

    if no_critical and (human_ok or not hallazgos):
        if score >= 75:
            return "Certificada por Agente"

    # --- En Revisión ---
    return "En Revisión"
