# Metabase Audit Agent

> Automated pipeline that audits Metabase instances for broken SQL queries, duplicate metrics, and data-quality issues — and produces a living metrics dictionary as output.

---

## Problem

Data teams using Metabase accumulate hundreds of cards and dashboards over time. Manually auditing them is slow and inconsistent:
- Broken SQL queries go unnoticed until a stakeholder reports a broken dashboard
- Duplicate metrics with subtly different definitions erode trust in numbers
- There is no canonical source of truth for what each metric actually measures

---

## Solution

This agent connects to a Metabase instance via its REST API, pulls all cards and dashboards, and runs them through an LLM-powered analysis pipeline that:

1. **Detects broken queries** — identifies SQL errors, missing table references, and deprecated patterns
2. **Finds duplicate metrics** — clusters cards by semantic similarity to surface redundancy
3. **Checks freshness** — flags cards that haven't been viewed or updated in a configurable threshold
4. **Builds a metrics dictionary** — enriches each card with a natural-language description, owner, tags, and quality score
5. **Applies remediations** — can archive stale cards, fix common SQL anti-patterns, and add filter parameters

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        Metabase REST API                    │
└────────────────────────┬────────────────────────────────────┘
                         │  cards, dashboards, collections
                         ▼
┌─────────────────────────────────────────────────────────────┐
│  src/core/extraction/      Fetch & snapshot                 │
│  extract_new_cards.py      extraer_dashboard.py             │
└────────────────────────┬────────────────────────────────────┘
                         │
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────────┐
│  analysis/   │ │   catalog/   │ │   remediation/   │
│  Obsolescence│ │  Dictionary  │ │  Fix & archive   │
│  Duplicates  │ │  Enrichment  │ │  Country filter  │
└──────┬───────┘ └──────┬───────┘ └──────────────────┘
       │                │
       └───────┬────────┘
               ▼
┌─────────────────────────────────────────────────────────────┐
│  src/core/audit/audit_agent.py                              │
│  LLM-powered review (OpenAI GPT-4o / Claude API)           │
│  — classifies each card as OK, WARNING, or ERROR           │
│  — generates natural-language audit notes                   │
└─────────────────────────────────────────────────────────────┘
               │
               ▼
┌─────────────────────────────────────────────────────────────┐
│  data/processed/  (gitignored)                              │
│  ├── auditoria/        Audit results per card               │
│  └── diccionario/      Metrics dictionary (CSV + wiki)      │
└─────────────────────────────────────────────────────────────┘
```

**Execution flow:**
1. `extraction/` fetches cards from Metabase API → raw snapshots
2. `audit/audit_agent.py` sends each card's SQL + metadata to the LLM → structured verdict
3. `catalog/pipeline.py` aggregates verdicts into the metrics dictionary
4. `analysis/` runs statistical checks (duplicates, obsolescence)
5. `remediation/` applies approved fixes back to Metabase via the API

---

## Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| LLM backend | OpenAI GPT-4o (default) · pluggable for Claude API |
| Metabase API | `requests` — REST v2, session-token auth |
| Data layer | `pandas` · `psycopg2` (direct DB queries for schema introspection) |
| Config | `python-dotenv` · `config/api_key.env` (gitignored) |
| Dependencies | `requirements.txt` |

The LLM call is isolated in `src/core/audit/audit_agent.py`. To swap providers, replace the `openai.ChatCompletion` call with any API that accepts a system prompt + user message.

---

## How to Run

### 1. Clone and install

```bash
git clone https://github.com/your-username/metabase-audit-agent.git
cd metabase-audit-agent
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example config/api_key.env
# Edit config/api_key.env with your values
```

Required variables (see `.env.example`):

| Variable | Description |
|---|---|
| `OPENAI_API_KEY` | Your OpenAI API key |
| `METABASE_URL` | Base URL of your Metabase instance |
| `METABASE_SESSION` | Session token (from Metabase login) |
| `DB_HOST/PORT/NAME/USER/PASSWORD` | PostgreSQL connection for schema introspection |

### 3. Run the audit

```bash
# Full pipeline: extract → audit → build dictionary
python -m src.core.catalog.pipeline

# Audit only (no dictionary build)
python -m src.core.audit.audit_agent

# Detect duplicates
python -m src.core.analysis.detectar_duplicados

# Apply remediation (dry-run first)
python -m src.core.remediation.fix_masivo --dry-run
```

---

## Limitations / Next Steps

- **Authentication**: uses Metabase session tokens (not OAuth). Tokens expire and must be refreshed manually.
- **Scale**: tested on instances with ~1 000 cards. Very large instances may need batching tuning in `audit_agent.py`.
- **LLM cost**: a full audit of 1 000 cards costs roughly $2–5 USD with GPT-4o. Caching repeated prompts reduces this significantly.
- **Remediation is opt-in**: no changes are applied to Metabase without an explicit `--apply` flag.

**Planned improvements:**
- OAuth / API key auth support for Metabase Cloud
- Incremental audits (only re-audit cards changed since last run)
- Web dashboard for browsing audit results

---

> **Note:** This is a sanitized public version of an internal tool. Real credentials, company-specific table names, and internal configurations have been replaced with placeholders. The core audit logic is intact.
