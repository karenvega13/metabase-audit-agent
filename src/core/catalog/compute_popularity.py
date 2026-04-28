"""
compute_popularity.py — Calcula ranking de métricas más consultadas (7d / 30d).

Lee los snapshots en data/processed/diccionario/popularity_snapshots/*.json
(producidos por src/core/extraction/snapshot_popularity.py) y computa deltas
de view_count por ventana. Escribe popularity_ranked.json con el top N.

Modo degradado: si hay <2 snapshots, no se pueden calcular deltas — se
retorna un ranking por view_count lifetime crudo, con degraded=true para
que la UI muestre un disclaimer.

Uso:
    python -m src.core.catalog.compute_popularity

Output: data/processed/diccionario/popularity_ranked.json
"""
from __future__ import annotations

import datetime as _dt
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
SNAPSHOTS_DIR = ROOT / "data" / "processed" / "diccionario" / "popularity_snapshots"
METRICS_PATH = ROOT / "data" / "processed" / "diccionario" / "metrics_master.json"
OUTPUT_PATH = ROOT / "data" / "processed" / "diccionario" / "popularity_ranked.json"
TOP_N = 50
WINDOW_TOLERANCE_DAYS = 3  # ±3 días alrededor del target para snapshots


def _bootstrap_from_metrics_master() -> list[dict]:
    """Fallback: si no hay snapshots, rankea por vistas_primary de metrics_master.json.
    Permite que el hero funcione desde el día 1 sin Metabase.
    """
    if not METRICS_PATH.exists():
        return []
    try:
        data = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
    except Exception:
        return []
    rows = []
    for m in data.get("metrics", []):
        if not m.get("is_primary", True):
            continue
        if m.get("dedup_type") == "DUPLICADO EXACTO" and not m.get("is_primary", True):
            continue
        cid = m.get("primary_card_id")
        v = int(m.get("vistas_primary") or 0)
        if cid is None or v <= 0:
            continue
        rows.append({"card_id": int(cid), "views": v, "last_used_at": m.get("last_used_at") or None})
    rows.sort(key=lambda r: -r["views"])
    return rows[:TOP_N]


def _load_snapshots() -> list[tuple[_dt.date, dict]]:
    """Retorna lista de (fecha, payload) ordenada por fecha ascendente."""
    if not SNAPSHOTS_DIR.exists():
        return []
    out = []
    for f in SNAPSHOTS_DIR.glob("*.json"):
        try:
            d = _dt.date.fromisoformat(f.stem)
        except ValueError:
            continue
        try:
            payload = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        out.append((d, payload))
    out.sort(key=lambda kv: kv[0])
    return out


def _pick_baseline(snapshots: list[tuple[_dt.date, dict]], target: _dt.date) -> tuple[_dt.date, dict] | None:
    """Elige el snapshot más cercano a target dentro de ±WINDOW_TOLERANCE_DAYS."""
    best = None
    best_gap = None
    for d, p in snapshots:
        gap = abs((d - target).days)
        if gap > WINDOW_TOLERANCE_DAYS:
            continue
        if best_gap is None or gap < best_gap:
            best = (d, p)
            best_gap = gap
    return best


def _views_by_card(payload: dict) -> dict[int, int]:
    out: dict[int, int] = {}
    for row in payload.get("cards", []):
        cid = row.get("card_id")
        if cid is None:
            continue
        out[int(cid)] = int(row.get("view_count") or 0)
    return out


def _last_used_by_card(payload: dict) -> dict[int, str | None]:
    return {int(r["card_id"]): r.get("last_used_at") for r in payload.get("cards", []) if r.get("card_id") is not None}


def _rank(deltas: dict[int, int], last_used: dict[int, str | None], top_n: int = TOP_N) -> list[dict]:
    def sort_key(cid: int) -> tuple[int, str]:
        # mayor delta primero; tie-breaker: last_used_at más reciente (string ISO)
        return (-deltas[cid], "" if last_used.get(cid) is None else "z" + str(last_used[cid]))
    # invertimos: último más reciente → prefijamos con 'z' para que ordene mayor
    ordered = sorted(deltas.keys(), key=sort_key)
    return [
        {"card_id": cid, "delta": deltas[cid], "last_used_at": last_used.get(cid)}
        for cid in ordered[:top_n]
    ]


def run() -> dict:
    snapshots = _load_snapshots()

    if not snapshots:
        bootstrap = _bootstrap_from_metrics_master()
        payload = {
            "snapshot_date": _dt.date.today().isoformat(),
            "window_7d": None,
            "window_30d": None,
            "degraded": True,
            "reason": "no_snapshots_bootstrap" if bootstrap else "no_snapshots",
            "top_lifetime": bootstrap,
        }
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"OK (bootstrap desde metrics_master.json, {len(bootstrap)} cards): {OUTPUT_PATH.relative_to(ROOT)}")
        return payload

    today_date, today_payload = snapshots[-1]
    today_views = _views_by_card(today_payload)
    today_last = _last_used_by_card(today_payload)

    # Modo degradado: <2 snapshots → ranking lifetime
    if len(snapshots) < 2:
        top_lifetime = sorted(today_views.items(), key=lambda kv: -kv[1])[:TOP_N]
        payload = {
            "snapshot_date": today_date.isoformat(),
            "window_7d": None,
            "window_30d": None,
            "degraded": True,
            "reason": "single_snapshot",
            "top_lifetime": [
                {"card_id": cid, "views": v, "last_used_at": today_last.get(cid)}
                for cid, v in top_lifetime
            ],
        }
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"OK (degraded, 1 snapshot): {OUTPUT_PATH.relative_to(ROOT)}")
        return payload

    target_7d = today_date - _dt.timedelta(days=7)
    target_30d = today_date - _dt.timedelta(days=30)

    base_7 = _pick_baseline(snapshots[:-1], target_7d)
    base_30 = _pick_baseline(snapshots[:-1], target_30d)

    def _compute_window(base: tuple[_dt.date, dict] | None, target_date: _dt.date, label: str) -> dict | None:
        if base is None:
            return None
        base_date, base_payload = base
        base_views = _views_by_card(base_payload)
        deltas: dict[int, int] = {}
        for cid, v_today in today_views.items():
            v_base = base_views.get(cid, 0)  # card nueva → 0 base
            d = v_today - v_base
            if d > 0:
                deltas[cid] = d
        top = _rank(deltas, today_last)
        for entry in top:
            entry["is_new"] = entry["card_id"] not in base_views
        return {
            "window": label,
            "from": base_date.isoformat(),
            "to": today_date.isoformat(),
            "baseline_gap_days": abs((base_date - target_date).days),
            "top": top,
        }

    payload = {
        "snapshot_date": today_date.isoformat(),
        "window_7d": _compute_window(base_7, target_7d, "7d"),
        "window_30d": _compute_window(base_30, target_30d, "30d"),
        "degraded": base_30 is None and base_7 is None,
    }
    if payload["window_7d"] is None and payload["window_30d"] is None:
        payload["reason"] = "no_baseline_in_tolerance"
        payload["top_lifetime"] = [
            {"card_id": cid, "views": v, "last_used_at": today_last.get(cid)}
            for cid, v in sorted(today_views.items(), key=lambda kv: -kv[1])[:TOP_N]
        ]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"OK: {OUTPUT_PATH.relative_to(ROOT)} (snapshot {today_date}, {len(snapshots)} en histórico)")
    return payload


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
