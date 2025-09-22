from __future__ import annotations
import re
import json
from typing import Iterable, Dict, Any, List, Optional
import pandas as pd

import database


# ----------------------------- Classifiers -----------------------------
def _norm_text(s: Optional[str]) -> str:
    return (s or "").strip().lower()

def classify_side_from_trackcat(raw: Optional[str]) -> Optional[str]:
    """Return one of: 'air', 'land', 'surface' (or None if unknown)."""
    c = _norm_text(raw)
    if not c:
        return None
    if "air" in c:
        return "air"
    if "land" in c or "ground" in c:
        return "land"
    if "surface" in c or "surf" in c or "sea" in c or "marit" in c or "naval" in c:
        return "surface"
    return None

_ENEMY_TRACKCAT_RE = re.compile(r"(?i)\btrack\s*cat\s*:\s*([A-Za-z]+)")

def classify_enemy_side_from_text(enemy_str: str) -> Optional[str]:
    """Supports string inputs like '(... Track Cat: Surface ...)'."""
    m = _ENEMY_TRACKCAT_RE.search(enemy_str or "")
    return classify_side_from_trackcat(m.group(1) if m else None)

# dict-/list-/string-safe enemy classifier
def _extract_trackcat_from_dict(d: dict) -> Optional[str]:
    for k, v in d.items():
        key = k.replace(" ", "").replace("_", "").lower()
        if key in {"trackcat", "trackcategory", "trackcategoryenemy", "entitytrackcategory"} and isinstance(v, str):
            return v
        if isinstance(v, dict):
            sub = _extract_trackcat_from_dict(v)
            if sub:
                return sub
    for k, v in d.items():
        if isinstance(k, str) and "track" in k.lower() and "cat" in k.lower() and isinstance(v, str):
            return v
    return None

def classify_enemy_side(enemy_data: Any) -> Optional[str]:
    """
    Accepts string, dict, or a list/tuple containing either.
    """
    if isinstance(enemy_data, dict):
        return classify_side_from_trackcat(_extract_trackcat_from_dict(enemy_data))
    if isinstance(enemy_data, (list, tuple)):
        for item in enemy_data:
            side = classify_enemy_side(item)
            if side:
                return side
        return None
    if isinstance(enemy_data, str):
        return classify_enemy_side_from_text(enemy_data)
    return None

def classify_friendly_side(asset: Dict[str, Any]) -> Optional[str]:
    # Prefer explicit trackcategory
    t = asset.get("trackcategory")
    if isinstance(t, str):
        k = classify_side_from_trackcat(t)
        if k:
            return k
    # Heuristic: aircraft_type implies 'air'
    if asset.get("aircraft_type"):
        return "air"
    return None

def bc3_jtn_for_asset(asset: Dict[str, Any]) -> Optional[str]:
    """Pick a unique-ish identifier for the asset: prefer bc3_jtn, then id, then callsign."""
    jtn = asset.get("bc3_jtn")
    if isinstance(jtn, str) and jtn.strip():
        return jtn.strip()
    id_ = asset.get("id")
    if isinstance(id_, str) and id_.strip():
        return id_.strip()
    callsign = asset.get("callsign")
    if isinstance(callsign, str) and callsign.strip():
        return callsign.strip()
    return None


# ----------------------------- Input normalization (friendlies only) -----------------------------
def _ensure_friendly_list(friendly_assets: Any) -> List[Dict[str, Any]]:
    """
    Normalize friendly_assets into a list of dicts.
    Accepts: dict, list[dict|str|json], json-string, bare ID string.
    """
    if friendly_assets is None:
        return []

    if isinstance(friendly_assets, dict):
        return [friendly_assets]

    if isinstance(friendly_assets, str):
        s = friendly_assets.strip()
        if s.startswith("{") or s.startswith("["):
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    return [obj]
                if isinstance(obj, list):
                    return [x for x in obj if isinstance(x, dict)]
            except Exception:
                pass
        return [{"id": s}]

    if isinstance(friendly_assets, (list, tuple)):
        out: List[Dict[str, Any]] = []
        for item in friendly_assets:
            if isinstance(item, dict):
                out.append(item)
            elif isinstance(item, str):
                s = item.strip()
                if s.startswith("{") or s.startswith("["):
                    try:
                        obj = json.loads(s)
                        if isinstance(obj, dict):
                            out.append(obj)
                        elif isinstance(obj, list):
                            out.extend([x for x in obj if isinstance(x, dict)])
                        else:
                            out.append({"id": s})
                    except Exception:
                        out.append({"id": s})
                else:
                    out.append({"id": s})
        return out

    return []


# ----------------------------- Deliverables query routing -----------------------------
# (friendly_side, enemy_side)
_QUERY_MAP = {
    ("air", "air"):        database.query_red_air_del_a2a,
    ("air", "land"):       database.query_red_ground_del_a2s,
    ("air", "surface"):    database.query_red_maritime_del_a2s,  # air→surface (maritime)
    ("surface", "air"):    database.query_red_air_del_s2a,       # surface→air
    ("land", "surface"):   database.query_red_maritime_del_s2s,  # per your rule
    ("ground", "surface"): database.query_red_maritime_del_s2s,  # alias
    ("land", "air"):       database.query_red_air_del_s2a,       # land→air
}

def fetch_deliverables_df(friendly_side: str, enemy_side: str) -> pd.DataFrame:
    key = (friendly_side, enemy_side)
    func = _QUERY_MAP.get(key)
    if not func:
        raise ValueError(
            f"No deliverables mapping for friendly='{friendly_side}' vs enemy='{enemy_side}'. "
            f"Known keys: {list(_QUERY_MAP.keys())}"
        )
    df = func()
    return _ensure_base_codes(_ensure_string_deliverable_col(df))


# ----------------------------- Weapon parsing -----------------------------
_SPLIT = re.compile(r"[;,/]+")
_QTY_RE = re.compile(r"^\s*(\d+)\s*[xX]\s*(.+?)\s*$")
_BASE_RE = re.compile(r"([A-Z]{2,4})[-\s]?(\d{2,3})")

def _normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip()).upper().replace("/", "-")

def parse_weapons_field(weapon_field: Optional[str]) -> List[Dict[str, Any]]:
    """
    "2XAIM-9, 4XAIM-120, 4XGBU-53 SD" → [
        {"qty": 2, "name": "AIM-9", "name_norm": "AIM-9", "base_code": "AIM-9"},
        {"qty": 4, "name": "AIM-120", "name_norm": "AIM-120", "base_code": "AIM-120"},
        {"qty": 4, "name": "GBU-53 SD", "name_norm": "GBU-53 SD", "base_code": "GBU-53"},
    ]
    """
    if not isinstance(weapon_field, str) or not weapon_field.strip():
        return []
    results: List[Dict[str, Any]] = []
    for raw in _SPLIT.split(weapon_field):
        token = raw.strip()
        if not token:
            continue
        qty = 1
        name = token
        m = _QTY_RE.match(token)
        if m:
            qty = int(m.group(1))
            name = m.group(2).strip()

        name_norm = _normalize_name(name)
        base = None
        mb = _BASE_RE.search(name_norm)
        if mb:
            base = f"{mb.group(1)}-{mb.group(2)}"
        results.append({"qty": qty, "name": name.strip(), "name_norm": name_norm, "base_code": base})
    return results


# ----------------------------- Deliverables introspection -----------------------------
def _first_string_col(df: pd.DataFrame) -> Optional[str]:
    for c in df.columns:
        if pd.api.types.is_string_dtype(df[c]):
            return c
    return None

def _deliverable_text_col(df: pd.DataFrame) -> Optional[str]:
    for c in ("weapon", "deliverable", "name", "munitions", "title"):
        if c in df.columns:
            return c
    return _first_string_col(df)

def _ensure_string_deliverable_col(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["deliverable_raw", "base_codes"])
    col = _deliverable_text_col(df)
    if not col:
        return pd.DataFrame(columns=["deliverable_raw", "base_codes"])
    out = df.copy()
    if "deliverable_raw" not in out.columns:
        out = out.rename(columns={col: "deliverable_raw"})
    return out

def _all_base_codes(text: Optional[str]) -> set[str]:
    if not isinstance(text, str) or not text.strip():
        return set()
    s = _normalize_name(text)
    return {f"{p}-{n}" for (p, n) in _BASE_RE.findall(s)}

def _ensure_base_codes(df: pd.DataFrame) -> pd.DataFrame:
    if "base_codes" not in df.columns:
        df = df.copy()
        df["base_codes"] = df["deliverable_raw"].apply(_all_base_codes)
    return df

CANDIDATES = {
    "effectiveness": [ "effectiveness_percentage"],
    "range":        ["range", "range_nm" ],
    "alt_low":      ["alt_low_kft", "min_alt", "min_alt_ft", "altitude_min", "alt_min", "alt(kts)", "employment_alt_kft", "employment_alt_low_kft"],
    "alt_high":     ["alt_high_kft", "max_alt", "max_alt_ft", "altitude_max", "alt_max", "alt(kts)", "employment_alt_kft", "employment_alt_high_kft"],
    "speed":        ["speed","speed_kts", "speed(kts)"],
    "dependencies": ["dependencies"],
}

def _pick_col(df: pd.DataFrame, names: Iterable[str]) -> Optional[str]:
    for n in names:
        if n in df.columns:
            return n
    return None


# ----------------------------- Matching -----------------------------
def _match_single_weapon(weap: Dict[str, Any], cat_df: pd.DataFrame) -> Optional[pd.Series]:
    """Match by base code first; fallback to substring."""
    if cat_df is None or cat_df.empty:
        return None

    base = weap.get("base_code")
    if base:
        hits = cat_df[cat_df["base_codes"].apply(lambda s: base in s if isinstance(s, set) else False)]
        if not hits.empty:
            return hits.iloc[0]

    nameN = weap.get("name_norm") or ""
    if len(nameN) >= 3:
        def _has_sub(txt: Any) -> bool:
            return isinstance(txt, str) and (nameN in _normalize_name(txt))
        hits = cat_df[cat_df["deliverable_raw"].apply(_has_sub)]
        if not hits.empty:
            return hits.iloc[0]

    return None


# ----------------------------- Ammo Needed -----------------------------
def qty_to_reach_threshold(eff_percent: Any, qty: Any, threshold: float = 0.9) -> tuple[float, int, str]:
    """
    Given per-try effectiveness in % (e.g., 33.33) and a max qty,
    iterate n = 1..qty and stop when 1-(1-p)**n >= threshold or n == qty.
    Returns: (cumulative_percent, qty_needed_for_threshold, need_more_note)
      - cumulative_percent: 0..100 with two decimals
      - qty_needed_for_90: n that first reaches >= threshold, else 0
    """
    try:
        p = float(eff_percent) / 100.0
        qmax = int(qty)
    except (TypeError, ValueError):
        return 0.0, 0, ""

    if qmax <= 0 or p <= 0.0:
        return 0.0, 0, ""

    last = 0.0
    qty_needed = 0
    for n in range(1, qmax + 1):
        last = 1 - (1 - p) ** n
        if last >= threshold:
            qty_needed = n
            break

    need_more = ""
    if qty_needed == 0:
        need_more = f"More than {qmax} needed for >{int(threshold*100)}% effectiveness"

    return round(last * 100, 2), qty_needed, need_more


# ----------------------------- Utils for actions & deliverables -----------------------------
RESCUE_HELO_PREFIXES = ("lifeguard", "pelican")

def is_rescue_callsign(fid: Optional[str]) -> bool:
    """Accept callsigns that start with approved rescue prefixes (case-insensitive).
    Examples: 'lifeguard', 'lifeguard61', 'Pelican7'."""
    return isinstance(fid, str) and fid.strip().lower().startswith(RESCUE_HELO_PREFIXES)

def _actions(asset: Dict[str, Any]) -> List[str]:
    """Normalize matched_actions to a lowercased list of strings."""
    a = asset.get("matched_actions")
    if isinstance(a, list):
        return [str(x).strip().lower() for x in a if isinstance(x, (str, int, float))]
    if isinstance(a, (str, int, float)):
        return [str(a).strip().lower()]
    return []

def _has_any_content(val: Any) -> bool:
    """Treat None, '', '-', 'null' as empty. Strings with any other chars count as present."""
    if val is None:
        return False
    if isinstance(val, str):
        s = val.strip().lower()
        return bool(s) and s not in {"-", "none", "null"}
    if isinstance(val, (list, tuple, set, dict)):
        return len(val) > 0
    return bool(val)

def _val_present(x: Any) -> bool:
    # truthy for non-empty strings/numbers and non-NaN values
    return x is not None and not (isinstance(x, float) and pd.isna(x)) and x != ""


# ----------------------------- Reduction (one result per friendly_id) -----------------------------
def _reduce_matches_by_friendly(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each friendly_id:
      - If ANY rows (with a weapon) have total_effectiveness_percent >= 90, keep exactly ONE:
          choose the row with the lowest qty_needed_for_90, then highest total_effectiveness_percent.
      - Else (no individual row reaches 90): keep ALL weapon rows for that friendly_id,
          and label the rows that are part of a greedy 'combined plan' with note='COMBINED PLAN'.
          Also attach:
            - combined_total_effectiveness_percent (same on all plan rows)
            - qty_used (per-weapon shots consumed in plan)
      - If a friendly_id has only note rows (weapon is NaN), keep a single note row.
    """
    if df is None or df.empty:
        return df

    out = df.copy()

    # Coerce to numeric for reliable comparisons/sorting
    out["total_effectiveness_percent"] = pd.to_numeric(out["total_effectiveness_percent"], errors="coerce")
    out["qty_needed_for_90"]          = pd.to_numeric(out["qty_needed_for_90"], errors="coerce")
    out["effectiveness"]              = pd.to_numeric(out["effectiveness"], errors="coerce")
    out["qty"]                        = pd.to_numeric(out["qty"], errors="coerce")

    # Preserve original columns + new combined metrics
    cols = out.columns.tolist()
    for extra in ["combined_total_effectiveness_percent", "qty_used"]:
        if extra not in cols:
            cols.append(extra)

    def choose(group: pd.DataFrame) -> pd.DataFrame:
        # Separate actual matches from pure note rows
        matches = group[~group["weapon"].isna()]
        if matches.empty:
            # Only note rows -> keep one
            return group.iloc[[0]]

        # Case 1: keep a single best row if any individual match reaches >=90%
        ge90 = matches[matches["total_effectiveness_percent"] >= 90.0]
        if not ge90.empty:
            best = ge90.sort_values(
                by=["qty_needed_for_90", "total_effectiveness_percent"],
                ascending=[True, False]
            ).iloc[[0]]
            return best

        # Case 2: combined plan across weapons (keep multiple rows, label + attach combined metrics)
        valid = matches[(matches["effectiveness"].notna()) & (matches["qty"].notna()) & (matches["qty"] >= 1)]
        if valid.empty:
            # Nothing to compute; return all matches unchanged
            return matches

        # Greedy: highest per-shot effectiveness first
        valid = valid.assign(
            p = valid["effectiveness"].astype(float) / 100.0,
            q = valid["qty"].astype(int)
        ).sort_values("p", ascending=False)

        failure = 1.0
        plan_counts: Dict[str, int] = {}

        # Build plan by consuming shots from highest-p first
        for _, r in valid.iterrows():
            p = float(r["p"])
            q = int(r["q"])
            name = str(r["weapon"])

            used = 0
            for _ in range(q):
                if 1.0 - failure >= 0.90:
                    break
                failure *= (1.0 - p)
                used += 1

            if used > 0:
                plan_counts[name] = used

            if 1.0 - failure >= 0.90:
                break

        combined_pct = round((1.0 - failure) * 100.0, 2)

        # Label rows used in the plan and attach combined metrics
        labeled = matches.copy()
        used_mask = labeled["weapon"].astype(str).isin(plan_counts.keys())

        labeled.loc[used_mask, "note"] = "COMBINED PLAN"
        labeled.loc[used_mask, "combined_total_effectiveness_percent"] = combined_pct

        # Per-weapon quantity used in the plan
        labeled["qty_used"] = pd.NA  # default
        for wname, cnt in plan_counts.items():
            labeled.loc[labeled["weapon"].astype(str) == wname, "qty_used"] = cnt

        # Clear combined metric on non-plan rows for clarity
        labeled.loc[~used_mask, "combined_total_effectiveness_percent"] = pd.NA

        return labeled

    reduced = (
        out.groupby(["friendly_id"], dropna=False, group_keys=False).apply(choose).reset_index(drop=True)
    )

    # Preserve original column order (+ combined metrics)
    return reduced[cols]


# ----------------------------- Execution -----------------------------
def check_armaments(friendly_assets: Any, enemy_data: Any) -> str:
    """
    Returns a JSON string:
      {
        "app_code": <int>,
        "results": [ { row... }, ... ]
      }

    Action overrides:
      - destroy: normal domain/weapon logic
      - degrade: requires ea_deliverables present; if present -> app_code 4 & ignore matching; else app_code 2
      - investigate: requires at least one of sensing_deliverables or comm_deliverables; if neither -> app_code 2; else app_code 4 & ignore matching
      - rescue: callsign must start with 'lifeguard' or 'pelican' (case-insensitive); if yes -> app_code 4 & ignore matching; else app_code 2
    """
    # Normalize friendlies
    friendly_list = _ensure_friendly_list(friendly_assets)

    # Classify enemy side
    enemy_side = classify_enemy_side(enemy_data)
    rows: List[Dict[str, Any]] = []
    cache: Dict[tuple[str, str], pd.DataFrame] = {}

    classified_any_friendly = False   # at least one friendly got a determinable side
    matched_any_overall = False       # at least one weapon matched across all assets

    # Track mission overrides from actions per asset
    authorized_action_present = False
    failed_action_present = False

    # Early exit if enemy side undetermined
    if not enemy_side:
        msg = "Could not determine enemy side/domain."
        df_arm_results = pd.DataFrame([{
            "friendly_id": None, "weapon": None, "weapon_base_code": None, "qty": None,
            "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
            "speed": None, "dependencies": None,
            "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
            "note": msg,
            "combined_total_effectiveness_percent": None, "qty_used": None,
            "ea_deliverables": None, "comm_deliverables": None, "sens_deliverables": None,
            "bc3_jtn": None  # no specific friendly to copy from
        }])
        payload = {"app_code": 1, "results": df_arm_results.where(pd.notnull(df_arm_results), None).to_dict("records")}
        return json.dumps(payload, indent=2)

    for asset in friendly_list:
        if not isinstance(asset, dict):
            continue  # safety

        fid = asset.get("callsign")
        fside = classify_friendly_side(asset)
        actions = _actions(asset)

        f_ea_del = asset.get("ea_deliverables")
        f_comm_del = asset.get("comm_deliverables")
        f_sens_del = asset.get("sensing_deliverables")
        f_bc3 = asset.get("bc3_jtn")  # <-- always carry through

        # ---------- bc3_jtn requirement DISABLED ----------
        # (left commented intentionally)
        # identifier = bc3_jtn_for_asset(asset)
        # if not identifier:
        #     rows.append({
        #         "friendly_id": fid, "weapon": None, "weapon_base_code": None, "qty": None,
        #         "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
        #         "speed": None, "dependencies": None,
        #         "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
        #         "note": "Missing bc3_jtn identifier.",
        #         "combined_total_effectiveness_percent": None, "qty_used": None,
        #         "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
        #         "bc3_jtn": f_bc3
        #     })
        #     continue
        # ---------------------------------------------------

        # ---------- ACTION LOGIC OVERRIDES ----------
        if "rescue" in actions:
            if is_rescue_callsign(fid):
                rows.append({
                    "friendly_id": fid,
                    "weapon": None, "weapon_base_code": None, "qty": None,
                    "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                    "speed": None, "dependencies": None,
                    "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                    "note": None,
                    "combined_total_effectiveness_percent": None, "qty_used": None,
                    "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
                    "bc3_jtn": f_bc3
                })
                authorized_action_present = True
                matched_any_overall = True  # prevent "no matches" path
            else:
                rows.append({
                    "friendly_id": fid,
                    "weapon": None, "weapon_base_code": None, "qty": None,
                    "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                    "speed": None, "dependencies": None,
                    "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                    "note": "This asset cannot accomplish this mission.",
                    "combined_total_effectiveness_percent": None, "qty_used": None,
                    "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
                    "bc3_jtn": f_bc3
                })
                failed_action_present = True
            continue  # bypass normal logic for rescue

        if "degrade" in actions:
            if _has_any_content(f_ea_del):
                rows.append({
                    "friendly_id": fid,
                    "weapon": None, "weapon_base_code": None, "qty": None,
                    "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                    "speed": None, "dependencies": None,
                    "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                    "note": "DEGRADE MISSION AUTHORIZED",
                    "combined_total_effectiveness_percent": None, "qty_used": None,
                    "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
                    "bc3_jtn": f_bc3
                })
                authorized_action_present = True
                matched_any_overall = True
            else:
                rows.append({
                    "friendly_id": fid,
                    "weapon": None, "weapon_base_code": None, "qty": None,
                    "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                    "speed": None, "dependencies": None,
                    "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                    "note": "This asset does not have the deliverables to accomplish this mission.",
                    "combined_total_effectiveness_percent": None, "qty_used": None,
                    "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
                    "bc3_jtn": f_bc3
                })
                failed_action_present = True
            continue  # bypass normal logic for degrade

        if "investigate" in actions:
            has_any = _has_any_content(f_sens_del) or _has_any_content(f_comm_del)
            if has_any:
                rows.append({
                    "friendly_id": fid,
                    "weapon": None, "weapon_base_code": None, "qty": None,
                    "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                    "speed": None, "dependencies": None,
                    "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                    "note": "INVESTIGATE MISSION AUTHORIZED",
                    "combined_total_effectiveness_percent": None, "qty_used": None,
                    "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
                    "bc3_jtn": f_bc3
                })
                authorized_action_present = True
                matched_any_overall = True
            else:
                rows.append({
                    "friendly_id": fid,
                    "weapon": None, "weapon_base_code": None, "qty": None,
                    "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                    "speed": None, "dependencies": None,
                    "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                    "note": "This asset does not have the deliverables to accomplish this mission.",
                    "combined_total_effectiveness_percent": None, "qty_used": None,
                    "ea_deliverables": f_ea_del, "comm_deliverables": f_comm_del, "sens_deliverables": f_sens_del,
                    "bc3_jtn": f_bc3
                })
                failed_action_present = True
            continue  # bypass normal logic for investigate
        # ---------- end action overrides ----------

        # If no explicit action or 'destroy' -> proceed with normal matching logic
        if not fside:
            rows.append({
                "friendly_id": fid, "weapon": None, "weapon_base_code": None, "qty": None,
                "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                "speed": None, "dependencies": None,
                "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                "note": "Could not determine friendly asset side/domain.",
                "combined_total_effectiveness_percent": None, "qty_used": None,
                "ea_deliverables": None, "comm_deliverables": None, "sens_deliverables": None,
                "bc3_jtn": f_bc3
            })
            continue
        classified_any_friendly = True

        key = (fside, enemy_side)
        if key not in cache:
            cache[key] = fetch_deliverables_df(fside, enemy_side)

        cat_df = cache[key]
        weapons = parse_weapons_field(asset.get("weapon"))

        if not weapons:
            rows.append({
                "friendly_id": fid, "weapon": None, "weapon_base_code": None, "qty": None,
                "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                "speed": None, "dependencies": None,
                "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                "note": "No parseable weapons provided.",
                "combined_total_effectiveness_percent": None, "qty_used": None,
                "ea_deliverables": None, "comm_deliverables": None, "sens_deliverables": None,
                "bc3_jtn": f_bc3
            })
            continue

        eff_col = _pick_col(cat_df, CANDIDATES["effectiveness"])
        rng_col = _pick_col(cat_df, CANDIDATES["range"])
        lo_col  = _pick_col(cat_df, CANDIDATES["alt_low"])
        hi_col  = _pick_col(cat_df, CANDIDATES["alt_high"])
        spd_col = _pick_col(cat_df, CANDIDATES["speed"])
        dep_col = _pick_col(cat_df, CANDIDATES["dependencies"])

        matched_any_this_asset = False

        for w in weapons:
            match = _match_single_weapon(w, cat_df)
            if match is None:
                continue

            matched_any_this_asset = True
            matched_any_overall = True

            row = {
                "friendly_id": fid,
                "weapon": w["name"],
                "weapon_base_code": w.get("base_code"),
                "qty": w["qty"],
                "effectiveness": (match.get(eff_col) if eff_col else None),
                "range": (match.get(rng_col) if rng_col else None),
                "alt_low": (match.get(lo_col) if lo_col else None),
                "alt_high": (match.get(hi_col) if hi_col else None),
                "speed": (match.get(spd_col) if spd_col else None),
                "dependencies": (match.get(dep_col) if dep_col else None),
                "note": None,
                "combined_total_effectiveness_percent": None,
                "qty_used": None,
                "ea_deliverables": f_ea_del,
                "comm_deliverables": f_comm_del,
                "sens_deliverables": f_sens_del,
                "bc3_jtn": f_bc3
            }

            if _val_present(row.get("effectiveness")) and isinstance(row.get("qty"), (int, float)):
                total_percent, qty_needed, need_more = qty_to_reach_threshold(
                    row.get("effectiveness"), row.get("qty"), threshold=0.9
                )
                row["total_effectiveness_percent"] = total_percent
                row["qty_needed_for_90"] = qty_needed
                if need_more:
                    row["needs_more_note"] = need_more

            rows.append(row)

        if not matched_any_this_asset:
            rows.append({
                "friendly_id": fid, "weapon": None, "weapon_base_code": None, "qty": None,
                "effectiveness": None, "range": None, "alt_low": None, "alt_high": None,
                "speed": None, "dependencies": None,
                "total_effectiveness_percent": None, "qty_needed_for_90": None, "needs_more_note": None,
                "note": "The asset has no armaments that meet the criteria of this engagement.",
                "combined_total_effectiveness_percent": None, "qty_used": None,
                "ea_deliverables": None, "comm_deliverables": None, "sens_deliverables": None,
                "bc3_jtn": f_bc3
            })

    # If we never classified any friendly side at all -> code 1
    if not classified_any_friendly and not authorized_action_present and not failed_action_present:
        df_arm_results = pd.DataFrame(rows, columns=[
            "friendly_id","weapon","weapon_base_code","qty",
            "effectiveness","range","alt_low","alt_high","speed","dependencies",
            "total_effectiveness_percent","qty_needed_for_90","needs_more_note","note",
            "combined_total_effectiveness_percent","qty_used",
            "ea_deliverables","comm_deliverables","sens_deliverables","bc3_jtn"
        ])
        payload = {"app_code": 1, "results": df_arm_results.where(pd.notnull(df_arm_results), None).to_dict("records")}
        return json.dumps(payload, indent=2)

    # Build DF before any reductions
    df_arm_results = pd.DataFrame(rows, columns=[
        "friendly_id","weapon","weapon_base_code","qty",
        "effectiveness","range","alt_low","alt_high","speed","dependencies",
        "total_effectiveness_percent","qty_needed_for_90","needs_more_note","note",
        "combined_total_effectiveness_percent","qty_used",
        "ea_deliverables","comm_deliverables","sens_deliverables","bc3_jtn"
    ])

    # If any authorized action rows exist (rescue/degrade/investigate), short-circuit to app_code 4
    if authorized_action_present:
        payload = {"app_code": 4, "results": df_arm_results.where(pd.notnull(df_arm_results), None).to_dict("records")}
        return json.dumps(payload, indent=2)

    # If any failed action rows exist (and no authorized action), short-circuit to app_code 2
    if failed_action_present:
        payload = {"app_code": 2, "results": df_arm_results.where(pd.notnull(df_arm_results), None).to_dict("records")}
        return json.dumps(payload, indent=2)

    # If absolutely no matches anywhere -> code 2
    if not matched_any_overall:
        df_arm_results = _reduce_matches_by_friendly(df_arm_results)
        payload = {"app_code": 2, "results": df_arm_results.where(pd.notnull(df_arm_results), None).to_dict("records")}
        return json.dumps(payload, indent=2)

    # Reduce with your rules (may mark combined plan rows and fill combined_total_effectiveness_percent/qty_used)
    df_arm_results = _reduce_matches_by_friendly(df_arm_results)

    # Determine final app_code: any outcome reaches ≥90% (single or combined)?
    reached_90_any = False
    if "total_effectiveness_percent" in df_arm_results.columns:
        reached_90_any = reached_90_any or pd.to_numeric(
            df_arm_results["total_effectiveness_percent"], errors="coerce"
        ).ge(90.0).any()

    if "combined_total_effectiveness_percent" in df_arm_results.columns:
        reached_90_any = reached_90_any or pd.to_numeric(
            df_arm_results["combined_total_effectiveness_percent"], errors="coerce"
        ).ge(90.0).any()

    app_code = 4 if reached_90_any else 3
    payload = {"app_code": app_code, "results": df_arm_results.where(pd.notnull(df_arm_results), None).to_dict("records")}
    return json.dumps(payload, indent=2)


# ----------------------------- Example -----------------------------
if __name__ == "__main__":
    enemy = {
        "id": 43826,
        "CallSign": None,
        "Track Cat": "Air",
        "Track ID": "Hostile",
        "Aircraft Type": None
    }

    friendly_assets = [{
        "callsign": "Lifeguard61",                 # rescue prefix OK
        "weapon": "2XAIM-9, 4XAIM-120, 4XGBU-53 SD",
        "aircraft_type": "F-A-22",
        "trackcategory": "air",
        "comm_deliverables": "VHF, UHF, Comm Sat",
        "sensing_deliverables": "AMTI, IMINT 1, ELINT 1",
        "ea_deliverables": "Responsive Noise, DRFM",
        "bc3_jtn": "15486",                        # now carried through in all scenarios
        "matched_actions": ["rescue"]              # try "degrade", "investigate", or "destroy"
    }]

    json_payload = check_armaments(friendly_assets, enemy)
    print(json_payload)
