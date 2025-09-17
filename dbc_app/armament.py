
from __future__ import annotations

import re
import json
from typing import Iterable, Dict, Any, List, Tuple, Optional
import pandas as pd

import database  # your DB creds + table names live here
import parsing as P  # we import helpers from parsing.py

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


def find_weapon_matches(conn, df_actions: pd.DataFrame) -> pd.DataFrame:
    df = ensure_weapon_list(df_actions.copy())

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


# ----------------------------- Execution -----------------------------
def check_armaments(friendly_assets: Any, enemy_data: Any) -> pd.DataFrame:
    """

    Output:
      df_arm_results with columns:
        ['friendly_id','weapon','weapon_base_code','qty',
         'effectiveness','range','alt_low','alt_high','speed','dependencies']
    """
    # Normalize friendlies
    friendly_list = _ensure_friendly_list(friendly_assets)

    # Classify enemy side from any input shape
    enemy_side = classify_enemy_side(enemy_data)
    if not enemy_side:
        raise ValueError(f"Could not determine enemy side from: {enemy_data!r}")

    rows: List[Dict[str, Any]] = []
    cache: Dict[Tuple[str, str], pd.DataFrame] = {}

    for asset in friendly_list:
        if not isinstance(asset, dict):
            continue  # safety

        fid =  asset.get("callsign")
        fside = classify_friendly_side(asset)
        if not fside:
            continue

        key = (fside, enemy_side)
        if key not in cache:
            cache[key] = fetch_deliverables_df(fside, enemy_side)

        cat_df = cache[key]
        weapons = parse_weapons_field(asset.get("weapon"))
        if not weapons:
            continue

        eff_col = _pick_col(cat_df, CANDIDATES["effectiveness"])
        rng_col = _pick_col(cat_df, CANDIDATES["range"])
        lo_col  = _pick_col(cat_df, CANDIDATES["alt_low"])
        hi_col  = _pick_col(cat_df, CANDIDATES["alt_high"])
        spd_col = _pick_col(cat_df, CANDIDATES["speed"])
        dep_col = _pick_col(cat_df, CANDIDATES["dependencies"])

        for w in weapons:
            match = _match_single_weapon(w, cat_df)
            if match is None:
                continue

            rows.append({
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
            })

    df_arm_results = pd.DataFrame(rows, columns=[
        "friendly_id","weapon","weapon_base_code","qty",
        "effectiveness","range","alt_low","alt_high","speed","dependencies"
    ])
    return df_arm_results


# ----------------------------- Example -----------------------------
if __name__ == "__main__":
    enemy = {
        "id": 43826,
        "CallSign": None,
        "Track Cat": "Surface",
        "Track ID": "Hostile",
        "Aircraft Type": None
    }

    friendly_assets = [{
        "id": "HARPY 02",
        "weapon": "2XAIM-9, 4XAIM-120, 4XGBU-53 SD",
        "aircraft_type": "F-A-22",
        "trackcategory": "air"
    }]

    df = check_armaments(friendly_assets, enemy)
    with pd.option_context("display.max_columns", None, "display.width", 160):
        print(df.head(20))
