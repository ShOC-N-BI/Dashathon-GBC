# match_weapons.py
# - Uses parsing.py to flatten actions and compute domain (ACTION -> ENTITY)
# - Normalizes weapons and matches against the right deliverables table
# - Matching prefers base-code (e.g., GBU-53, GBU-38, AGM-114, AIM-9), with substring fallback

from __future__ import annotations

import re
import json
import pandas as pd
import psycopg2
from psycopg2 import sql

import database           # your DB creds + table names live here
import parsing as P       # we import helpers from parsing.py

DB = dict(
    host=database.DB_HOST,
    port=database.DB_PORT,
    dbname=database.DB_NAME,
    user=database.DB_USER,
    password=database.DB_PASS,
)

# --------------------------------------------------------------------------------------
# Weapon normalization (re-use from parsing.py if you already expose them there)
# --------------------------------------------------------------------------------------

# Expect these in parsing.py. If you haven't exported them, either move them there or keep here.
normalize_name = getattr(P, "normalize_name", None)
tokenize_weapons = getattr(P, "tokenize_weapons", None)

if normalize_name is None:
    def normalize_name(s: str) -> str:
        if not isinstance(s, str):
            return ""
        s = s.strip()
        s = re.sub(r"\s+", " ", s)
        return s.upper()

if tokenize_weapons is None:
    _SPLIT = re.compile(r"[;,/]+")
    _PFX = re.compile(r"^\s*\d+\s*[xX]\s*")
    def tokenize_weapons(w: str | None) -> list[str]:
        if not isinstance(w, str) or not w.strip():
            return []
        return [_PFX.sub("", t).strip() for t in _SPLIT.split(w) if t.strip()]

def ensure_weapon_list(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has weapon_list and weapon_clean columns."""
    if "weapon_list" not in df.columns:
        df["weapon_list"] = df["weapon"].apply(tokenize_weapons)
    if "weapon_clean" not in df.columns:
        df["weapon_clean"] = df["weapon_list"].apply(lambda xs: ", ".join(xs))
    return df

# --------------------------------------------------------------------------------------
# Base-code extraction (GBU-53, GBU-38, AGM-114, AIM-120, etc.)
# --------------------------------------------------------------------------------------

# Examples that become base codes:
#   "GBU-53 SD"  -> GBU-53
#   "GBU38 JDA"  -> GBU-38
#   "AGM-114N"   -> AGM-114
#   "AIM-120C-7" -> AIM-120
#   "GBU-53/B"   -> GBU-53
_BASE_RE = re.compile(r'([A-Z]{2,4})[-\s]?(\d{2,3})')

def to_base_code(text: str | None) -> str | None:
    """Return the first base code like 'GBU-53' from a token; else None."""
    if not isinstance(text, str) or not text.strip():
        return None
    s = normalize_name(text).replace('/', '-')    # treat slash as hyphen
    m = _BASE_RE.search(s)
    return f"{m.group(1)}-{m.group(2)}" if m else None

def all_base_codes(text: str | None) -> set[str]:
    """Return all base codes present in a deliverable name string."""
    if not isinstance(text, str) or not text.strip():
        return set()
    s = normalize_name(text).replace('/', '-')
    return {f"{pfx}-{num}" for (pfx, num) in _BASE_RE.findall(s)}

# --------------------------------------------------------------------------------------
# Deliverables loading / indexing
# --------------------------------------------------------------------------------------

def load_deliverable_index(conn, table_name: str) -> dict:
    """
    Build a per-table index:
      base_index:  dict[base_code] -> list[orig_name]
      names_norm:  list[(norm_name, orig_name)]  # for substring fallback
    """
    q = sql.SQL('SELECT "name" FROM {tbl} WHERE "name" IS NOT NULL;').format(
        tbl=P.qident(table_name)
    )
    df = pd.read_sql_query(q.as_string(conn), conn)
    names = df["name"].dropna().astype(str).tolist()

    # Build base-code index
    base_index: dict[str, list[str]] = {}
    for x in names:
        for b in all_base_codes(x):
            base_index.setdefault(b, []).append(x)

    # Normalized names for substring fallback
    names_norm = [(normalize_name(x), x) for x in names]

    return {"base_index": base_index, "names_norm": names_norm}

# --------------------------------------------------------------------------------------
# Deliverables table selection
#   - Domain is ACTION → ENTITY (as provided by parsing.extract_actions_with_domain)
#   - Lookup key is (entity_family, domain)
#   - entity_family ∈ {'air','ground','maritime'}, with 'land' → 'ground'
# --------------------------------------------------------------------------------------

TABLE_FOR: dict[tuple[str, str], str] = {
    ("air",      "surf_to_air"):  database.red_air_del_s2a,
    ("air",      "air_to_air"):   database.red_air_del_a2a,

    ("ground",   "air_to_surf"):  database.red_ground_del_a2s,
    ("ground",   "surf_to_surf"): database.red_ground_del_s2s,

    ("maritime", "surf_to_surf"): database.red_maritine_del_s2s,
    ("maritime", "air_to_surf"):  database.red_maritine_del_a2s,
}

# --------------------------------------------------------------------------------------
# Matching
# --------------------------------------------------------------------------------------

def find_weapon_matches(conn, df_actions: pd.DataFrame) -> pd.DataFrame:
    df = ensure_weapon_list(df_actions.copy())

    # preload indices (unchanged) ...
    dl_index: dict[str, dict] = {}
    for tbl in sorted(set(TABLE_FOR.values())):
        try:
            dl_index[tbl] = load_deliverable_index(conn, tbl)
        except Exception as e:
            print(f"Note: could not load {tbl}: {e}")
            dl_index[tbl] = {"base_index": {}, "names_norm": []}

    matched_table, matched_weapons, matched_deliverables, has_match = [], [], [], []

    for _, row in df.iterrows():
        ent_family = row.get("entity_family")
        domain = row.get("domain")
        tbl = TABLE_FOR.get((ent_family, domain))

        if not tbl:
            matched_table.append(None)
            matched_weapons.append([])
            matched_deliverables.append([])
            has_match.append(False)
            continue

        idx = dl_index.get(tbl, {"base_index": {}, "names_norm": []})
        base_index: dict[str, list[str]] = idx["base_index"]
        names_norm: list[tuple[str, str]] = idx["names_norm"]

        tokens = row.get("weapon_list") or []
        token_hits: list[str] = []
        deliverable_hits: list[str] = []

        for tok in tokens:
            tok_norm = normalize_name(tok)
            tok_base = to_base_code(tok)
            hit_this_token = False

            if tok_base and tok_base in base_index:
                token_hits.append(tok)
                deliverable_hits.extend(base_index[tok_base])
                hit_this_token = True

            if not hit_this_token and tok_norm:
                subs = [orig for dn_norm, orig in names_norm if tok_norm in dn_norm]
                if subs:
                    token_hits.append(tok)
                    deliverable_hits.extend(subs)

        # de-dup deliverables
        seen = set()
        deliverable_hits = [d for d in deliverable_hits if not (d in seen or seen.add(d))]

        matched_table.append(tbl)
        matched_weapons.append(token_hits)
        matched_deliverables.append(deliverable_hits)
        has_match.append(bool(token_hits))

    # ✅ assign lists with the same length as df
    df["matched_table"] = matched_table
    df["matched_weapons"] = matched_weapons
    df["matched_deliverables"] = matched_deliverables
    df["has_match"] = has_match

    # (optional sanity checks)
    assert len(matched_table) == len(df) == len(matched_weapons) == len(matched_deliverables) == len(has_match)

    return df

# --------------------------------------------------------------------------------------
# Orchestrator
# --------------------------------------------------------------------------------------

def run() -> pd.DataFrame:
    """
    End-to-end:
      1) Use parsing.extract_actions_with_domain to get a tidy actions DF with:
         - entity_family ∈ {'air','ground','maritime'}
         - domain ∈ {'air_to_air','air_to_surf','surf_to_air','surf_to_surf'}  (ACTION → ENTITY)
         - weapon (raw string)
      2) Match weapons against the selected deliverables table
    """
    with psycopg2.connect(**DB) as conn:
        # parsing.extract_actions_with_domain MUST return the new action→entity domain
        actions = P.extract_actions_with_domain(conn, database.mef_data)

        # Keep only rows with resolvable family+domain
        actions = actions[actions["entity_family"].notna() & actions["domain"].notna()].copy()

        checked = find_weapon_matches(conn, actions)
    return checked

if __name__ == "__main__":
    out = run()
    cols = [
        "entity", "timestamp",
        "trackcategory_entity", "trackcategory", "action_kind",
        "entity_family", "domain", "domain_display",
        "weapon", "weapon_clean",
        "matched_table", "matched_weapons", "matched_deliverables", "has_match",
    ]
    print(out[[c for c in cols if c in out.columns]].head(25).to_string(index=False))
