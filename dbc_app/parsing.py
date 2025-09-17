# match_weapons.py
# Flatten actions, compute domain (action→entity), normalize weapons,
# and match weapons using (entity_family, domain).

import re
import json
import pandas as pd
import psycopg2
from psycopg2 import sql

import database  # must expose DB_HOST/PORT/NAME/USER/PASS and your deliverable table names

DB = dict(
    host=database.DB_HOST,
    port=database.DB_PORT,
    dbname=database.DB_NAME,
    user=database.DB_USER,
    password=database.DB_PASSWORD,
)


# ----------------------------- SQL identifier helper -----------------------------
def qident(qualified_name: str) -> sql.Composed:
    if not isinstance(qualified_name, str):
        raise TypeError("Identifier must be a string")
    parts = [p for p in qualified_name.split(".") if p]
    return sql.SQL(".").join(map(sql.Identifier, parts))


# ----------------------------- JSON & text helpers -----------------------------
def ensure_list_of_dicts(x):
    if x is None:
        return []
    if isinstance(x, list):
        return [e for e in x if isinstance(e, dict)]
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return [e for e in v if isinstance(e, dict)] if isinstance(v, list) else []
        except json.JSONDecodeError:
            return []
    return []


_ENTITY_CAT_RE = re.compile(r"(?i)\btrack\s*cat\s*:\s*([^,;|\n\r]+)")


def parse_entity_track_cat(entity_text):
    if not isinstance(entity_text, str):
        return None
    m = _ENTITY_CAT_RE.search(entity_text)
    return m.group(1).strip() if m else None


# ---------- normalizers ----------
def entity_family_for_tables(cat_raw):
    """Return entity family for table selection: air | ground | maritime."""
    if not isinstance(cat_raw, str):
        return None
    c = cat_raw.strip().lower()
    if "air" in c:
        return "air"
    if ("land" in c) or ("ground" in c):
        return "ground"
    if any(k in c for k in ("surf", "surface", "sea", "marit", "naval")):
        return "maritime"
    return None


def entity_side_for_domain_mapping(cat_raw):
    """Right-hand side for domain (mapping): air | surf (land/surface → surf)."""
    fam = entity_family_for_tables(cat_raw)
    if fam == "air":
        return "air"
    if fam in {"ground", "maritime"}:
        return "surf"
    return None


def entity_side_for_domain_display(cat_raw):
    """Right-hand side for domain (display): air | surf | surf(land)."""
    if not isinstance(cat_raw, str):
        return None
    c = cat_raw.strip().lower()
    if "air" in c:
        return "air"
    if ("land" in c) or ("ground" in c):
        return "surf(land)"
    if any(k in c for k in ("surf", "surface", "sea", "marit", "naval")):
        return "surf"
    return None


def norm_action_kind(raw):
    """Classify action trackcategory to: air | land | surface."""
    if not isinstance(raw, str):
        return None
    c = raw.strip().lower()
    if c.startswith("air"):
        return "air"
    if c in {"land", "ground"}:
        return "land"
    if c in {"surface", "surf", "sea", "maritime", "naval"}:
        return "surface"
    return None


def action_side_for_domain(kind):
    """Left-hand side (actions) for domain: air | surf (land/surface → surf)."""
    if not isinstance(kind, str):
        return None
    k = kind.strip().lower()
    if k == "air":
        return "air"
    if k in {"land", "surface"}:
        return "surf"
    return None


def make_domain_labels(entity_cat_raw, action_kind_raw):
    """
    Domain is ACTION first, then ENTITY.
    Returns (domain_mapping, domain_display) or (None, None)
    where domain_mapping ∈ {air_to_air, air_to_surf, surf_to_air, surf_to_surf}
    and domain_display uses surf(land) on the entity side when applicable.
    """
    left = action_side_for_domain(action_kind_raw)  # action side
    right_map = entity_side_for_domain_mapping(entity_cat_raw)  # entity side (mapping)
    right_disp = entity_side_for_domain_display(entity_cat_raw)  # entity side (display)
    if not (left and right_map and right_disp):
        return None, None
    return f"{left}_to_{right_map}", f"{left}_to_{right_disp}"


# ----------------------------- Weapons normalization -----------------------------
def normalize_name(s: str) -> str:
    if not isinstance(s, str):
        return ""
    s = s.strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()


_SPLIT = re.compile(r"[;,/]+")
_PFX = re.compile(r"^\s*\d+\s*[xX]\s*")


def tokenize_weapons(w: str | None) -> list[str]:
    if not isinstance(w, str) or not w.strip():
        return []
    return [_PFX.sub("", t).strip() for t in _SPLIT.split(w) if t.strip()]


def ensure_weapon_list(df: pd.DataFrame) -> pd.DataFrame:
    if "weapon_list" not in df.columns:
        df["weapon_list"] = df["weapon"].apply(tokenize_weapons)
    if "weapon_clean" not in df.columns:
        df["weapon_clean"] = df["weapon_list"].apply(lambda xs: ", ".join(xs))
    return df


_BASE_RE = re.compile(r"([A-Z]{2,4})[-\s]?(\d{2,3})")


def to_base_code(text: str | None) -> str | None:
    """Return the first base code like 'GBU-53' from a token; else None."""
    if not isinstance(text, str) or not text.strip():
        return None
    s = normalize_name(text)  # uppercase + collapse spaces
    s = s.replace("/", "-")  # treat slash as hyphen for codes like GBU-53/B
    m = _BASE_RE.search(s)
    if not m:
        return None
    pfx, num = m.group(1), m.group(2)
    return f"{pfx}-{num}"


def all_base_codes(text: str | None) -> set[str]:
    """Return all base codes found in a deliverable name string."""
    if not isinstance(text, str) or not text.strip():
        return set()
    s = normalize_name(text).replace("/", "-")
    return {f"{pfx}-{num}" for (pfx, num) in _BASE_RE.findall(s)}


# ----------------------------- Flatten actions & compute domains -----------------------------
def extract_actions_with_domain(conn, table: str) -> pd.DataFrame:
    """
    SELECT entity, timestamp, actions
    -> explode actions[]
    -> compute action_kind and domain (action→entity)
    -> return tidy DF including entity family for lookups.
    """
    q = sql.SQL('SELECT entity, "timestamp", actions FROM {tbl};').format(
        tbl=qident(table)
    )
    base = pd.read_sql_query(q.as_string(conn), conn)

    base["actions"] = base["actions"].apply(ensure_list_of_dicts)
    exploded = base.explode("actions", ignore_index=True)
    exploded = exploded[exploded["actions"].notna()].copy()

    flat = pd.json_normalize(exploded["actions"]).rename(
        columns={"id": "asset_id", "trackcategory": "trackcategory_action"}
    )

    entity_raw = exploded["entity"].apply(parse_entity_track_cat)
    entity_family = entity_raw.apply(entity_family_for_tables)
    action_kind = flat["trackcategory_action"].apply(norm_action_kind)

    dom_map, dom_disp = [], []
    for e_raw, a_kind in zip(entity_raw, action_kind):
        m, d = make_domain_labels(e_raw, a_kind)
        dom_map.append(m)
        dom_disp.append(d)

    out = pd.concat(
        [
            exploded[["entity", "timestamp"]].reset_index(drop=True),
            flat[["asset_id", "weapon", "trackcategory_action"]],
            pd.Series(entity_raw, name="trackcategory_entity"),
            pd.Series(entity_family, name="entity_family"),
            pd.Series(action_kind, name="action_kind"),
            pd.Series(dom_map, name="domain"),  # action→entity (mapping)
            pd.Series(dom_disp, name="domain_display"),  # action→entity (display)
        ],
        axis=1,
    )
    # For convenience keep 'trackcategory' as the action-side raw value
    out["trackcategory"] = out["trackcategory_action"]
    return out
