#parsing of the tables so that the json can be interpreted and compared later.
import json
import pandas as pd
import psycopg2
from psycopg2 import sql
import database
import re

DB = dict(host=database.DB_HOST, port=database.DB_PORT, dbname=database.DB_NAME, user=database.DB_USER, password=database.DB_PASS)

def qident(qualified_name: str):
    parts = [p for p in qualified_name.split('.') if p]
    return sql.SQL('.').join(map(sql.Identifier, parts))

def ensure_list_of_dicts(x):
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, str):
        try:
            v = json.loads(x)
            return v if isinstance(v, list) else []
        except json.JSONDecodeError:
            return []
    return []

def extract_actions_python(conn, table: str) -> pd.DataFrame:
    q = sql.SQL('SELECT entity, "timestamp", actions FROM {tbl};').format(tbl=qident(table))
    df = pd.read_sql_query(q.as_string(conn), conn)

    df["actions"] = df["actions"].apply(ensure_list_of_dicts)

    rows = []
    for _, r in df.iterrows():
        for e in r["actions"]:
            if isinstance(e, dict):
                rows.append({
                    "entity": r["entity"],
                    "timestamp": r["timestamp"],
                    "asset_id": e.get("id"),
                    "weapon": e.get("weapon"),
                    "trackcategory": e.get("trackcategory"),
                })
    return pd.DataFrame(rows, columns=["entity", "timestamp", "asset_id", "weapon", "trackcategory"])

# usage:
with psycopg2.connect(**DB) as conn:
    df_min = extract_actions_python(conn, database.mef_data)
print(df_min.head())

# --- patterns (case-insensitive, flexible spacing) ---


ID_RE  = re.compile(r'(?i)\btrack\s*id\s*:\s*([A-Za-z0-9_-]+)')
CAT_RE = re.compile(r'(?i)\btrack\s*cat\s*:\s*([^,;|\n\r]+)')

def parse_track_fields(text: str):
    if not isinstance(text, str):
        return None, None
    m_id  = ID_RE.search(text)
    m_cat = CAT_RE.search(text)
    track_id  = m_id.group(1).strip()  if m_id  else None
    track_cat = m_cat.group(1).strip() if m_cat else None
    return track_id, track_cat

# Parse into new columns
database.df_mef_data[["track_id", "track_cat"]] = database.df_mef_data["entity"].apply(
    lambda s: pd.Series(parse_track_fields(s))
)

# Filter WHERE track_id == "Hostile" (case-insensitive), safely handling NaNs
mask = database.df_mef_data["track_id"].str.casefold().fillna("").eq("hostile")
hostile_results = database.df_mef_data.loc[
    mask, ["entity", "timestamp", "message", "track_id", "track_cat"]
]

#print(hostile_results.head())
