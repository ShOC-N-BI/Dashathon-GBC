# fuel.py
from __future__ import annotations
import math
from dataclasses import dataclass
from typing import Dict, Tuple, List, Optional, Union, Any
import database
import pandas as pd

# ----------------------------- Utility Functions -----------------------------

def haversine(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """
    Great-circle distance between two (lat, lon) pairs in nautical miles.
    """
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 3440.065  # earth radius in nautical miles

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ----------------------------- Data Classes ----------------------------------

@dataclass
class Aircraft:
    bc3_jtn: str
    aircraft_type: str
    fuel_capacity: float
    fuel_remaining: float
    location: Tuple[float, float]
    groundspeed: float
    heading: float
    vcs: str

    consumption_rate: float = 1.0
    max_fuel_capacity: float = 0.0

    def __post_init__(self):
        atype = (self.aircraft_type or "Unknown").upper()
        fuel_data = AIRCRAFT_FUEL_DATA.get(atype, {"consumption_rate": 7000, "max_fuel_capacity": 10000})
        # Protect against zero / falsy values
        self.consumption_rate = float(fuel_data.get("consumption_rate") or 1.0)
        self.max_fuel_capacity = float(fuel_data.get("max_fuel_capacity") or 0.0)

        # Safeguard numeric fields
        self.fuel_remaining = float(self.fuel_remaining or 0.0)
        self.fuel_capacity = float(self.fuel_capacity or self.max_fuel_capacity or 0.0)
        self.groundspeed = float(self.groundspeed or 0.0)

    def range_remaining(self) -> float:
        """Return remaining range (nautical miles) based on fuel_remaining, consumption_rate, and groundspeed."""
        if self.consumption_rate <= 0 or self.groundspeed <= 0:
            return 0.0
        hours_left = self.fuel_remaining / self.consumption_rate  # hours of flight left
        return hours_left * self.groundspeed

    def max_range(self) -> float:
        """Return maximum possible range with full tanks (nautical miles)."""
        if self.consumption_rate <= 0 or self.groundspeed <= 0:
            return 0.0
        hours_full = self.max_fuel_capacity / self.consumption_rate
        return hours_full * self.groundspeed

@dataclass
class Tanker:
    tanker_id: str
    location: Tuple[float, float]
    tanker_type: str
    refuel_speed: float = 1000.0  # lb/min

    def __post_init__(self):
        self.refuel_speed = float(REFUEL_SPEEDS.get(self.tanker_type, self.refuel_speed))

@dataclass
class Target:
    target_id: str
    location: Tuple[float, float]

# ----------------------------- Constants -------------------------------------

AIRCRAFT_FUEL_DATA = {
    # normalized keys (upper) for matching: values are example placeholders
    "FA18F": {"consumption_rate": 6500, "max_fuel_capacity": 14500},
    "FA18E": {"consumption_rate": 6500, "max_fuel_capacity": 14500},
    "EA18G": {"consumption_rate": 6700, "max_fuel_capacity": 14500},
    "F-35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750},
    "F-35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500},
    "F-35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700},
    "F-15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000},
    "F-16": {"consumption_rate": 5800, "max_fuel_capacity": 7000},
    "F-22": {"consumption_rate": 8000, "max_fuel_capacity": 18000},
    "B-2": {"consumption_rate": 11000, "max_fuel_capacity": 167000},
    "B-52": {"consumption_rate": 12000, "max_fuel_capacity": 312000},
    "B-1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000},
    "MQ-9": {"consumption_rate": 400, "max_fuel_capacity": 9000},
    "RQ-4": {"consumption_rate": 1200, "max_fuel_capacity": 17500},
    "RC-135": {"consumption_rate": 11000, "max_fuel_capacity": 130000},
    "E-3": {"consumption_rate": 10000, "max_fuel_capacity": 130000},
    "E-2": {"consumption_rate": 5000, "max_fuel_capacity": 5800},
    "P-3": {"consumption_rate": 4200, "max_fuel_capacity": 64000},
    "P-8": {"consumption_rate": 8000, "max_fuel_capacity": 100000},
    "KC-135": {"consumption_rate": 11000, "max_fuel_capacity": 200000},
    "KC-46": {"consumption_rate": 12000, "max_fuel_capacity": 212000},
    "C-130": {"consumption_rate": 5000, "max_fuel_capacity": 45000},
    "C-17": {"consumption_rate": 20000, "max_fuel_capacity": 240000},
    "U2": {"consumption_rate": 2500, "max_fuel_capacity": 28000},
    # placeholders
    "NS": {"consumption_rate": 0, "max_fuel_capacity": 0},
}

REFUEL_SPEEDS = {
    "KC-135": 1000,
    "KC-10": 1200,
    "KC-46": 1100,
}

# ----------------------------- Mission Logic ---------------------------------

def find_nearest_tanker(asset: Aircraft, tankers: List[Tanker]) -> Optional[Tanker]:
    """Return the nearest tanker to the asset (or None if no tankers)."""
    if not tankers:
        return None
    return min(tankers, key=lambda t: haversine(asset.location, t.location))

def evaluate_mission(asset: Aircraft, target: Target, tankers: Optional[List[Tanker]] = None) -> int:
    """
    Evaluate mission feasibility and return:
      3 -> Green  (can complete round trip, or can refuel after target)
      2 -> Yellow (must refuel before target)
      1 -> Red    (cannot complete round trip)
    """
    tankers = tankers or []
    distance_to_target = haversine(asset.location, target.location)
    round_trip_distance = distance_to_target * 2.0

    range_remaining = float(asset.range_remaining())
    max_range = float(asset.max_range())

    # If we have an on-route tanker that can extend the mission this function could be
    # expanded. For now, use simple checks analogous to your original logic.
    if range_remaining >= round_trip_distance:
        return 3  # Green
    # can reach target and can refuel after target (i.e., max range with full tank >= round trip)
    if range_remaining >= distance_to_target and max_range >= round_trip_distance:
        return 3  # Green (can refuel after target)
    if max_range >= round_trip_distance:
        return 2  # Yellow (must refuel before target)
    return 1  # Red (cannot complete round trip)

# ----------------------------- Input Parsing Helpers -------------------------

def _row_to_aircraft(row: Union[pd.Series, Dict[str, Any]]) -> Aircraft:
    """Convert a DataFrame row or dict into Aircraft dataclass."""
    get = row.get if isinstance(row, dict) else lambda k, default=None: row.get(k, default) if k in row.index else default

    bc3_jtn = get("bc3_jtn", get("callsign", get("id", "unknown")))
    aircraft_type = str(get("aircraft_type", get("type", "Unknown")))
    fuel_capacity = get("fuel_capacity", get("max_fuel_capacity", 0)) or 0.0
    fuel_remaining = get("fuel_remaining", get("fuel", 0)) or 0.0
    lat = float(get("lat", get("latitude", 0)) or 0.0)
    lon = float(get("lon", get("longitude", 0)) or 0.0)
    groundspeed = get("groundspeed", get("speed", 0)) or 0.0
    heading = get("heading", 0) or 0.0
    vcs = get("vcs", "") or ""

    return Aircraft(
        bc3_jtn=str(bc3_jtn),
        aircraft_type=aircraft_type,
        fuel_capacity=float(fuel_capacity),
        fuel_remaining=float(fuel_remaining),
        location=(lat, lon),
        groundspeed=float(groundspeed),
        heading=float(heading),
        vcs=str(vcs)
    )

def _normalize_friendly_input(friendly: Union[str, pd.DataFrame, Dict, List]) -> Aircraft:
    """
    Accepts:
      - callsign string -> queries database.query_friendly_asset(callsign)
      - pandas.DataFrame -> uses first row
      - dict -> builds Aircraft from dict
      - list[dict] -> picks the first element (you can modify to pick nearest, etc)
    """
    # callsign string: query database (database.query_friendly_asset should return a DataFrame)
    if isinstance(friendly, str):
        df = database.query_friendly_asset(friendly)
        if isinstance(df, pd.DataFrame) and not df.empty:
            return _row_to_aircraft(df.iloc[0])
        # fallback if DB didn't return anything
        raise ValueError(f"No friendly asset found for callsign: {friendly}")

    # pandas DataFrame
    if isinstance(friendly, pd.DataFrame):
        if friendly.empty:
            raise ValueError("Empty DataFrame provided for friendly asset")
        return _row_to_aircraft(friendly.iloc[0])

    # list of dicts (like your example)
    if isinstance(friendly, list):
        if not friendly:
            raise ValueError("Empty list provided for friendly asset")
        first = friendly[0]
        if isinstance(first, dict):
            return _row_to_aircraft(first)
        raise TypeError("List elements must be dicts representing assets")

    # dict
    if isinstance(friendly, dict):
        return _row_to_aircraft(friendly)

    raise TypeError("Unsupported type for friendly asset input")

def _normalize_target_input(target: Union[int, str, Dict[str, Any]]) -> Target:
    """
    Accepts:
      - int or str -> create a Target with that id and no coordinates (0,0)
      - dict -> expect keys like 'latitude'/'longitude' or 'lat'/'lon'
    """
    if isinstance(target, (int, str)):
        return Target(target_id=str(target), location=(0.0, 0.0))

    if isinstance(target, dict):
        lat = float(target.get("latitude", target.get("lat", 0)) or 0.0)
        lon = float(target.get("longitude", target.get("lon", 0)) or 0.0)
        target_id = str(target.get("id", target.get("target_id", target.get("track_id", "unknown"))))
        return Target(target_id=target_id, location=(lat, lon))

    raise TypeError("Unsupported type for target input")

# ----------------------------- External Interface -----------------------------

def analyze_fuel(friendly: Union[str, pd.DataFrame, Dict, List],
                 target: Union[int, str, Dict[str, Any]]) -> int:
    """
    Public function used by app.py.

    Usage examples:
      results_fuel = fuel.analyze_fuel(friendly, target)

    friendly can be:
      - callsign string (will call database.query_friendly_asset)
      - DataFrame (first row used)
      - dict describing the asset
      - list of dicts (first element used)

    target can be:
      - int or str (ID only)
      - dict with 'latitude'/'longitude' (or 'lat'/'lon')

    Returns mission status int: 3 (Green), 2 (Yellow), 1 (Red)
    """
    # Normalize inputs
    aircraft = _normalize_friendly_input(friendly)
    tgt = _normalize_target_input(target)

    # If target has no coordinates, attempt to fetch them from DB (best-effort)
    if tgt.location == (0.0, 0.0):
        # If target is an ID string/number, you could try to fetch its coordinates from DB.
        # That behavior depends on your database.schema. For now we keep (0,0) if not provided.
        pass

    return evaluate_mission(aircraft, tgt)
