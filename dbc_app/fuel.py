import math
from typing import List, Dict, Tuple

# ----------------------------- Utility Functions -----------------------------

def haversine(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    """Calculate great-circle distance in nautical miles."""
    lat1, lon1 = coord1
    lat2, lon2 = coord2
    R = 3440.065  # Earth radius in nautical miles

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)

    a = math.sin(d_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

# ----------------------------- Data Classes ----------------------------------

class Aircraft:
    def __init__(self, bc3_jtn: str, aircraft_type: str, fuel_capacity: float,
                 fuel_remaining: float, location: Tuple[float, float],
                 groundspeed: float, heading: float, vcs: str):
        self.bc3_jtn = bc3_jtn
        self.aircraft_type = aircraft_type
        self.fuel_remaining = fuel_remaining  # in pounds
        self.location = location
        self.groundspeed = groundspeed  # in knots
        self.heading = heading
        self.vcs = vcs

        fuel_data = AIRCRAFT_FUEL_DATA.get(aircraft_type, {
            "consumption_rate": 7000,
            "max_fuel_capacity": 10000
        })

        self.consumption_rate = fuel_data["consumption_rate"]  # lb/hr
        self.max_fuel_capacity = fuel_data["max_fuel_capacity"]  # lb

    def range_remaining(self) -> float:
        """Calculate range in nautical miles based on current fuel and cruise consumption."""
        hours_remaining = self.fuel_remaining / self.consumption_rate
        return hours_remaining * self.groundspeed

class Tanker:
    def __init__(self, tanker_id: str, location: Tuple[float, float], tanker_type: str):
        self.tanker_id = tanker_id
        self.location = location
        self.refuel_speed = REFUEL_SPEEDS.get(tanker_type, 1000)  # lb/min

class Target:
    def __init__(self, target_id: str, location: Tuple[float, float]):
        self.target_id = target_id
        self.location = location

# ----------------------------- Constants -------------------------------------

AIRCRAFT_FUEL_DATA = {
    "FA18F": {"consumption_rate": 6500, "max_fuel_capacity": 14500},
    "FA18E": {"consumption_rate": 6500, "max_fuel_capacity": 14500},
    "F-A-18E-F": {"consumption_rate": 6500, "max_fuel_capacity": 14500},
    "EA18G": {"consumption_rate": 6700, "max_fuel_capacity": 14500},
    "F-35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750},
    "F-35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500},
    "F-35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700},
    "F35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750},
    "F35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500},
    "F35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700},
    "F-15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000},
    "F15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000},
    "F-16": {"consumption_rate": 5800, "max_fuel_capacity": 7000},
    "F16C": {"consumption_rate": 5800, "max_fuel_capacity": 7000},
    "F16J": {"consumption_rate": 5800, "max_fuel_capacity": 7000},
    "F-A-22": {"consumption_rate": 8000, "max_fuel_capacity": 18000},
    "F22": {"consumption_rate": 8000, "max_fuel_capacity": 18000},
    "B-2 SPIR": {"consumption_rate": 11000, "max_fuel_capacity": 167000},
    "B2": {"consumption_rate": 11000, "max_fuel_capacity": 167000},
    "B-52": {"consumption_rate": 12000, "max_fuel_capacity": 312000},
    "B52": {"consumption_rate": 12000, "max_fuel_capacity": 312000},
    "B-1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000},
    "B1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000},
    "MQ-9": {"consumption_rate": 400, "max_fuel_capacity": 9000},
    "MQ9": {"consumption_rate": 400, "max_fuel_capacity": 9000},
    "RQ-4": {"consumption_rate": 1200, "max_fuel_capacity": 17500},
    "RQ4": {"consumption_rate": 1200, "max_fuel_capacity": 17500},
    "RC135VW": {"consumption_rate": 11000, "max_fuel_capacity": 130000},
    "RC-135": {"consumption_rate": 11000, "max_fuel_capacity": 130000},
    "E-3": {"consumption_rate": 10000, "max_fuel_capacity": 130000},
    "E3": {"consumption_rate": 10000, "max_fuel_capacity": 130000},
    "E2D": {"consumption_rate": 5000, "max_fuel_capacity": 5800},
    "E-2C": {"consumption_rate": 5000, "max_fuel_capacity": 5800},
    "P-3": {"consumption_rate": 4200, "max_fuel_capacity": 64000},
    "P3": {"consumption_rate": 4200, "max_fuel_capacity": 64000},
    "P8": {"consumption_rate": 8000, "max_fuel_capacity": 100000},
    "KC135": {"consumption_rate": 11000, "max_fuel_capacity": 200000},
    "KC-135": {"consumption_rate": 11000, "max_fuel_capacity": 200000},
    "KC46": {"consumption_rate": 12000, "max_fuel_capacity": 212000},
    "C130": {"consumption_rate": 5000, "max_fuel_capacity": 45000},
    "C-130": {"consumption_rate": 5000, "max_fuel_capacity": 45000},
    "MC130": {"consumption_rate": 5200, "max_fuel_capacity": 60000},
    "EC-130": {"consumption_rate": 5200, "max_fuel_capacity": 60000},
    "C17": {"consumption_rate": 20000, "max_fuel_capacity": 240000},
    "U2": {"consumption_rate": 2500, "max_fuel_capacity": 28000},
    "MH60S": {"consumption_rate": 1200, "max_fuel_capacity": 6000},
    "MH60R": {"consumption_rate": 1200, "max_fuel_capacity": 6000},
    "E7": {"consumption_rate": 9500, "max_fuel_capacity": 130000},
    "NS": {"consumption_rate": 0, "max_fuel_capacity": 0},           # Placeholder
    "DIS(265)": {"consumption_rate": 0, "max_fuel_capacity": 0},    # Placeholder
    "EA37B": { "consumption_rate": 4500, "max_fuel_capacity": 41300 
}
}

REFUEL_SPEEDS = {
    "KC-135": 1000,  # lb/min
    "KC-10": 1200,
    "KC-46": 1100
}

# ----------------------------- Mission Logic ---------------------------------

def find_nearest_tanker(asset: Aircraft, tankers: List[Tanker]) -> Tanker:
    return min(tankers, key=lambda t: haversine(asset.location, t.location))

def evaluate_mission(asset: Aircraft, target: Target, tankers: List[Tanker]) -> Dict:
    distance_to_target = haversine(asset.location, target.location)
    range_remaining = asset.range_remaining()

    refuel_required = range_remaining < distance_to_target
    refuel_time = 0
    tanker_assigned = None
    status = "Green"

    if refuel_required:
        tanker = find_nearest_tanker(asset, tankers)
        tanker_assigned = tanker.tanker_id
        refuel_time = 10  # Fixed time for midair refuel

        max_range_with_refuel = (asset.max_fuel_capacity / asset.consumption_rate) * asset.groundspeed
        if range_remaining + max_range_with_refuel >= distance_to_target:
            status = "Yellow"
        else:
            status = "Red"

    return {
        "bc3_jtn": asset.bc3_jtn,
        "vcs": asset.vcs,
        "target_id": target.target_id,
        "status": status,
        "distance_to_target": round(distance_to_target, 2),
        "range_remaining": round(range_remaining, 2),
        "refuel_required": refuel_required,
        "refuel_time_minutes": refuel_time,
        "tanker_assigned": tanker_assigned,
        "notes": "Refuel required before reaching target" if refuel_required else "Asset can reach target without refueling"
    }

# ----------------------------- External Interface -----------------------------

def process_mission(asset_data: Dict, target_data: Dict, tankers: List[Tanker]) -> Dict:
    """
    Accepts raw asset and target data dictionaries, instantiates objects,
    and returns mission evaluation result.
    """
    asset = Aircraft(
        bc3_jtn=asset_data["bc3_jtn"],
        aircraft_type=asset_data["aircraft_type"],
        fuel_capacity=asset_data.get("fuel_capacity", 0),  # optional, not used directly
        fuel_remaining=asset_data["fuel_remaining"],
        location=tuple(asset_data["location"]),
        groundspeed=asset_data["groundspeed"],
        heading=asset_data["heading"],
        vcs=asset_data["vcs"]
    )

    target = Target(
        target_id=target_data["target_id"],
        location=tuple(target_data["location"])
    )

    return evaluate_mission(asset, target, tankers)