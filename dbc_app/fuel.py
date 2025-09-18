import database
import math
import re

# -----------------------------
# Aircraft fuel data (lbs/hour consumption, max fuel capacity in lbs)
# -----------------------------
AIRCRAFT_FUEL_DATA = {
    "FA18F": {"consumption_rate": 6500, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "B-2 SPIR": {"consumption_rate": 11000, "max_fuel_capacity": 167000, "cruise_speed": 900},
    "RQ-4": {"consumption_rate": 1200, "max_fuel_capacity": 17500, "cruise_speed": 575},
    "F16J": {"consumption_rate": 5800, "max_fuel_capacity": 7000, "cruise_speed": 850},
    "F-35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700, "cruise_speed": 850},
    "RQ4": {"consumption_rate": 1200, "max_fuel_capacity": 17500, "cruise_speed": 575},
    "MQ9": {"consumption_rate": 400, "max_fuel_capacity": 9000, "cruise_speed": 310},
    "F35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500, "cruise_speed": 850},
    "F-15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000, "cruise_speed": 900},
    "KC46": {"consumption_rate": 12000, "max_fuel_capacity": 212000, "cruise_speed": 850},
    "B-52": {"consumption_rate": 12000, "max_fuel_capacity": 312000, "cruise_speed": 830},
    "EA18G": {"consumption_rate": 6700, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "E-3": {"consumption_rate": 10000, "max_fuel_capacity": 130000, "cruise_speed": 780},
    "RC135VW": {"consumption_rate": 11000, "max_fuel_capacity": 130000, "cruise_speed": 800},
    "F-16": {"consumption_rate": 5800, "max_fuel_capacity": 7000, "cruise_speed": 850},
    "E2D": {"consumption_rate": 5000, "max_fuel_capacity": 5800, "cruise_speed": 600},
    "MQ-9": {"consumption_rate": 400, "max_fuel_capacity": 9000, "cruise_speed": 310},
    "FA18E": {"consumption_rate": 6500, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "MH60S": {"consumption_rate": 1200, "max_fuel_capacity": 6000, "cruise_speed": 270},
    "EC-130": {"consumption_rate": 5200, "max_fuel_capacity": 60000, "cruise_speed": 540},
    "U2": {"consumption_rate": 2500, "max_fuel_capacity": 28000, "cruise_speed": 760},
    "F-35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750, "cruise_speed": 850},
    "C17": {"consumption_rate": 20000, "max_fuel_capacity": 240000, "cruise_speed": 830},
    "MC130": {"consumption_rate": 5200, "max_fuel_capacity": 60000, "cruise_speed": 540},
    "F35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700, "cruise_speed": 850},
    "C-130": {"consumption_rate": 5000, "max_fuel_capacity": 45000, "cruise_speed": 540},
    "B2": {"consumption_rate": 11000, "max_fuel_capacity": 167000, "cruise_speed": 900},
    "F35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750, "cruise_speed": 850},
    "RC-135": {"consumption_rate": 11000, "max_fuel_capacity": 130000, "cruise_speed": 800},
    "F-35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500, "cruise_speed": 850},
    "F-A-22": {"consumption_rate": 8000, "max_fuel_capacity": 18000, "cruise_speed": 850},
    "F-A-18E-F": {"consumption_rate": 6500, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "F16C": {"consumption_rate": 5800, "max_fuel_capacity": 7000, "cruise_speed": 850},
    "MH60R": {"consumption_rate": 1200, "max_fuel_capacity": 6000, "cruise_speed": 270},
    "P8": {"consumption_rate": 8000, "max_fuel_capacity": 100000, "cruise_speed": 820},
    "F22": {"consumption_rate": 8000, "max_fuel_capacity": 18000, "cruise_speed": 850},
    "KC135": {"consumption_rate": 11000, "max_fuel_capacity": 200000, "cruise_speed": 850},
    "C130": {"consumption_rate": 5000, "max_fuel_capacity": 45000, "cruise_speed": 540},
    "B-1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000, "cruise_speed": 900},
    "F15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000, "cruise_speed": 900},
    "P-3": {"consumption_rate": 4200, "max_fuel_capacity": 64000, "cruise_speed": 650},
    "E-2C": {"consumption_rate": 5000, "max_fuel_capacity": 5800, "cruise_speed": 600},
    "E7": {"consumption_rate": 9500, "max_fuel_capacity": 130000, "cruise_speed": 800},
    "B1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000, "cruise_speed": 900},
    "KC-135": {"consumption_rate": 11000, "max_fuel_capacity": 200000, "cruise_speed": 850},
    "P3": {"consumption_rate": 4200, "max_fuel_capacity": 64000, "cruise_speed": 650},
    "B52": {"consumption_rate": 12000, "max_fuel_capacity": 312000, "cruise_speed": 830},
    "EA37B": {"consumption_rate": 4500, "max_fuel_capacity": 41300, "cruise_speed": 740},
    "E3": {"consumption_rate": 10000, "max_fuel_capacity": 130000, "cruise_speed": 780},
    "NaN": {"consumption_rate": 7000, "max_fuel_capacity": 20000, "cruise_speed": 780}
}


REFUEL_SPEEDS = {
    "KC-135": 1000,
    "KC-10": 1200,
    "KC-46": 1100,
}


# -----------------------------
# Utility Functions
# -----------------------------
def get_consumption_rate_mps(speed_mps, aircraft_data):
    """
    Estimate fuel consumption rate based on actual speed in m/s.
    
    Parameters:
        speed_mps (float): Actual speed in meters per second
        aircraft_data (dict): Dictionary with cruise speed and base consumption rate
    
    Returns:
        float: Estimated fuel consumption rate in lbs/hr
    """
    # Convert cruise speed from km/h to m/s
    cruise_speed_mps = aircraft_data["cruise_speed"] * 1000 / 3600
    base_rate = aircraft_data["consumption_rate"]
    
    # Linear scaling based on speed ratio
    rate = base_rate * (speed_mps / cruise_speed_mps)
    return rate


def midpoint_to_tanker(tank_lat, tank_long, asset_lat, asset_long):
    """
    Calculate the midpoint between a tanker and an asset
    """
    midpoint_lat = (tank_lat + asset_lat) / 2
    midpoint_long = (tank_long + asset_long) / 2

    return midpoint_lat, midpoint_long

def midpoint_for_target(calc, target, asset_lat, asset_long):
    """
    Calculate the distance between asset and target from a tanker midpoint
    """
    entity_lat = float(target["Lattitude"])
    entity_long = float(target["Longitude"])
    distance_to_target = haversine(entity_lat, entity_long, calc[0],calc[1])
    distance_to_origin = haversine(asset_lat, asset_long, calc[0],calc[1])

    return distance_to_target, distance_to_origin
    

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on the Earth in km.
    """
    R = 6371  # Earth radius in km
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)

    a = math.sin(delta_phi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c



def find_nearest_tanker(friendly_lat, friendly_lon, tankers, target):
    """
    Returns the closest tanker and distance to it.
    tankers : list of dicts with keys "latitude" and "longitude"
    """
    nearest = None
    min_distance = float("inf")
    entity_lat = float(target["Lattitude"])
    entity_long = float(target["Longitude"])

    for tanker in tankers:
        tanker_lat = float(tanker["latitude"])
        tanker_lon = float(tanker["longitude"])
        distance = haversine(friendly_lat, friendly_lon, tanker_lat, tanker_lon)
        target_distance = haversine(entity_lat, entity_long, tanker_lat, tanker_lon)
        if distance < min_distance:
            min_distance = distance
            nearest = tanker
            tanker_to_target = target_distance

    return nearest, min_distance,tanker_to_target

def parse_track_info(track_string):
    """
    Regex to capture the main ID and all key-value pairs inside parentheses
    """
    match = re.match(r"(\d+)\s*\((.*)\)", track_string)
    if not match:
        return None
       
    track_id = match.group(1)
    key_values = match.group(2)
 
    # Convert key-value pairs to a dictionary
    info_dict = {}
    for kv in key_values.split(","):
        key, value = kv.split(":", 1)
        info_dict[key.strip()] = value.strip()
       
    # Include the main ID as well
    info_dict["ID"] = track_id
    return info_dict


# -----------------------------
# Main Code
# -----------------------------
def analyze_fuel(friendly, target):
    """
    Determines if an asset can reach its target and return.
    Returns:
        3: Can make round trip with current fuel
        2: Cannot with current fuel, but max fuel allows it
        1: Cannot make round trip even at max fuel
    """
    # Asset Identification
    # -----------------------------
    track_id = friendly["bc3_jtn"]
    distance = float(friendly["distance_km"])
    asset_lat = float(friendly["lat"])
    asset_long = float(friendly["lon"])
    all_view = database.query_friendly_asset(track_id)
    speed = all_view["groundspeed"]
    if speed[0] == '0':
         speed = 220
    tankers = database.query_tankers()

    
    
    # Aircraft Data
    # -----------------------------

    

    if all_view.empty or "fuel" not in all_view.columns or "groundspeed" not in all_view.columns:
         current_fuel = 20000
         speed = 220
 
    if all_view.loc[0, "fuel"] is None:
        current_fuel = 20000
    else:
         current_fuel = float(all_view.loc[0, "fuel"])
    if friendly["aircraft_type"] == None or "[null]":
         aircraft_type = "NaN"
    else:
         aircraft_type = friendly["aircraft_type"].upper()

    if aircraft_type not in AIRCRAFT_FUEL_DATA or aircraft_type == None:
        aircraft_type = "NaN"  # Unknown aircraft, assume worst case

    cruisespeed = AIRCRAFT_FUEL_DATA[aircraft_type]["cruise_speed"]
    groundspeed = cruisespeed * 3.6
    aircraft_rate = AIRCRAFT_FUEL_DATA[aircraft_type]["consumption_rate"] # Redundant, only use as backup
    aircraft_max = AIRCRAFT_FUEL_DATA[aircraft_type]["max_fuel_capacity"]

    # Trip Functions
    # -----------------------------
    def can_make_round_trip(F, R, D, V):
        time_required = (2 * D) / V
        fuel_needed = R * time_required

        return F >= fuel_needed
    def can_make_tanker_trip(F, R, D, V):
        time_required = D / V
        fuel_needed = R * time_required
        return F >= fuel_needed

    def build_report(score, tanker):

        return_report = {
            "score": score,
            "tanker_jtn": tanker["bc3_jtn"],
            "tanker_vcs": tanker["bc3_vcs"],
            "tanker_callsign": tanker["callsign"]
        }
        return return_report

# -----------------------------
# Logic calls for variable consumption rate, data for the nearest fuel tankers, and distance to target calculations
# -----------------------------
    aircraft_consumption_rate = get_consumption_rate_mps(speed, AIRCRAFT_FUEL_DATA[aircraft_type])
    nearest_tanker, distance_to_tanker, target_distance = find_nearest_tanker(asset_lat, asset_long, tankers, parse_track_info(target))
    midpoint_calc = midpoint_to_tanker(nearest_tanker["latitude"], nearest_tanker["longitude"], asset_lat, asset_long)
    distance_to_target, distance_to_origin = midpoint_for_target(midpoint_calc, parse_track_info(target), asset_lat, asset_long)

    if can_make_round_trip(current_fuel, aircraft_consumption_rate[0], distance, groundspeed):
            return build_report(4, nearest_tanker)  # Can make it with current fuel
    elif can_make_round_trip(current_fuel, aircraft_consumption_rate[0], distance_to_tanker, groundspeed):
            # Can make it to the tanker
            if can_make_tanker_trip(aircraft_max, aircraft_consumption_rate[0], (distance_to_target + distance + distance_to_origin), groundspeed):
                return build_report(3, nearest_tanker["bc3_vcs"], nearest_tanker["bc3_jtn"]) (distance_to_target + distance + distance_to_origin)  # Needs refuel, but max fuel allows it, considers new target distance
            else:
                return build_report(2, nearest_tanker), "Cannot reach target after refuel" # Cannot make round trip even at max fuel       
    else:
            return build_report(2, nearest_tanker), "Cannot make round trip"  # Cannot make round trip even at max fuel


        
        