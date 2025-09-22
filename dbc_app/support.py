import database
import math 
import fuel
import re

# -----------------------------
# Utility Functions
# -----------------------------
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


# Mission Pairing
#----------------
# AWAC 1x
# Tanker 1x
# EW (Electronic Warfare) 1x
# Escort 1-2x

# -----------------------------
# Support Finders
# -----------------------------

def find_tankers(friendly):
    nearest_tanker = None
    tanker_list = database.query_tankers()
    min_distance = float("inf")

    for row in tanker_list:
        # parsed = parse_track_info(escort)
        # print(parsed)
        escort_lat = float(row["latitude"])
        escort_lon = float(row["latitude"])
        distance = haversine(escort_lat, escort_lon, float(friendly["lat"]), float(friendly["lon"]))
        if distance < min_distance:
            min_distance = distance
            nearest_tanker = row
    return nearest_tanker

# Edit for different 
def find_escort(friendly, hostile, target):
    
    escort_list = database.query_assets("weapon","ILIKE", "%AIM-120%")
    escort_distances = []

    for row in escort_list:
        escort_lat = float(row["latitude"])
        escort_lon = float(row["longitude"])  # Make sure this isn't latitude again!
        distance = haversine(escort_lat, escort_lon, float(friendly["lat"]), float(friendly["lon"]))
        escort_distances.append((distance, row))

    # Sort escorts by distance
    escort_distances.sort(key=lambda x: x[0])

    # Select top n + 1 closest escorts
    nearest_escort = [row for _, row in escort_distances[:hostile + 1]]
    escort_report = {
        "escort": [
          {
                "bc3_jtn": escort["bc3_jtn"],
                "bc3_vcs": escort["bc3_vcs"],
                "callsign": escort["callsign"],
                "lat": escort["latitude"],
                "lon": escort["longitude"],
                "aircraft_type": escort["aircraft_type"],
                "distance_km": haversine(
                    escort["latitude"],
                    escort["longitude"],
                    float(target["Latitude"]),
                    float(target["Longitude"])
                ),
            }
        for escort in nearest_escort
        ]
    }
    return escort_report

def find_awac(friendly):
    min_distance = float("inf")
    awacs_list = database.query_awacs()
    for awacs in awacs_list:
        awacs_lat = float(awacs["latitude"])
        awacs_lon = float(awacs["longitude"])
        distance = haversine(float(friendly["lat"]), float(friendly["lon"]), awacs_lat, awacs_lon)
        if distance < min_distance:
            min_distance = distance
            nearest = awacs
    return nearest

def find_ew(friendly):
    min_distance = float("inf")
    ew_list = database.query_ew()
    for ew in ew_list:
        ew_lat = float(ew["latitude"])
        ew_lon = float(ew["longitude"])
        distance = haversine(float(friendly["lat"]), float(friendly["lon"]), ew_lat, ew_lon)
        if distance < min_distance:
            min_distance = distance
            nearest = ew
    return nearest

def find_sead(friendly):
    min_distance = float("inf")
    sead_list = database.query_assets("weapon","ILIKE", "%AGM-88%")
    for sead in sead_list:
        sead_lat = float(sead["latitude"])
        sead_lon = float(sead["longitude"])
        distance = haversine(float(friendly["lat"]), float(friendly["lon"]), sead_lat, sead_lon)
        if distance < min_distance:
            min_distance = distance
            nearest = sead
    return nearest


# -----------------------------
# Main Code
# -----------------------------
def gather_support(friendly, target, hostiles):
    # asset = friendly["bc3_jtn"]
    target_data = parse_track_info(target)
    # tankers = find_tankers(friendly)
    awacs = find_awac(friendly)
    ew = find_ew(friendly)
    sead = find_sead(friendly)
    hostile_code = hostiles[0]
    hostile_data = hostiles[1]
    fuel_report = []

    if hostile_code < 4:
        escort_report = find_escort(friendly, len(hostile_data), target_data)
        for item in escort_report["escort"]:
            fuel_report.append(fuel.analyze_fuel(item, target))
        escorts = escort_report["escort"]
    else:
        escorts = "None"
        fuel_report = "None"

    # build_report = {
    #     "escort": escorts,
    #     "tankers": {"vcs": tankers["bc3_vcs"], "jtn": tankers["bc3_jtn"]}
    # }
    build_report = {
        "escort": escorts,
        "tankers": fuel_report,
        "awacs": awacs,
        "ew": ew,
        "sead": sead,
    }
    # print(build_report)
    return build_report
