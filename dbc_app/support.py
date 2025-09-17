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
# SEAD 1-2x
# AWAC 1x
# Tanker 1x
# EW (Electronic Warfare) 1x
# Escort 1-2x
# 1-2x Same Effective Aircraft

# -----------------------------
# Support Finders
# -----------------------------

def find_tankers(tanker):
    if 1 in tanker:
        return False
    else:
        tanker_support = tanker
    return tanker_support

# Edit for different 
def find_escort(friendly):
    escort_list = database.query_assets("aircraft_type", "F-A-22")
    min_distance = float("inf")

    for row in escort_list.itertuples():
        # parsed = parse_track_info(escort)
        # print(parsed)
        escort_lat = float(row.latitude)
        escort_lon = float(row.latitude)
        distance = haversine(escort_lat, escort_lon, float(friendly["lat"]), float(friendly["lon"]))
        if distance < min_distance:
            min_distance = distance
            nearest = row

    return nearest

# -----------------------------
# Main Code
# -----------------------------
def gather_support(friendly, target, tanker):
    asset = friendly["bc3_jtn"]
    target_data = parse_track_info(target)
    tankers = find_tankers(tanker)
    escorts = find_escort(friendly)
    if 1 in tanker:
        return False
    else:
        build_report = {
            "escort": {"vcs": escorts.bc3_vcs, "jtn": escorts.bc3_jtn},
            "tankers": {"vcs": tankers["tanker_vcs"], "jtn": tankers["tanker_jtn"]}
        }
    return build_report
