import database
import math 
import fuel
import re
import armament
import json

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
    min_distance = float("inf")
    
    a = armament.check_armaments(friendly, target)
    print(a)
    escort_list = database.query_assets("trackid","ILIKE", "Friend", friendly["bc3_jtn"])
    escort_distances = []
    test_list = []
    raw = []
    final = []

    for row in escort_list:
        escort_lat = float(row["latitude"])
        escort_lon = float(row["longitude"])  # Make sure this isn't latitude again!
        distance = haversine(escort_lat, escort_lon, float(friendly["lat"]), float(friendly["lon"]))
        # if distance < min_distance:
        #     min_distance = distance
        escort_distances.append((distance, row))

    # Sort escorts by distance
    escort_distances.sort(key=lambda x: x[0])

    for enemy in hostile[1]:
        if len(escort_distances) >= 1:
            for ally in escort_distances:
                test_list = json.loads(armament.check_armaments(ally, enemy))
                if test_list["app_code"] > 2:
                    raw.append(test_list)
                    escort_distances.remove(ally)
                    break
        else:
            return "{'1', 'No Escorts Available' }"
            break
                

    # Select top n + 1 closest escorts
    for listed_escort in raw:
        extract = listed_escort["results"]
        final.append(database.query_friendly_asset(extract[0]["bc3_jtn"]))
    nearest_escort = [row for row in final]
    escort_report = {
        "escort": [
          {
                "bc3_jtn": escort["bc3_jtn"][0],
                "bc3_vcs": escort["bc3_vcs"][0],
                "callsign": escort["callsign"][0],
                "lat": escort["latitude"][0],
                "lon": escort["longitude"][0],
                "weapon": escort["weapon"][0],
                "trackcategory": escort["trackcategory"][0],
                "aircraft_type": escort["aircraft_type"][0],
                "distance_km": haversine(
                    escort["latitude"][0],
                    escort["longitude"][0],
                    float(target["Latitude"]),
                    float(target["Longitude"])
                ),
            }
        for escort in nearest_escort
        ]
    }
    # for targ in hostile[1]:
    #     for item in escort_report["escort"]:
    #         test_list = (json.loads(armament.check_armaments(item, targ)))
    #         if test_list["app_code"] > 2:
    #             final.append(test_list)
    #             print(final)
            

    
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
    sead_list = database.query_assets("weapon","ILIKE", "%AGM-88%", friendly["bc3_jtn"])
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
        escort_report = find_escort(friendly, hostiles, target_data)
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

# if __name__ == "__main__":
#     enemy = '44142, ( "CallSign": "LEADERSH", "Track Cat": "Land", "Track ID": "Hostile", "Aircraft Type": None, "Latitude": 25.798603224961937, "Longitude": -80.19250687633644 )'


#     hostile = (4, [43628, 'Hostile','Air', 'None', 'None']),(4, [43623, 'Hostile','Air', 'None', 'None']),(4, [43625, 'Hostile','Air', 'None', 'None'])

#     friendly_assets = [{
#         "callsign": "Lifeguard61",                 # rescue prefix OK
#         "weapon": "2XAIM-9, 4XAIM-120, 4XGBU-53 SD",
#         "aircraft_type": "F-A-22",
#         "trackcategory": "air",
#         "comm_deliverables": "VHF, UHF, Comm Sat",
#         "sensing_deliverables": "AMTI, IMINT 1, ELINT 1",
#         "ea_deliverables": "Responsive Noise, DRFM",
#         "bc3_jtn": "15486",                        # now carried through in all scenarios
#         "matched_actions": ["rescue"]              # try "degrade", "investigate", or "destroy"
#     }]

#     json_payload = gather_support(friendly_assets, enemy, hostile)
#     print(json_payload)