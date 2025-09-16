import pandas as pd
import geopy.distance
import database     
import re
bc3_all = database.query_bc3_with_all_vw()
# print(bc3_all.head())

mef_data = database.query_mef_data_testing()
# print(mef_data["entity"])
# print(mef_data["actions"][0][0]["lon"])


# entity_data = expand_track_data(mef_data, "entity")

# print(entity_data["ID"])

# asset_coord = [52, 53]
# hostile_coord = [60, 57]

# hostiles = [
#     {"name": "zulu", "lat": 54, "lon": 56, "entity": "hostile"},
#     {"name": "hawk", "lat": 54, "lon": 56, "entity": "hostile"},
#     {"name": "mako", "lat": 56, "lon": 58, "entity": "hostile"}
# ]
def evaluate_threat(friendly, target):
    
    friendly = friendly
    target = target

    def parse_track_info(track_string):
        # Regex to capture the main ID and all key-value pairs inside parentheses
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
    
    hostile = parse_track_info(target)
    
    def compute_midPoint(friendly, target):  
        return [
            (friendly["lat"] + target["latitude"]) / 2,
            (friendly["lon"] + target["longitude"]) / 2
        ]
    
    midpoint = compute_midPoint(friendly,hostile)
    
    def determine_radius(friendly,target ):
        return geopy.distance.geodesic([friendly["lat"],friendly["lon"]], [target["latitude"], target["longitude"]]).km / 2
    
    radius = determine_radius(friendly,hostile)

    def locate_hostiles(midpoint_coords, coords, radius):
        detected = []
        for h in coords:
            distance = geopy.distance.geodesic(midpoint_coords, [h["latatitude"], h["longitude"]]).km
            if distance < radius and h["entitytrackid"] == "hostile":   
                detected.append(h)
        return detected
    
    detected_hotiles = locate_hostiles(midpoint, bc3_all, radius)

    def determine_score(list):
        if len(list) == 0:
            return 4 
        if len(list) == 1:
            return 3
        if len(list) == 2:
            return 2
        if len(list) == 3:
            return 1
    
    return determine_score(detected_hotiles)

