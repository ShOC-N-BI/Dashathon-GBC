from geopy.distance import geodesic
import database     
import re



#target = "44875 (CallSign: None, Track Cat: Air, Track ID: Hostile, Aircraft Type: None, Lattitude: 23.940473666159686, Longitude: -78.38917303598667)"
#friendly = {'lat': 24.24638132680745, 'lon': -77.9662517984651, 'weapon': '8XMK-84, 8XJDAM-BLU-, 8XAGM-158', 'bc3_jtn': '12257', 'callsign': 'HELENA 2', 'distance_km': 54.84259894385782, 'aircraft_type': 'B-2 SPIR', 'trackcategory': 'air', 'ea_deliverables': '-', 'matched_actions': ['attack'], 'comm_deliverables': 'UHF, Airborne Datalink, LPI Datalink, GEO MilSat, VLF', 'merged_tracknumber': 255, 'sensing_deliverables': '-'}

# for row in bc3_all.itertuples(index=False):
#     distance = geodesic([50, 55], [float(row.latitude), float(row.longitude)]).km
#     if distance < 6000 and row.entitytrackid == "hostile":
#         detected.append(row)  # append the whole row

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
            (float(friendly["lat"]) + float(target["Lattitude"])) / 2,
            (float(friendly["lon"]) + float(target["Longitude"])) / 2
        ]
    
    midpoint = compute_midPoint(friendly,hostile)
    
    def determine_radius(friendly,target ):
        return geodesic([friendly["lat"],friendly["lon"]], [target["Lattitude"], target["Longitude"]]).km / 2
    
    radius = determine_radius(friendly,hostile)

    def locate_hostiles(midpoint, radius):
        bc3_all = database.query_bc3_with_all_vw()
        detected = []
        for row in bc3_all.itertuples(index=False):
            distance = geodesic(midpoint, [float(row.latitude), float(row.longitude)]).km
            if distance < radius and row.trackid in ("Hostile"):
                detected.append((row.tracknumber, row.trackid, row.trackcategory ))  # append the whole row
        return detected
    
    detected_hotiles = locate_hostiles(midpoint, radius)

    def determine_score(list):
        if len(list) == 0:
            return 4 
        if len(list) == 1:
            return 3
        if len(list) == 2:
            return 2
        if len(list) == 3:
            return 1
        if len(list) > 3:
            return 0
    return determine_score(detected_hotiles), detected_hotiles
    
# score = evaluate_threat(friendly, target)
# print(score)