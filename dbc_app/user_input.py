import database
import math
import json
import re

def insert_input():
    # Load data
    user_input = database.query_user_input()
    bc3_all = database.query_bc3_with_all_vw()
    bc3_friends = database.query_bc3_friends_vw()
    mef= database.query_mef()
    print(user_input)

    def haversine(lat1, lon1, lat2, lon2):
        """Calculate the great-circle distance between two points on Earth in km."""
        R = 6371
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    # Pre-build lookup dictionaries for faster matching
    bc3_all_dict = {row.tracknumber: row for row in bc3_all.itertuples(index=False)}
    bc3_friends_dict = {row.merged_tracknumber: row for row in bc3_friends.itertuples(index=False)}

    existing_pairs = set()
    for row in mef.itertuples(index=False):
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
        
        target_mef = parse_track_info(row.entity)

        asset_mef = row.actions


        # row.asset_tn and row.target_tn are the columns in the other table
        existing_pairs.add((asset_mef[0]["merged_tracknumber"], target_mef["ID"]))

    # Now process user_input
    for a in user_input.itertuples(index=False):
        pair_key = (a.asset_tn, a.target_tn)

        # Skip if this pair exists in the other table
        if pair_key in existing_pairs:
            print(f"Skipping duplicate found in other table: Asset {a.asset_tn}, Target {a.target_tn}")
            continue

        # Proceed with insertion
        asset = bc3_friends_dict.get(a.asset_tn)
        entity_row = bc3_all_dict.get(a.target_tn)
        if asset == None:
            print("ASSET NONE")
            print(type(asset))
            continue
        if asset == None:
            print("TARGET NONE")
            print(type(entity_row))
            continue
        print(f" Asset {a.asset_tn}, Target {a.target_tn}")

        #if asset is None:
        #    print(f"Non-existent match for Asset {a.asset_tn}")
        #    continue
        #if entity_row is None:
        #    print(f"Non-existent match for Target {a.target_tn}")
        #    continue



        # Compute distance
        distance = haversine(asset.latitude, asset.longitude, entity_row.latitude, entity_row.longitude)

        # Build action dictionary
        action = [{
            "lat": asset.latitude,
            "lon": asset.longitude,
            "weapon": asset.munition_deliverables,
            "bc3_jtn": asset.bc3_jtn,
            "callsign": asset.callsign,
            "distance_km": distance,
            "aircraft_type": asset.aircraft_type,
            "trackcategory": asset.trackcategory.lower(),
            "ea_deliverables": asset.ea_deliverables,
            "matched_actions": [a.battle_effect.lower()],
            "comm_deliverables": asset.comm_deliverables,
            "merged_tracknumber": asset.merged_tracknumber,
            "sensing_deliverables": asset.sensing_deliverables,
        }]

        # Build entity description
        entity = (
            f"{a.target_tn} (CallSign: {entity_row.callsign}, Track Cat: {entity_row.trackcategory}, "
            f"Track ID: {entity_row.trackid}, Aircraft Type: {entity_row.aircraft_type}, "
            f"Latitude: {entity_row.latitude}, Longitude: {entity_row.longitude})"
        )

        timestamp = a.timestamp

        # Insert into database
        try:
            database.insert_data(entity, json.dumps(action), "text", timestamp)
            existing_pairs.add(pair_key)
            print(f"Inserted: Asset {asset.merged_tracknumber}, Target {a.target_tn}")
        except Exception as e:
            print(f"Error inserting data for Asset {asset.merged_tracknumber}, Target {a.target_tn}: {e}")
