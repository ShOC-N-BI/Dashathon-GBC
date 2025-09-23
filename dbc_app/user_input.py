import database
import math
import json
import re

def insert_input():
    # Load data
    user_input = database.query_user_input()
    bc3_all = database.query_bc3_with_all_vw()
    bc3_friends = database.query_bc3_friends_vw()
    mef = database.query_all_mef()
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

    def parse_track_info(track_string):
        """Parse MEF entity string into a dictionary with ID."""
        match = re.match(r"(\d+)\s*\((.*)\)", track_string)
        if not match:
            return None
        track_id = match.group(1)
        key_values = match.group(2)
        info_dict = {}
        for kv in key_values.split(","):
            key, value = kv.split(":", 1)
            info_dict[key.strip()] = value.strip()
        info_dict["ID"] = track_id
        return info_dict

    # Build existing pairs from MEF table (normalize to strings)
    existing_pairs = set()
    for row in mef.itertuples(index=False):
        target_mef = parse_track_info(row.entity)
        if target_mef is None or "ID" not in target_mef:
            # print(f"Skipping invalid MEF row: {row.entity}")
            continue
        asset_mef = row.actions
        if not asset_mef or "merged_tracknumber" not in asset_mef[0]:
            continue
        existing_pairs.add((
            str(asset_mef[0]["merged_tracknumber"]).strip(),
            str(target_mef["ID"]).strip()
        ))

    # Track duplicates in the current batch
    
    # print(existing_pairs)
    # Process user_input
    for a in user_input.itertuples(index=False):
        pair_key = (str(a.asset_tn).strip(), str(a.target_tn).strip())
        # Find asset and entity
        print("***")
        asset = next((row for row in bc3_friends.itertuples(index=False)
                    if a.asset_tn == row.merged_tracknumber), None)
                    
        entity_row = next((row for row in bc3_all.itertuples(index=False)
                        if a.target_tn == row.tracknumber), None)
       
        if entity_row == None:
            print("Non existent entity match" )
            continue

      

        # Proceed with insertion
        asset = next((row for row in bc3_friends.itertuples(index=False) if a.asset_tn == row.merged_tracknumber), None)
        entity_row = next((row for row in bc3_all.itertuples(index=False) if a.target_tn == row.tracknumber), None)
        flag = False

        if asset is None:
            asset = next((row for row in bc3_all.itertuples(index=False) if a.asset_tn == row.tracknumber), None)
            if asset is None:
                print(f"Non-existent match for Asset {a.asset_tn}")
                continue
            else:
                print("FOUND in all instead")
                flag = True
            
        if entity_row is None:
            print(f"Non-existent match for Target {a.target_tn}")
            continue
        print(pair_key)

        # Compute distance
        distance = haversine(asset.latitude, asset.longitude, entity_row.latitude, entity_row.longitude)

        # Build action dictionary
        if flag:
            action = [{
                "lat": asset.latitude,
                "lon": asset.longitude,
                "weapon": asset.weapon,
                "bc3_jtn": asset.bc3_jtn,
                "callsign": asset.callsign,
                "distance_km": distance,
                "aircraft_type": asset.aircraft_type,
                "trackcategory": asset.trackcategory.lower(),
                "ea_deliverables": None,
                "matched_actions": [a.battle_effect.lower()],
                "comm_deliverables": None,
                "merged_tracknumber": asset.tracknumber,
                "sensing_deliverables": None,
            }]
        else:
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
            if flag:
                print(f"Inserted: Asset {asset.tracknumber}, Target {a.target_tn}")
            else:
                print(f"Inserted: Asset {asset.merged_tracknumber}, Target {a.target_tn}")
        except Exception as e:
            if flag:
                print(f"Error inserting data for Asset {asset.tracknumber}, Target {a.target_tn}: {e}")
            else:
                print(f"Error inserting data for Asset {asset.merged_tracknumber}, Target {a.target_tn}: {e}")
