import database
import math
import json

def insert_input():
    # Load data
    user_input = database.query_user_input()
    bc3_all = database.query_bc3_with_all_vw()
    bc3_friends = database.query_bc3_friends_vw()
    print(user_input)
    def haversine(lat1, lon1, lat2, lon2):
        """Calculate the great-circle distance between two points on Earth in km."""
        R = 6371
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        a = math.sin(delta_phi / 2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(delta_lambda/2)**2
        c = 2*math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c


    # Keep track of inserted asset-target pairs to avoid duplicates
    inserted_pairs = set()

    # Process each user_input row
    for a in user_input.itertuples(index=False):
        # Find asset and entity
        print("***")
        asset = next((row for row in bc3_friends.itertuples(index=False)
                    if a.asset_tn == row.merged_tracknumber), None)
                    
        entity_row = next((row for row in bc3_all.itertuples(index=False)
                        if a.target_tn == row.tracknumber), None)
       
        if entity_row == None:
            print("Non existent entity match" )
            continue
        if asset == None:
            print("Non existent asset match" )
            continue
        if asset and entity_row:
            # Compute distance
            distance = haversine(asset.latitude, asset.longitude, entity_row.latitude, entity_row.longitude)

            # Define variables
            action = [{
                "lat": asset.latitude,
                "lon": asset.longitude,
                "weapon": asset.munition_deliverables,
                "bc3_jtn":asset.bc3_jtn,
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

            entity = (
                f"{a.target_tn} (CallSign: {entity_row.callsign}, Track Cat: {entity_row.trackcategory}, "
                f"Track ID: {entity_row.trackid}, Aircraft Type: {entity_row.aircraft_type}, "
                f"Latitude: {entity_row.latitude}, Longitude: {entity_row.longitude})"
            )

            timestamp = a.timestamp
            
            # Skip duplicates
            pair_key = (asset.merged_tracknumber, a.target_tn)

            print(pair_key)
            print(inserted_pairs)

            if pair_key not in inserted_pairs:
                #if not database.record_exists(asset.merged_tracknumber, a.target_tn):
                database.insert_data(entity, json.dumps(action), "text", timestamp)
                inserted_pairs.add(pair_key)
                print(f"Inserted: Asset {asset.merged_tracknumber}, Target {a.target_tn}")
                #else:
                #    print(f"Skipping duplicate in DB: Asset {asset.merged_tracknumber}, Target {a.target_tn}")
            else:
                print(f"Skipping duplicate in current run: Asset {asset.merged_tracknumber}, Target {a.target_tn}")

            # Variables are available for further use if needed
            # print(entity)
            # print(action)
            # print(timestamp)
