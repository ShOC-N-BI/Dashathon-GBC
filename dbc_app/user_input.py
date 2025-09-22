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
        a = math.sin(delta_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    # Pre-build lookup dictionaries for faster matching
    bc3_all_dict = {row.tracknumber: row for row in bc3_all.itertuples(index=False)}
    bc3_friends_dict = {row.merged_tracknumber: row for row in bc3_friends.itertuples(index=False)}

    # Track inserted asset-target pairs in this run to prevent batch duplicates
    inserted_pairs = set()

    for a in user_input.itertuples(index=False):
        pair_key = (a.asset_tn, a.target_tn)

        # Skip duplicates in current batch
        if pair_key in inserted_pairs:
            print(f"Skipping duplicate in current run: Asset {a.asset_tn}, Target {a.target_tn}")
            continue

        # Skip duplicates already in DB
        if database.record_exists(a.asset_tn, a.target_tn):
            print(f"Skipping duplicate in DB: Asset {a.asset_tn}, Target {a.target_tn}")
            continue

        # Get asset and entity
        asset = bc3_friends_dict.get(a.asset_tn)
        entity_row = bc3_all_dict.get(a.target_tn)

        if asset is None:
            print(f"Non-existent asset match: {a.asset_tn}")
            continue
        if entity_row is None:
            print(f"Non-existent entity match: {a.target_tn}")
            continue

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
            inserted_pairs.add(pair_key)
            print(f"Inserted: Asset {asset.merged_tracknumber}, Target {a.target_tn}")
        except Exception as e:
            print(f"Error inserting data for Asset {asset.merged_tracknumber}, Target {a.target_tn}: {e}")
