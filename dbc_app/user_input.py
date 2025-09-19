import database
import math
import json

# Load data
user_input = database.query_user_input()
bc3_all = database.query_bc3_with_all_vw()
bc3_friends = database.query_bc3_friends_vw()


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(delta_lambda/2)**2
    c = 2*math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c


# Process each user_input row individually
for a in user_input.itertuples(index=False):
    # Find asset and entity
    asset = next((row for row in bc3_friends.itertuples(index=False)
                  if a.asset_tn == row.merged_tracknumber), None)
    entity_row = next((row for row in bc3_all.itertuples(index=False)
                       if a.target_tn == row.tracknumber), None)

    if asset and entity_row:
        # Compute distance
        distance = haversine(asset.latitude, asset.longitude, entity_row.latitude, entity_row.longitude)

        # Define separate variables for this row
        action = {
            "lat": asset.latitude,
            "lon": asset.longitude,
            "weapon": asset.munition_deliverables,
            "bc3_jtn": asset.bc3_jtn,
            "callsign": asset.callsign,
            "distance_km": distance,
            "aircraft_type": asset.aircraft_type,
            "trackcategory": asset.trackcategory,
            "ea_deliverables": asset.ea_deliverables,
            "matched_actions": [a.battle_effect],
            "comm_deliverables": asset.comm_deliverables,
            "merged_tracknumber": asset.merged_tracknumber,
            "sensing_deliverables": asset.sensing_deliverables,
        }

        entity = (
            f"{a.target_tn} (CallSign: {entity_row.callsign}, Track Cat: {entity_row.trackcategory}, "
            f"Track ID: {entity_row.trackid}, Aircraft Type: {entity_row.aircraft_type}, "
            f"Latitude: {entity_row.latitude}, Longitude: {entity_row.longitude})"
        )

        timestamp = a.timestamp

        # Check duplicates in DB
        if not database.record_exists(asset.merged_tracknumber, a.target_tn):
            database.insert_data(entity, json.dumps(action), "text", timestamp)
        else:
            print(f"Skipping duplicate: Asset {asset.merged_tracknumber}, Target {a.target_tn}")

        # You can now use these variables individually in your code
        # For example:
        # print(entity)
        # print(action)
        # print(timestamp)
