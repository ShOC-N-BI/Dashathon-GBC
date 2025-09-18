import database 
import math

user_input = database.query_df_user_input()
bc3_all = database.query_bc3_with_all_vw()


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

def create_action():
    for a in user_input.itertuples(index=False):
        for row in bc3_all.itertuples(index=False):  # iterate as namedtuples
            if a.asset_tn == row.tracknumber:
                action = {
                    "lat": row.latitude,
                    "lon": row.longitude,
                    "weapon": row.weapon,
                    "bc3_jtn": row.bc3_jtn,
                    "callsign": row.callsign,
                    "distance_km": "haversine()",
                    "aircraft_type": row.aircraft_type,
                    "trackcategory": row.trackcategory,
                    "ea_deliverables": "-",
                    "matched_actions": [a.battle_effect],
                    "comm_deliverables": "HF, VHF, UHF, Comm Sat",
                    "merged_tracknumber": row.tracknumber,
                    "sensing_deliverables": "-"
                }
                return action  # immediately return the first match
    return {}  # return empty dict if no match found



def create_entity():
     for a in user_input.itertuples(index=False):
        for row in bc3_all.itertuples(index=False):  # iterate as namedtuples
            if a.target_tn == row.tracknumber:
                entity = f"44073 (CallSign: {row.callsign} , Track Cat: {row.trackcategory}, Track ID: {row.trackid}, Aircraft Type: {row.aircraft_type}, Lattitude: {row.latitude}, Longitude: {row.longitude})"

                
     return entity

actions =create_action()
entity = create_entity()

print(actions)
print(entity)

