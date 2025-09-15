import pandas as pd
import geopy.distance
import database     

# bc3_all = database.query_bc3_with_all_vw()
# print(bc3_all.head())

mef_data = database.query_mef_data_testing()
# print(mef_data["actions"][0][0]["lat"])
# print(mef_data["actions"][0][0]["lon"])

def expand_track_data(df, column_name):
    
    # Copy to avoid modifying original
    df = df.copy()

    # Extract ID (number before parentheses)
    df["ID"] = df[column_name].str.extract(r"^(\d+)").astype("Int64")

    # Extract inside parentheses
    df["inside"] = df[column_name].str.extract(r"\((.*?)\)")

    # Convert to dicts
    df_dicts = df["inside"].apply(
        lambda x: dict(item.strip().split(": ", 1) for item in x.split(", "))
    )

    # Expand dicts into columns
    expanded = df_dicts.apply(pd.Series)

    # Merge back
    df = pd.concat([df.drop(columns=["inside"]), expanded], axis=1)

    return df


print(mef_data)

# asset_coord = [52, 53]
# hostile_coord = [60, 57]

# hostiles = [
#     {"name": "zulu", "lat": 54, "lon": 56, "entity": "hostile"},
#     {"name": "hawk", "lat": 54, "lon": 56, "entity": "hostile"},
#     {"name": "mako", "lat": 56, "lon": 58, "entity": "hostile"}
# ]

# def compute_midPoint(asset_coord, hostile_coord):  
#     return [
#         (asset_coord[0] + hostile_coord[0]) / 2,
#         (asset_coord[1] + hostile_coord[1]) / 2
#     ]

# def determine_radius(asset, hostile):
#     return geopy.distance.geodesic(asset, hostile).km / 2

# def locate_hostiles(mid_point_coords, coords, radius):
#     detected = []
#     for h in coords:
#         distance = geopy.distance.geodesic(mid_point_coords, [h["lat"], h["lon"]]).km
#         if distance < radius and h["entity"] == "hostile":   
#             detected.append(h)
#     return detected


# midpoint = compute_midPoint(asset_coord, hostile_coord)
# radius = determine_radius(asset_coord, hostile_coord)
# detected_hostiles = locate_hostiles(midpoint, hostiles, radius)
# number_of_hostiles = len(detected_hostiles)

# print("Midpoint:", midpoint)
# print("Radius (km):", radius)
# print("Detected hostiles inside radius:", detected_hostiles)
# print(number_of_hostiles)