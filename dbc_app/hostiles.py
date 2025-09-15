import pandas as pd
import geopy.distance
import database     


# asset_coord = [52,53]
# hostile_coord = [52,53]

# def compute_midPoint(asset_coord,hostile_coord):  
#     return ([(asset_coord[0] + hostile_coord[0])/2, (asset_coord[1] + hostile_coord[1])/2])

# compute_midPoint(asset_coord,hostile_coord)

# def determine_distance(asset,hostile):
#     return print(geopy.distance.geodesic(asset, hostile).mi)

# determine_distance([52, 53],[52.406374, 16.9251681])

print(database.df_bc3_with_all_vw.head())