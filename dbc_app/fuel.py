import database
import pandas as pd
import numpy as np
import math
import re
import json

# Aircraft fuel data (lbs/hour consumption, max fuel capacity in lbs)
AIRCRAFT_FUEL_DATA = {
    "FA18F": {"consumption_rate": 6500, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "B-2 SPIR": {"consumption_rate": 11000, "max_fuel_capacity": 167000, "cruise_speed": 900},
    "RQ-4": {"consumption_rate": 1200, "max_fuel_capacity": 17500, "cruise_speed": 575},
    "F16J": {"consumption_rate": 5800, "max_fuel_capacity": 7000, "cruise_speed": 850},
    "F-35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700, "cruise_speed": 850},
    "RQ4": {"consumption_rate": 1200, "max_fuel_capacity": 17500, "cruise_speed": 575},
    "MQ9": {"consumption_rate": 400, "max_fuel_capacity": 9000, "cruise_speed": 310},
    "F35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500, "cruise_speed": 850},
    "F-15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000, "cruise_speed": 900},
    "KC46": {"consumption_rate": 12000, "max_fuel_capacity": 212000, "cruise_speed": 850},
    "B-52": {"consumption_rate": 12000, "max_fuel_capacity": 312000, "cruise_speed": 830},
    "EA18G": {"consumption_rate": 6700, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "E-3": {"consumption_rate": 10000, "max_fuel_capacity": 130000, "cruise_speed": 780},
    "RC135VW": {"consumption_rate": 11000, "max_fuel_capacity": 130000, "cruise_speed": 800},
    "F-16": {"consumption_rate": 5800, "max_fuel_capacity": 7000, "cruise_speed": 850},
    "E2D": {"consumption_rate": 5000, "max_fuel_capacity": 5800, "cruise_speed": 600},
    "MQ-9": {"consumption_rate": 400, "max_fuel_capacity": 9000, "cruise_speed": 310},
    "FA18E": {"consumption_rate": 6500, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "MH60S": {"consumption_rate": 1200, "max_fuel_capacity": 6000, "cruise_speed": 270},
    "EC-130": {"consumption_rate": 5200, "max_fuel_capacity": 60000, "cruise_speed": 540},
    "U2": {"consumption_rate": 2500, "max_fuel_capacity": 28000, "cruise_speed": 760},
    "F-35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750, "cruise_speed": 850},
    "C17": {"consumption_rate": 20000, "max_fuel_capacity": 240000, "cruise_speed": 830},
    "MC130": {"consumption_rate": 5200, "max_fuel_capacity": 60000, "cruise_speed": 540},
    "F35C": {"consumption_rate": 7200, "max_fuel_capacity": 19700, "cruise_speed": 850},
    "C-130": {"consumption_rate": 5000, "max_fuel_capacity": 45000, "cruise_speed": 540},
    "B2": {"consumption_rate": 11000, "max_fuel_capacity": 167000, "cruise_speed": 900},
    "F35A": {"consumption_rate": 7000, "max_fuel_capacity": 18750, "cruise_speed": 850},
    "RC-135": {"consumption_rate": 11000, "max_fuel_capacity": 130000, "cruise_speed": 800},
    "F-35B": {"consumption_rate": 7300, "max_fuel_capacity": 13500, "cruise_speed": 850},
    "F-A-22": {"consumption_rate": 8000, "max_fuel_capacity": 18000, "cruise_speed": 850},
    "DIS(265)": {"consumption_rate": 0, "max_fuel_capacity": 0, "cruise_speed": 0},
    "F-A-18E-F": {"consumption_rate": 6500, "max_fuel_capacity": 14500, "cruise_speed": 850},
    "F16C": {"consumption_rate": 5800, "max_fuel_capacity": 7000, "cruise_speed": 850},
    "MH60R": {"consumption_rate": 1200, "max_fuel_capacity": 6000, "cruise_speed": 270},
    "P8": {"consumption_rate": 8000, "max_fuel_capacity": 100000, "cruise_speed": 820},
    "F22": {"consumption_rate": 8000, "max_fuel_capacity": 18000, "cruise_speed": 850},
    "KC135": {"consumption_rate": 11000, "max_fuel_capacity": 200000, "cruise_speed": 850},
    "C130": {"consumption_rate": 5000, "max_fuel_capacity": 45000, "cruise_speed": 540},
    "B-1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000, "cruise_speed": 900},
    "F15E": {"consumption_rate": 7500, "max_fuel_capacity": 27000, "cruise_speed": 900},
    "P-3": {"consumption_rate": 4200, "max_fuel_capacity": 64000, "cruise_speed": 650},
    "E-2C": {"consumption_rate": 5000, "max_fuel_capacity": 5800, "cruise_speed": 600},
    "E7": {"consumption_rate": 9500, "max_fuel_capacity": 130000, "cruise_speed": 800},
    "B1B": {"consumption_rate": 13500, "max_fuel_capacity": 184000, "cruise_speed": 900},
    "KC-135": {"consumption_rate": 11000, "max_fuel_capacity": 200000, "cruise_speed": 850},
    "P3": {"consumption_rate": 4200, "max_fuel_capacity": 64000, "cruise_speed": 650},
    "B52": {"consumption_rate": 12000, "max_fuel_capacity": 312000, "cruise_speed": 830},
    "EA37B": {"consumption_rate": 4500, "max_fuel_capacity": 41300, "cruise_speed": 740},
    "E3": {"consumption_rate": 10000, "max_fuel_capacity": 130000, "cruise_speed": 780}
}


REFUEL_SPEEDS = {
    "KC-135": 1000,
    "KC-10": 1200,
    "KC-46": 1100,
}

def midpoint_to_tanker(tank_lat, tank_long, asset_lat, asset_long):
    # print(f"Tanker Lat/Long {tank_lat},{tank_long}")
    # print(f"Asset Lat/Long {asset_lat},{asset_long}")
    midpoint_lat = (tank_lat + asset_lat) / 2
    midpoint_long = (tank_long + asset_long) / 2
    # print(f"Midpoint Lat/Long: {midpoint_lat}, {midpoint_long}")
    return midpoint_lat, midpoint_long

def midpoint_for_target(calc, target, asset_lat, asset_long):
    entity_lat = float(target["Lattitude"])
    entity_long = float(target["Longitude"])
    distance_to_target = haversine(entity_lat, entity_long, calc[0],calc[1])
    distance_to_origin = haversine(asset_lat, asset_long, calc[0],calc[1])
    
    # print(f"Target: {entity_lat},{entity_long}")
    # print(f"Midpoint: {calc[0]}, {calc[1]}")
    return distance_to_target, distance_to_origin
    

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



def find_nearest_tanker(friendly_lat, friendly_lon, tankers, target):
    """
    Returns the closest tanker and distance to it.
    tankers : list of dicts with keys "latitude" and "longitude"
    """
    nearest = None
    min_distance = float("inf")
    entity_lat = float(target["Lattitude"])
    entity_long = float(target["Longitude"])

    for tanker in tankers:
        tanker_lat = float(tanker["latitude"])
        tanker_lon = float(tanker["longitude"])
        distance = haversine(friendly_lat, friendly_lon, tanker_lat, tanker_lon)
        target_distance = haversine(entity_lat, entity_long, tanker_lat, tanker_lon)
        if distance < min_distance:
            min_distance = distance
            nearest = tanker
            tanker_to_target = target_distance

    return nearest, min_distance,tanker_to_target

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


def analyze_fuel(friendly, target):
    """
    Determines if an asset can reach its target and return.
    Returns:
        3: Can make round trip with current fuel
        2: Cannot with current fuel, but max fuel allows it
        1: Cannot make round trip even at max fuel
    """

    track_id = friendly["bc3_jtn"]
    distance = float(friendly["distance_km"])
    asset_lat = float(friendly["lat"])
    asset_long = float(friendly["lon"])
    all_view = database.query_friendly_asset(track_id)
    tankers = database.query_tankers()
    
    if all_view.empty or "fuel" not in all_view.columns or "groundspeed" not in all_view.columns:
        return 1, "Unknown Fuel or Speed"  # Cannot determine, assume worst case
    if all_view.loc[0, "fuel"] is None:
        return 1, "Unknown Fuel" # Cannot determine fuel status, assume worst case
    
    current_fuel = float(all_view.loc[0, "fuel"])
    aircraft_type = friendly["aircraft_type"].upper()
    cruisespeed = AIRCRAFT_FUEL_DATA[aircraft_type]["cruise_speed"]

    if aircraft_type not in AIRCRAFT_FUEL_DATA:
        return 1, "Unknown Aircraft Type"  # Unknown aircraft, assume worst case

    aircraft_rate = AIRCRAFT_FUEL_DATA[aircraft_type]["consumption_rate"]
    aircraft_max = AIRCRAFT_FUEL_DATA[aircraft_type]["max_fuel_capacity"]

    def can_make_round_trip(F, R, D, V):
        time_required = (2 * D) / V
        fuel_needed = R * time_required
        return F >= fuel_needed
    def can_make_tanker_trip(F, R, D, V):
        time_required = D / V
        fuel_needed = R * time_required
        return F >= fuel_needed

    # print(target)
    # print("targets")
    nearest_tanker, distance_to_tanker, target_distance = find_nearest_tanker(asset_lat, asset_long, tankers, parse_track_info(target))
    midpoint_calc = midpoint_to_tanker(nearest_tanker["latitude"], nearest_tanker["longitude"], asset_lat, asset_long)
    distance_to_target, distance_to_origin = midpoint_for_target(midpoint_calc, parse_track_info(target), asset_lat, asset_long)
    # print(f"Asset: {asset_lat}, {asset_long}")
    # print(f"Distance from Tanker to Target: {distance_to_target}")
    # print(f"Distance to Tanker Midpoint: {distance_to_origin}")
    # print(f"Tankers: {nearest_tanker["latitude"]}, {nearest_tanker["longitude"]}")

    # print(target_distance)
    # print(midpoint_distance)
    # print(current_fuel, aircraft_max, aircraft_rate, distance, distance_to_tanker, cruisespeed, nearest_tanker["bc3_vcs"], (distance + distance_to_tanker))

    if can_make_round_trip(100, aircraft_rate, distance, cruisespeed):
        return 3  # Can make it with current fuel
    elif can_make_round_trip(current_fuel, aircraft_rate, distance_to_tanker, cruisespeed):
        # Can make it to the tanker
        if can_make_tanker_trip(aircraft_max, aircraft_rate, (distance_to_target + distance + distance_to_origin), cruisespeed):
            # print(distance, target_distance, distance_to_tanker, (distance_to_target + distance + distance_to_origin))
            return 2, nearest_tanker["bc3_vcs"], nearest_tanker["bc3_jtn"]  # Needs refuel, but max fuel allows it, considers new target distance
    else:
        return 1, "Cannot make round trip"  # Cannot make round trip even at max fuel