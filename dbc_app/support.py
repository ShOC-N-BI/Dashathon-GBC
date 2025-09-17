import database
import math 
import re

# -----------------------------
# Utility Functions
# -----------------------------
def parse_track_info(track_string):
    """
    Regex to capture the main ID and all key-value pairs inside parentheses
    """
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

# Mission Pairing
#----------------
# SEAD 1-2x
# AWAC 1x
# Tanker 1x
# EW (Electronic Warfare) 1x
# Escort 1-2x
# 1-2x Same Effective Aircraft

# -----------------------------
# Main Code
# -----------------------------
def gather_support(friendly, target, tanker):
    asset = friendly["bc3_jtn"]
    target_data = parse_track_info(target)
    print (tanker)
    tanker_list = tanker
    return

gather_support()