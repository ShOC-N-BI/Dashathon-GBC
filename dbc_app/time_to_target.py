# time_to_target.py
import database
import json

# groundspeed = meters / second


def compute_time(friendly, target):
    distance = float(friendly["distance_km"]) * 1000
    groundspeed = database.get_groundspeed(friendly["bc3_jtn"])
    time = (distance / groundspeed["groundspeed"]) / 60
    time = round(time, 2)
    return time
