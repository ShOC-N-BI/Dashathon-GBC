"""time_to_target.py"""

# pylint: disable=W0613

import database
import pandas as pd

# groundspeed = meters / second


def compute_time(friendly, target):
    """calculate time to target"""

    distance = float(friendly["distance_km"]) * 1000  # convert to meters
    groundspeed_data = database.get_groundspeed(friendly["merged_tracknumber"])

    groundspeed = groundspeed_data["groundspeed"]
    if isinstance(groundspeed, pd.Series):
        groundspeed = groundspeed.iloc[0]

    time = (distance / groundspeed) / 60  # time in minutes
    time = round(time, 2)

    if time < 10:
        risk = 4
    elif 10 <= time < 20:
        risk = 3
    elif 20 <= time < 60:
        risk = 2
    else:
        risk = 1

    return risk, float(time)
