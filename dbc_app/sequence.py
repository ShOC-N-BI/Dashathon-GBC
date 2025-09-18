from datetime import datetime, timedelta
import database 
import fiveline
import armament

def make_timeline(friendly, results_hostiles, results_fuel, results_support, timestamp):
    # Parse timestamp into datetime
    dt = datetime.fromisoformat(str(timestamp))  
    time = dt.strftime("%H%M")
    support_time = (dt + timedelta(minutes=10)).strftime("%H%M")
    mission_time = (dt + timedelta(minutes=20)).strftime("%H%M")
    asset = friendly["callsign"]
    tanker = results_fuel["tanker_callsign"]
    support_list = (results_support or {}).get("escort") or []
    support = " ".join(
    d.get("callsign") or d.get("bc3_jtn", "")
    for d in support_list if d is not None)



    fuel_score = results_fuel["score"]
    hostile_score = results_hostiles[0]
    
    if fuel_score == 4 and hostile_score == 4:
        return f"Push {asset} {time}Z"
    
    if fuel_score in (2, 3) and hostile_score == 4:
        return f"Push {tanker} {time}Z, Push {asset} {support_time}Z"
    
    if fuel_score == 4 and hostile_score < 4:
        return f"Push {support} {time}Z, Push {asset} {support_time}Z"
    
    if fuel_score in (2, 3) and hostile_score < 4:
        return f"Push {tanker} {time}Z, Push {support} {support_time}Z, Push {asset} {mission_time}Z"
    
    else:
       return "N/A"
    
