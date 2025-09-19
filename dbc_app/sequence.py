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
    tanker = results_fuel.get("tanker_callsign", "") if isinstance(results_fuel, dict) else ""

    if isinstance(results_support, dict):
        support_list = results_support.get("escort") or []
    else:
        support_list = []

    support = " ".join(
        d.get("callsign") or d.get("bc3_jtn", "")
        for d in support_list
        if isinstance(d, dict)
    )
    print(support)
    print("*************")
    
    fuel_score = results_fuel if isinstance(results_fuel, int) else (results_fuel[0] if isinstance(results_fuel, list) and results_fuel else 0)

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
    
