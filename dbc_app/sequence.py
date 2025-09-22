from datetime import datetime, timedelta
import database 

import armament

def make_timeline(friendly, results_hostiles, results_fuel, results_support, timestamp):
    # Parse timestamp into datetime
    dt = datetime.fromisoformat(str(timestamp))  
    time = dt.strftime("%H%M")
    support_time = (dt + timedelta(minutes=10)).strftime("%H%M")
    mission_time = (dt + timedelta(minutes=20)).strftime("%H%M")
    asset = friendly["callsign"]
    tanker = results_fuel.get("tanker_callsign", "") if isinstance(results_fuel, dict) else ""

    def extract_single(results_support: dict, key: str) -> str:
    
        if not isinstance(results_support, dict):
            return ""
        items = results_support.get(key) or []
        if items and isinstance(items, dict):
            return items.get("callsign") or items.get("bc3_jtn", "")
        return ""

    def extract_multiple(results_support: dict, key: str) -> str:
        """Extract multiple callsigns/bc3_jtn from list"""
        if not isinstance(results_support, dict):
            return ""
        items = results_support.get(key) or []
        return " ".join(
            d.get("callsign") or d.get("bc3_jtn", "")
            for d in items
            if isinstance(d, dict)
        )

    # usage
    support = extract_multiple(results_support, "escort")  # may have multiple
    awacs   = extract_single(results_support, "awacs")     # only one
    ew      = extract_single(results_support, "ew")        # only one
    sead    = extract_single(results_support, "sead")      # only one

    #print(support, awacs, ew, sead)
   
    fuel_score = results_fuel if isinstance(results_fuel, int) else (results_fuel[0] if isinstance(results_fuel, list) and results_fuel else 0)
    hostile_score = results_hostiles[0]
    
    if fuel_score == 4 and hostile_score == 4:
        return f"Push {awacs} {ew} {sead} {time}Z, Push {asset} {support_time}Z"
    
    if fuel_score in (2, 3) and hostile_score == 4:
        return f"Push {tanker} {time}Z, Push {awacs} {ew} {sead} {support_time}Z,Push {asset} {mission_time}Z"
    
    if fuel_score == 4 and hostile_score < 4:
        return f"Push {support} {awacs} {ew} {sead} {time}Z, Push {asset} {support_time}Z"
    
    if fuel_score in (2, 3) and hostile_score < 4:
        return f"Push {tanker} {time}Z, Push {support} {awacs} {ew} {sead} {support_time}Z, Push {asset} {mission_time}Z"
    
    else:
       return "N/A"
    
