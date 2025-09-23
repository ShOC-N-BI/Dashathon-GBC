from datetime import datetime, timedelta
import database 

import armament

def make_timeline(friendly, results_hostiles, results_fuel, results_support, timestamp):
    # Parse timestamp into datetime
    dt = datetime.fromisoformat(str(timestamp))  
    time = dt.strftime("%H%M")
    support_time = (dt + timedelta(minutes=10)).strftime("%H%M")
    mission_time = (dt + timedelta(minutes=20)).strftime("%H%M")
    asset = friendly["callsign"] or friendly["merged_tracknumber"]
    tanker = results_fuel.get("tanker_callsign", "") if isinstance(results_fuel, dict) else ""

    def extract_single(results_support: dict, key: str) -> str:
    
        if not isinstance(results_support, dict):
            return ""
        items = results_support.get(key) or []
        if items and isinstance(items, dict):
            return items.get("callsign") or items.get("merged_tracknumber", "")
        return ""

    def extract_multiple(results_support: dict, key: str) -> str:
        """Extract multiple callsigns/bc3_jtn from list"""
        if not isinstance(results_support, dict):
            return ""
        items = results_support.get(key) or []
        return " ".join(
            d.get("callsign") or d.get("merged_tracknumber", "")
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
    print(f"Hostile score: {hostile_score}")
    print(f"Fuel Score: {fuel_score}")
    if fuel_score == 4 and hostile_score == 4:
        return f"Push {awacs} {ew} {sead} T+0Z, Push {asset} T+10Z"
    
    if fuel_score in (2, 3) and hostile_score == 4:
        return f"Push {tanker} T+0Z, Push {awacs} {ew} {sead} T+10Z, Push {asset} T+20Z"
    
    if fuel_score == 4 and hostile_score < 4:
        return f"Push {support} {awacs} {ew} {sead} T+0Z, Push {asset} T+10Z"
    
    if fuel_score in (2, 3) and hostile_score < 4:
        return f"Push {tanker} T+0Z, Push {support} {awacs} {ew} {sead} T+10Z, Push {asset} T+20Z"
    
    else:
       return "N/A"
    
