from datetime import datetime, timedelta

def make_timeline(friendly, results_hostiles, results_fuel, results_support, timestamp):
    # Parse timestamp into datetime
    dt = datetime.fromisoformat(str(timestamp))  
    time = dt.strftime("%H%M")
    support_time = (dt + timedelta(minutes=10)).strftime("%H%M")
    mission_time = (dt + timedelta(minutes=20)).strftime("%H%M")
    asset = friendly["callsign"]
    tanker = results_fuel["tanker_callsign"]
    if results_support and "escort" in results_support:
      support = results_support["escort"].get("callsign", "")

    else:
      support = ""


    fuel_score = results_fuel["score"]
    hostile_score = results_hostiles[0]

    if fuel_score == 4 and hostile_score == 4:
        return f"Push {asset} {time}"
    
    if fuel_score in (2, 3) and hostile_score == 4:
        return f"Push {tanker} {time}, Push {asset} {support_time}"
    
    if fuel_score == 4 and hostile_score < 4:
        return f"Push {support} {time}, Push {asset} {support_time}"
    
    if fuel_score in (2, 3) and hostile_score < 4:
        return f"Push {tanker} {time}, Push {support} {support_time}, Push {asset} {mission_time}"
    
    return "N/A"