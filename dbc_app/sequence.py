from datetime import datetime, timedelta

def make_timeline(friendly, results_hostiles, results_fuel, results_support, timestamp):
    # Parse timestamp into datetime
    dt = datetime.fromisoformat(str(timestamp))  
    time = dt.strftime("%H%M")
    support_time = (dt + timedelta(minutes=10)).strftime("%H%M")
    mission_time = (dt + timedelta(minutes=20)).strftime("%H%M")

    tanker = results_fuel["tanker_callsign"]
    support = results_support
    fuel_score = results_fuel["score"]
    hostile_score = results_hostiles[0]

    if fuel_score == 4 and hostile_score == 4:
        return f"Push {friendly} {time}"
    
    if fuel_score in (2, 3) and hostile_score == 4:
        return f"Push {tanker} {time}, Push {friendly} {support_time}"
    
    if fuel_score == 4 and hostile_score < 4:
        return f"Push {support} {time}, Push {friendly} {support_time}"
    
    if fuel_score in (2, 3) and hostile_score < 4:
        return f"Push {tanker} {time}, Push {support} {support_time}, Push {friendly} {mission_time}"
    
    return None
