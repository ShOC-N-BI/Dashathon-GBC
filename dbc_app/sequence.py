from datetime import datetime, timedelta
import database

def make_timeline(friendly,results_hostiles, results_fuel, results_support, timestamp):
    # get hour/minute from timestamp i.e. 2315
    dt = datetime.fromisoformat(str(timestamp))  # parse the string into datetime
    time= dt.strftime("%H%M")
    new_dt = dt + timedelta(minutes=15)
    
    if results_fuel["score"] == 3 and results_hostiles[0] == 4:
       return f"Push {friendly} {time} "
    if results_fuel["score"] == 3 and results_hostiles[0] < 4:
       dt = datetime.fromisoformat(str(timestamp))  # parse the string into datetime
       new_dt = dt + timedelta(minutes=10)
       mission_time = new_dt.strftime("%H%M")
       return f"push {results_support} {time}, Push {friendly} {mission_time} "
    if results_fuel["score"] == 2 and results_hostiles[0] < 4:
       dt = datetime.fromisoformat(str(timestamp))  
       new_dt = dt + timedelta(minutes=10)
       support_time = new_dt.strftime("%H%M")
        
       new2_dt = dt + timedelta(minutes=20)
       mission_time = new2_dt.strftime("%H%M")
       return f"push {results_fuel["tanker_callsign"]} {time}, push {results_support} {support_time}, push {friendly} {mission_time} "
    
    # see if tankers are needed -> results_fuel
    
    # see if hostiles are in route -> results_hostiles
    #     if yes identify support -> results support 

    # push out friendly 
