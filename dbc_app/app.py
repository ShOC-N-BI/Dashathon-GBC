"""
app.py
---------
Entry point for GBC analysis.

Purpose:
- Import friendly and hostile aircraft data from `database.py`.
- For each friendly aircraft:
    1. Run through weapon checks (weapon.py)
    2. Run through hostile evaluation (hostile.py)
    3. Run through fuel analysis (fuel.py)
    4. Run through timing evaluation (time.py)
- Aggregate all results into a structured report for each aircraft.
"""

# === Imports ===
import database 
#import armament
import hostiles
#import fuel
#import time_to_target  
import json

# === Main Workflow ===

def get_friendly_aircraft():
    return 0


def evaluate_aircraft(friendly, target):
    """
    Given a single friendly aircraft and a target aircraft,
    run through all evaluation modules and return results.
    """
    results = {}
    print(target)
    print(friendly)
    
    # 1. Weapon Viability
    #results_amament = armament.check_armaments(friendly, target)

    # 2. Hostile Threat Evaluation
    results_hostiles = hostiles.evaluate_threat(friendly, target)
    return 
    # 3. Fuel Analysis
    results_fuel = fuel.analyze_fuel(friendly, target)

    # 4. Time Analysis
    results_time = time_to_target.compute_time(friendly, target)

    # calculate results 
    #
    # 
    #
    return results


def main():
    """
    Main execution logic:
    - Pull friendly aircraft (3 total) and 1 hostile target from the database.
    - Iterate each friendly through evaluation pipeline.
    - Print or log final summary for all aircraft.
    """
    # Step 1: Get Data
    current_MEF = database.query_mef()
    friendly_aircraft_list = current_MEF['actions'].iloc[0]  # Expect list of 3 aircraft
    #friendly_aircraft_list = json.loads(friendly_aircraft_list)
    #print(type(friendly_aircraft_list))
    #print(friendly_aircraft_list[0].keys())
    target_aircraft = current_MEF['entity'].iloc[0]          # Expect single hostile aircraft

     

    
    # Step 2: Run evaluations
    all_results = {}
    for idx, friendly in enumerate(friendly_aircraft_list, start=1):
        print(f"\n=== Evaluating Friendly Aircraft {idx} ===")
        evaluation = evaluate_aircraft(friendly, target_aircraft)
        all_results[f"Aircraft_{idx}"] = evaluation

    return
    # Step 3: Summarize results
    print("\n===== Final Summary =====")
    for ac_name, eval_data in all_results.items():
        print(f"\nResults for {ac_name}:")
        for category, result in eval_data.items():
            print(f" - {category.capitalize()} Assessment: {result}")

    # TODO: Later we could write results to file, or pass to a reporting module


if __name__ == "__main__":
    main()
