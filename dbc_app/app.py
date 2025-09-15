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
from database import get_friendly_aircraft, get_target_aircraft
import armament
import hostiles
import fuel
import time_to_target  # renamed to avoid conflict with Python's built-in time module

# === Main Workflow ===

def evaluate_aircraft(friendly, target):
    """
    Given a single friendly aircraft and a target aircraft,
    run through all evaluation modules and return results.
    """
    results = {}

    # 1. Weapon Viability
    # Check if this aircraft has the correct loadout / range / probability of kill
    results["weapon"] = armament.check_armaments(friendly, target)

    # 2. Hostile Threat Evaluation
    # Determine threat level, defensive systems, engagement risk
    results["hostile"] = hostiles.evaluate_threat(friendly, target)

    # 3. Fuel Analysis
    # Calculate if this aircraft can reach target, loiter, and return safely
    results["fuel"] = fuel.analyze_fuel(friendly, target)

    # 4. Time Analysis
    # Check how long until intercept, engagement window, timing constraints
    results["time"] = time_to_target.compute_time(friendly, target)

    return results


def main():
    """
    Main execution logic:
    - Pull friendly aircraft (3 total) and 1 hostile target from the database.
    - Iterate each friendly through evaluation pipeline.
    - Print or log final summary for all aircraft.
    """

    # Step 1: Get Data
    friendly_aircraft_list = get_friendly_aircraft()  # Expect list of 3 aircraft
    target_aircraft = get_target_aircraft()           # Expect single hostile aircraft

    # Step 2: Run evaluations
    all_results = {}
    for idx, friendly in enumerate(friendly_aircraft_list, start=1):
        print(f"\n=== Evaluating Friendly Aircraft {idx} ===")
        evaluation = evaluate_aircraft(friendly, target_aircraft)
        all_results[f"Aircraft_{idx}"] = evaluation

    # Step 3: Summarize results
    print("\n===== Final Summary =====")
    for ac_name, eval_data in all_results.items():
        print(f"\nResults for {ac_name}:")
        for category, result in eval_data.items():
            print(f" - {category.capitalize()} Assessment: {result}")

    # TODO: Later we could write results to file, or pass to a reporting module


if __name__ == "__main__":
    main()
