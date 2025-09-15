# main.py
import database
import json


# Function to calculate time for each entry based on distance and groundspeed
def compute_time(json_data, groundspeed_data):
    results = []

    for entry in json_data:
        if isinstance(entry, list):
            for sub_entry in entry:
                callsign = sub_entry.get("callsign")
                distance = sub_entry.get("distance_km")

                # Get the groundspeed for this callsign
                groundspeed = groundspeed_data.get(callsign)

                if (
                    groundspeed is not None and groundspeed != 0
                ):  # If groundspeed is not null and not zero
                    time_hours = distance / groundspeed  # Time = Distance / Speed
                    results.append(
                        {
                            "callsign": callsign,
                            "distance_km": distance,
                            "groundspeed_kmh": groundspeed,
                            "time_hours": time_hours,
                        }
                    )
                else:
                    print(f"Warning: Invalid groundspeed for {callsign}. Skipping...")
        elif isinstance(entry, dict):
            callsign = entry.get("callsign")
            distance = entry.get("distance_km")

            # Get the groundspeed for this callsign
            groundspeed = groundspeed_data.get(callsign)

            if (
                groundspeed is not None and groundspeed != 0
            ):  # If groundspeed is not null and not zero
                time_hours = distance / groundspeed  # Time = Distance / Speed
                results.append(
                    {
                        "callsign": callsign,
                        "distance_km": distance,
                        "groundspeed_kmh": groundspeed,
                        "time_hours": time_hours,
                    }
                )
            else:
                print(f"Warning: Invalid groundspeed for {callsign}. Skipping...")

    return results


# Connect to the database, fetch data, and calculate time
conn = database.connect_db()
if not conn:
    exit()

json_data = database.fetch_json_data(conn)
groundspeed_data = database.fetch_groundspeed_data(conn)
results = compute_time(json_data, groundspeed_data)

# Print the results
for result in results:
    print(
        f"Callsign: {result['callsign']}, Distance: {result['distance_km']} km, "
        f"Groundspeed: {result['groundspeed_kmh']} km/h, Time: {result['time_hours']:.2f} hours"
    )

# Close the database connection
conn.close()
