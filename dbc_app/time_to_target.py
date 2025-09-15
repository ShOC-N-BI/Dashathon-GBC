import os
import psycopg2
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Retrieve database connection details from environment variables
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")


def connect_db():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None


# Fetch JSON data from the 'mef_data_testing' table
def fetch_json_data(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT actions FROM mef_data_testing;")
        json_data = cursor.fetchall()

        # No need for json.loads() since psycopg2 converts jsonb to Python objects
        return [
            item[0] for item in json_data
        ]  # Extract the JSON data directly from the tuple

    except Exception as e:
        print(f"Error fetching JSON data: {e}")
        return []


# Fetch groundspeed data from the 'bc3_with_all_vw' table
def fetch_groundspeed_data(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT callsign, groundspeed FROM bc3_with_all_vw WHERE groundspeed IS NOT NULL;"
        )
        groundspeed_data = cursor.fetchall()
        return {
            item[0]: item[1] for item in groundspeed_data
        }  # Return as a dictionary {callsign: groundspeed}
    except Exception as e:
        print(f"Error fetching groundspeed data: {e}")
        return {}


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
conn = connect_db()
if not conn:
    exit()

json_data = fetch_json_data(conn)
groundspeed_data = fetch_groundspeed_data(conn)
results = compute_time(json_data, groundspeed_data)

# Print the results
for result in results:
    print(
        f"Callsign: {result['callsign']}, Distance: {result['distance_km']} km, "
        f"Groundspeed: {result['groundspeed_kmh']} km/h, Time: {result['time_hours']:.2f} hours"
    )

# Close the database connection
conn.close()
