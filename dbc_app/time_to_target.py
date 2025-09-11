import os
import psycopg2
from dotenv import load_dotenv
from parsing import extract_actions_python
import database


load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

try:
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
    )
    print("Connection successful!")

    cursor = conn.cursor()

    cursor.execute("SELECT groundspeed FROM bc3_with_all_vw WHERE groundspeed IS NOT;")

    rows = cursor.fetchall()
    for row in rows:
        print(f"Ground speed:", row[0])

    cursor.close()
    conn.close()

except Exception as e:
    print(f"Error: {e}")

with psycopg2.connect(
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
) as conn:
    df_min = extract_actions_python(conn, database.mef_data)
    print(df_min["distance_km"])
