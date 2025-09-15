# establish connection to database. database functions that will pull and put into a dataframe that we can use.
import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

# Database connection settings
load_dotenv()
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Make table name a variable
mef_data = "mef_data_testing"
red_air_act_a2a = "red_air_actionables_air_to_air"
red_air_act_s2a = "red_air_actionables_surf_to_air"
red_air_del_a2a = "red_air_deliverables_air_to_air"
red_air_del_s2a = "red_air_deliverables_surf_to_air"
red_ground_act_a2s = "red_ground_actionables_air_to_surf"
red_ground_act_drone = "red_ground_actionables_drone"
red_ground_act_s2s = "red_ground_actionables_surf_to_surf"
red_ground_del_a2s = "red_ground_deliverables_air_to_surf"
red_ground_del_drone = "red_ground_deliverables_drone"
red_ground_del_s2s = "red_ground_deliverables_surf_to_surf"
red_maritime_act_a2s = "red_maritime_actionables_air_to_surf"
red_maritime_act_drone = "red_maritime_actionables_drone"
red_maritime_act_s2s = "red_maritime_actionables_surf_to_surf"
red_maritime_del_a2s = "red_maritime_deliverables_air_to_surf"
red_maritime_del_drone = "red_maritime_deliverables_drone"
red_maritime_del_s2s = "red_maritime_deliverables_surf_to_surf"


# Use pandas to fetch the data
def query_mef():
    df_mef_data = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {mef_data} order by timestamp desc limit 1;"
        df_mef_data = pd.read_sql(query, conn)

    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()

    return df_mef_data


def query_red_air_act_a2a():
    df_red_air_act_a2a = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_air_act_a2a};"
        df_red_air_act_a2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_air_act_a2a


def query_red_air_act_s2a():
    df_red_air_act_s2a = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_air_act_s2a};"
        df_red_air_act_s2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()

    return df_red_air_act_s2a


def query_red_air_del_a2a():
    df_red_air_del_a2a = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_air_del_a2a};"
        df_red_air_del_a2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()

    return df_red_air_del_a2a


def query_red_air_del_s2a():
    df_red_air_del_s2a = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_air_del_s2a};"
        df_red_air_del_s2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_air_del_s2a


def query_red_ground_act_a2s():
    df_red_ground_act_a2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_ground_act_a2s};"
        df_red_ground_act_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_ground_act_a2s


def query_red_ground_act_drone():
    df_red_ground_act_drone = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_ground_act_drone};"
        df_red_ground_act_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_ground_act_drone


def query_red_ground_act_s2s():
    df_red_ground_act_s2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_ground_act_s2s};"
        df_red_ground_act_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_ground_act_s2s


def query_red_ground_del_a2s():
    df_red_ground_del_a2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_ground_del_a2s};"
        df_red_ground_del_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_ground_del_a2s


def query_red_ground_del_drone():
    df_red_ground_del_drone = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_ground_del_drone};"
        df_red_ground_del_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_ground_del_drone


def query_red_ground_del_s2s():
    df_red_ground_del_s2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_ground_del_s2s};"
        df_red_ground_del_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_ground_del_s2s


def query_red_maritime_act_a2s():
    df_red_maritime_act_a2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_maritime_act_a2s};"
        df_red_maritime_act_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_act_a2s


def query_red_maritime_act_drone():
    df_red_maritime_act_drone = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_maritime_act_drone};"
        df_red_maritime_act_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_act_drone


def query_red_maritime_act_s2s():
    df_red_maritime_act_s2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_maritime_act_s2s};"
        df_red_maritime_act_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_act_s2s


def query_red_maritime_del_a2s():
    df_red_maritime_del_a2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_maritime_del_a2s};"
        df_red_maritime_del_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_del_a2s


def query_red_maritime_del_drone():
    df_red_maritime_del_drone = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_maritime_del_drone};"
        df_red_maritime_del_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_del_drone


def query_red_maritime_del_s2s():
    df_red_maritime_del_s2s = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
        )
        query = f"SELECT * FROM {red_maritime_del_s2s};"
        df_red_maritime_del_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_del_s2s

    # Show preview of data
    # print(df_red_maritime_del_s2s.head())


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


def fetch_json_data(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT actions FROM mef_data_testing;")
        json_data = cursor.fetchall()
        return [item[0] for item in json_data]
    except Exception as e:
        print(f"Error fetching JSON data: {e}")
        return []


def fetch_groundspeed_data(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT callsign, groundspeed FROM bc3_with_all_vw WHERE groundspeed IS NOT NULL;"
        )
        groundspeed_data = cursor.fetchall()
        return {item[0]: item[1] for item in groundspeed_data}
    except Exception as e:
        print(f"Error fetching groundspeed data: {e}")
        return {}
