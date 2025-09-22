# establish connection to database. database functions that will pull and put into a dataframe that we can use.
import psycopg2
import pandas as pd
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv
import datetime
import os
import json

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
bc3_with_all_vw = "bc3_with_all_vw"
entity = "pae_data"
user_input = "user_input"
bc3_friends_vw = "bc3_friends_vw"

def insert_data(entity: str, actions, message, timestamp) -> None:
    print(timestamp)
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = "INSERT INTO mef_data_testing (entity, actions, message, timestamp) VALUES (%s, %s, %s, %s);"
        params = (entity, actions, message, timestamp)  # <-- fixed here
        with conn.cursor() as cur:
            cur.execute(query, params)
        conn.commit()  # don't forget to commit
        print(f"{entity},{actions},{ message},{timestamp}")
    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()

def push_coa_to_db(target_aircraft_id: str, coa: dict, target_message: str, target_time: str, table_name: str = "gronemeier_frontend_testing"):
    try:
        # Convert COA to JSON string
        coa_json = json.dumps(coa) if coa else None

        # Validation check
        if not target_aircraft_id:
            print("Skipping insert: target_aircraft_id is null/empty")
            return
        if not target_time:
            print("Skipping insert: target_time is null/empty")
            return
        if not coa_json or coa_json == "[]":
            print("Skipping insert: coa_json is null/empty")
            return

        # Connect and insert if all checks pass
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        with conn.cursor() as cur:
            insert_query = f"""
            INSERT INTO {table_name} (entity, five_line, message, timestamp)
            VALUES (%s, %s, %s, %s)
            """
            cur.execute(insert_query, (target_aircraft_id, coa_json, target_message, target_time))
            conn.commit()
            print(f"Inserted COA for target {target_aircraft_id} into {table_name}")

    except Exception as e:
        print("Error inserting COA:", e)
    finally:
        if 'conn' in locals():
            conn.close()



def query_assets(column: str, operator:str, filter: str) -> list:
    results = []
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )

        # Use parameterized query to prevent SQL injection
        query = f"SELECT * FROM {bc3_with_all_vw} WHERE {column} {operator} '{filter}' AND aircraft_type NOT LIKE 'DIS(265)';"
        with conn.cursor() as cur:
            cur.execute(query,)
            columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                results.append(dict(zip(columns, row)))
    except Exception as e:
        print("Error:", e)
    finally:
        if 'conn' in locals():
            conn.close()
    return results

def query_awacs() -> list:
    results = []
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = """
            SELECT * FROM bc3_with_all_vw
            WHERE aircraft_type IN (%s, %s, %s, %s, %s)
            AND bc3_jtn IS NOT NULL
            AND bc3_jtn != '[null]';
        """
        params = ("E-3", "E3", "E7", "E-2C", "E2D")
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                results.append(dict(zip(columns, row)))
    except Exception as e:
        print("Error:", e)
    finally:
        if 'conn' in locals():
            conn.close()
    return results

def query_ew() -> list:
    results = []
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = """
            SELECT * FROM bc3_with_all_vw
            WHERE aircraft_type IN (%s, %s, %s, %s, %s)
            AND bc3_jtn IS NOT NULL
            AND bc3_jtn != '[null]'
            AND trackid = 'Friend';
        """
        params = ("EA18G", "EC-130", "EA37B", "RC135VW", "RC-135")
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                results.append(dict(zip(columns, row)))
    except Exception as e:
        print("Error:", e)
    finally:
        if 'conn' in locals():
            conn.close()
    return results

def query_tankers() -> list:
    results = []
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = """
            SELECT * FROM bc3_with_all_vw
            WHERE aircraft_type IN (%s, %s, %s)
            AND bc3_jtn IS NOT NULL
            AND bc3_jtn != '[null]';
        """
        params = ("KC-135", "KC135", "KC46")
        with conn.cursor() as cur:
            cur.execute(query, params)
            columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                results.append(dict(zip(columns, row)))
    except Exception as e:
        print("Error:", e)
    finally:
        if 'conn' in locals():
            conn.close()
    return results




    # Use pandas to fetch the data
def query_friendly_asset(bc3_jtn: str) -> pd.DataFrame:
    df_friendly_asset = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )

        # Use parameterized query to prevent SQL injection
        query = f"SELECT * FROM {bc3_with_all_vw} WHERE bc3_jtn = %s;"
        df_friendly_asset = pd.read_sql(query, conn, params=(bc3_jtn,))
        
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()

    return df_friendly_asset


def query_mef(): 
    df_mef_data = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = f"SELECT * FROM {mef_data} order by timestamp desc limit 1;"
        df_mef_data = pd.read_sql(query, conn)

    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()

    return df_mef_data

def query_all_mef(): 
    df_mef_data = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = f"SELECT * FROM {mef_data} order by timestamp desc;"
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
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
            host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD
        )
        query = f"SELECT * FROM {red_maritime_del_s2s};"
        df_red_maritime_del_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return df_red_maritime_del_s2s

    #Show preview of data
def query_bc3_with_all_vw():
    df_bc3_with_all_vw = pd.DataFrame()
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        query = f"SELECT * FROM {bc3_with_all_vw};"
        df_bc3_with_all_vw = pd.read_sql(query, con=engine)

    except Exception as e:
        print("Error:", e)

    return df_bc3_with_all_vw 

def query_user_input():
    df_user_input = pd.DataFrame()
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        query = f"SELECT * FROM {user_input} order by timestamp desc limit 1;"
        df_user_input = pd.read_sql(query, con=engine)

    except Exception as e:
        print("Error:", e)

    return df_user_input 

def query_bc3_friends_vw():
    df_bv3_friends_vw = pd.DataFrame()
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        query = f"SELECT * FROM {bc3_friends_vw};"
        df_bv3_friends_vw = pd.read_sql(query, con=engine)

    except Exception as e:
        print("Error:", e)

    return df_bv3_friends_vw 

def get_groundspeed(identifier: str) -> pd.DataFrame:
    # print(identifier)
    groundspeed = pd.DataFrame()
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        query = f"SELECT * FROM {bc3_with_all_vw} WHERE bc3_jtn = %s;"
        groundspeed = pd.read_sql(query, conn, params=(identifier,))
    except Exception as e:
        print("Error:", e)

    finally:
        if "conn" in locals():
            conn.close()
    return groundspeed

import pandas as pd


def record_exists(asset_tn, target_tn):
    try:
        # Create SQLAlchemy engine
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        )

        query = text("SELECT EXISTS (SELECT 1 FROM user_input WHERE asset_tn = :asset AND target_tn = :target);")
        with engine.connect() as conn:
            result = conn.execute(query, {"asset": asset_tn, "target": target_tn}).scalar()
            print(result)
            return result

    except Exception as e:
        print("Error checking record existence:", e)
        return False
