#establish connection to database. database functions that will pull and put into a dataframe that we can use.
import psycopg2
import pandas as pd

# Database connection settings 
DB_HOST = "10.5.185.21"       
DB_PORT = "5432"            
DB_NAME = "shooca_db"   
DB_USER = "shooca"   
DB_PASS = "shooca222"   

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
friendly_asset = "bc3_with_all_vw"
entity = "pae_data"

def query_assets(column: str, filter: str) -> pd.DataFrame:
    df_asset = pd.DataFrame()
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        # Use parameterized query to prevent SQL injection
        query = f"SELECT * FROM {friendly_asset} WHERE {column} = '{filter}';"
        df_asset = pd.read_sql(query, conn,)
        
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()

    return df_asset

def query_tankers() -> list:
    results = []
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        query = """
            SELECT * FROM bc3_with_all_vw
            WHERE aircraft_type IN (%s, %s, %s)
            AND bc3_jtn IS NOT NULL
            AND bc3_jtn != '[null]';
        """
        params = ("KC-135", "KC-10", "KC-46")
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
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )

        # Use parameterized query to prevent SQL injection
        query = f"SELECT * FROM {friendly_asset} WHERE bc3_jtn = %s;"
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
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {mef_data} order by timestamp desc limit 1;"
        df_mef_data = pd.read_sql(query, conn)

    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()   

    
    return df_mef_data
 
    

def query_red_air_act_a2a():
    df_red_air_act_a2a = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_air_act_a2a};"
        df_red_air_act_a2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()    
    return df_red_air_act_a2a

def query_red_air_act_s2a():
    df_red_air_act_s2a = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_air_act_s2a};"
        df_red_air_act_s2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()   
    
    return df_red_air_act_s2a

def query_red_air_del_a2a():
    df_red_air_del_a2a = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_air_del_a2a};"
        df_red_air_del_a2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()
    
    return df_red_air_del_a2a

def query_red_air_del_s2a():
    df_red_air_del_s2a = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_air_del_s2a};"
        df_red_air_del_s2a = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()           
    return df_red_air_del_s2a

def query_red_ground_act_a2s():
    df_red_ground_act_a2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_ground_act_a2s};"
        df_red_ground_act_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()           
    return df_red_ground_act_a2s

def query_red_ground_act_drone():
    df_red_ground_act_drone = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_ground_act_drone};"
        df_red_ground_act_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()        
    return df_red_ground_act_drone

def query_red_ground_act_s2s():
    df_red_ground_act_s2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_ground_act_s2s};"
        df_red_ground_act_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()       
    return df_red_ground_act_s2s

def query_red_ground_del_a2s():
    df_red_ground_del_a2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_ground_del_a2s};"
        df_red_ground_del_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()       
    return df_red_ground_del_a2s

def query_red_ground_del_drone():
    df_red_ground_del_drone = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_ground_del_drone};"
        df_red_ground_del_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()           
    return df_red_ground_del_drone

def query_red_ground_del_s2s():
    df_red_ground_del_s2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_ground_del_s2s};"
        df_red_ground_del_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()          
    return df_red_ground_del_s2s

def query_red_maritime_act_a2s():
    df_red_maritime_act_a2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_maritime_act_a2s};"
        df_red_maritime_act_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()           
    return df_red_maritime_act_a2s

def query_red_maritime_act_drone():
    df_red_maritime_act_drone = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_maritime_act_drone};"
        df_red_maritime_act_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()   
    return df_red_maritime_act_drone

def query_red_maritime_act_s2s():
    df_red_maritime_act_s2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_maritime_act_s2s};"
        df_red_maritime_act_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()       
    return df_red_maritime_act_s2s

def query_red_maritime_del_a2s():
    df_red_maritime_del_a2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_maritime_del_a2s};"
        df_red_maritime_del_a2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()      
    return df_red_maritime_del_a2s

def query_red_maritime_del_drone():
    df_red_maritime_del_drone = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_maritime_del_drone};"
        df_red_maritime_del_drone = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()       
    return df_red_maritime_del_drone

def query_red_maritime_del_s2s():
    df_red_maritime_del_s2s = pd.DataFrame()
    try:
    # Connect to PostgreSQL
        conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
        query = f"SELECT * FROM {red_maritime_del_s2s};"
        df_red_maritime_del_s2s = pd.read_sql(query, conn)
    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals():
            conn.close()       
    return df_red_maritime_del_s2s

    #Show preview of data
    # print(df_red_maritime_del_s2s.head())


