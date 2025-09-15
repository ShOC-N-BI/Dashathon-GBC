#establish connection to database. database functions that will pull and put into a dataframe that we can use.
import psycopg2
import pandas as pd

# Database connection settings 
DB_HOST = "10.5.185.53"       
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
red_maritine_act_a2s = "red_maritine_actionables_air_to_surf"
red_maritine_act_drone = "red_maritine_actionables_drone"
red_maritine_act_s2s = "red_maritine_actionables_surf_to_surf"
red_maritine_del_a2s = "red_maritine_deliverables_air_to_surf"
red_maritine_del_drone = "red_maritine_deliverables_drone"
red_maritine_del_s2s = "red_maritine_deliverables_surf_to_surf"
get_friendly_aircraft = "vc3_with_all_vw"

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    # Use pandas to fetch the data
    query = f"SELECT * FROM {mef_data};"
    df_mef_data = pd.read_sql(query, conn)

    query = f"SELECT * FROM {red_air_act_a2a};"
    df_red_air_act_a2a = pd.read_sql(query, conn)

    query = f"SELECT * FROM {red_air_act_s2a};"
    df_red_air_act_s2a = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_air_del_a2a};"
    df_red_air_del_a2a = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_air_del_s2a};"
    df_red_air_del_s2a = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_ground_act_a2s};"
    df_red_ground_act_a2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_ground_act_drone};"
    df_red_ground_act_drone = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_ground_act_s2s};"
    df_red_ground_act_s2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_ground_del_a2s};"
    df_red_ground_del_a2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_ground_del_drone};"
    df_red_ground_del_drone = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_ground_del_s2s};"
    df_red_ground_del_s2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_maritine_act_a2s};"
    df_red_maritine_act_a2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_maritine_act_drone};"
    df_red_maritine_act_drone = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_maritine_act_s2s};"
    df_red_maritine_act_s2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_maritine_del_a2s};"
    df_red_maritine_del_a2s = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_maritine_del_drone};"
    df_red_maritine_del_drone = pd.read_sql(query, conn)
    
    query = f"SELECT * FROM {red_maritine_del_s2s};"
    df_red_maritine_del_s2s = pd.read_sql(query, conn)

    query = f"SELECT * FROM {get_friendly_aircraft};"
    df_get_friendly_aircraft = pd.read_sql(query, conn)

    #Show preview of data
    # print(df_red_maritine_del_s2s.head())

except Exception as e:
    print("Error:", e)

finally:
    if 'conn' in locals():
        conn.close()
