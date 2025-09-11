"""database.py"""

# pylint: disable=C0301

import os
import psycopg2
import json
from dotenv import load_dotenv

load_dotenv()

# Database connection parameters
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

# Tables
PAE_TABLE = "pae_data"
BC3_TABLE = "tmbackup.bc3_with_all_vw"
BC3_FRIENDS_TABLE = "tmbackup.bc3_friends_vw"
OUTPUT_TABLE = "public.mef_data_testing"

# Columns
PAE_COLUMNS = "entity, message, action1, action2, action3, timestamp"
BC3_COLUMNS = "latitude, longitude, trackcategory, callsign, weapon, bc3_jtn, tracknumber, aircraft_type"
BC3_FRIENDS_COLUMNS = "latitude, longitude, trackcategory, callsign, munition_deliverables, ea_deliverables, bc3_jtn, merged_tracknumber, aircraft_type, sensing_deliverables, comm_deliverables"
OUTPUT_COLUMNS = "entity, actions, message, timestamp"
