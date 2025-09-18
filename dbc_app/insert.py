import database
import json

def grab_user():
    input = database.query_user_input()
    return input

def insert_data():
    input = grab_user()
    database.insert_data(input["asset_tn"],(input["battle_effect"]),input["target_tn"],input["timestamp"])

insert_data()