import requests
import json
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from os import environ
from sqlalchemy import create_engine
import datetime

load_dotenv(find_dotenv("../.env"))


## DADOS CLEVERTAP
headers = {
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json',
    'X-CleverTap-Account-Id': environ.get("clevertap_project_id"),
    'X-CleverTap-Passcode': environ.get("clevertap_password"),
}


## QUERIES
initial_query_columns = "your_columns_from_db___dont_forget_the_required_columns"
initial_query = f"select {initial_query_columns} from your_source_table_(customer_table_for_example)_with_limit_100_because_a_api_limitation"

lookup_query_columns = "your_columns_from_db___dont_forget_the_required_columns"
lookup_query = f"select {lookup_query_columns} from your_source_table_(subscription_table_for_example)"

add_another_useful_information = "select any_other_useful_information from your_source_table"


## MONTA OS DATAFRAMES
def create_dataframe(query, param=None):

    engine = create_engine('mysql+mysqlconnector://%s:%s@%s' % (environ.get("db_user"), environ.get("db_pwd"), environ.get("tns")))
    df = pd.read_sql_query(query.format(param), engine)
    new_json = df.to_json(orient='records')

    return json.loads(new_json)


## LOOKUP BETWEEN TWO TABLES CUSTOMERS X SUBSCRIPTIONS
def lookup_queries():
    users = []

    try:
        with open('files/last_id.txt') as file:
            last_id = file.read()
    except FileNotFoundError:
        last_id = 0


    ## CREATE INITIAL DATAFRAME WITH CUSTOMER'S INFORMATION
    customer_info = create_dataframe(initial_query, last_id)


    ## CREATE JSON TO UPLOAD PROFILES
    print("STARTING AT THE ID:", last_id, " AT: ", datetime.datetime.now())
    for user in customer_info:

        ## SEARCH FOR SUBSCRIPTION INFORMATION
        df_to_lookup = create_dataframe(lookup_query, user['identity'])

        ## IF THERE IS NO INFORMATION, IT INSERTS THE CUSTOMER INFORMATION INTO THE MAIN JSON
            ## AND PUT THE LAST ID PROCESSED INTO THE TXT FILE
        if not df_to_lookup:
            user_data = {
                "type": "profile",
                "identity": user['identity'],
                "profileData": user
            }
            users.append(user_data)

            with open('files/last_id.txt', "w") as file:
                file.write(str(user['identity']))

            last_id = user['identity']
        else:

            ## SEARCH FOR ANOTHER USEFUL INFORMATION
            df_to_additional_information = create_dataframe(add_another_useful_information, user['identity'])

            ## IF EXISTS, MERGE AND INSERTS ALL CUSTOMER INFORMATION INTO THE MAIN JSON
                ## AND PUT THE LAST ID PROCESSED INTO THE TXT FILE
            for add_info in df_to_additional_information:
                user[add_info["New_column"]] = add_info['value']

            user_data = {
                "type": "profile",
                "identity": user['identity'],
                "profileData": {**user, **df_to_lookup[0]}
            }
            users.append(user_data)

            with open('files/last_id.txt', "w") as file:
                file.write(str(user['identity']))

            last_id = user['identity']

    print("LAST ID:", last_id, " PROCESSED AT: ", datetime.datetime.now())
    json_to_upload = {"d": users}

    requests.post("https://us1.api.clevertap.com/1/upload", headers=headers, data=json.dumps(json_to_upload))


if __name__ == '__main__':

    ## UPLOAD 1K PROFILES
    for i in range(10):
        lookup_queries()
