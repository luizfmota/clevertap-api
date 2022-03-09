import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
from os import environ
from sqlalchemy import create_engine

load_dotenv(find_dotenv(".env"))

## QUERY TO GET PROFILE INFO
columns = "your_columns_from_db___dont_forget_the_required_columns"
query = f"select {columns} from your_source_table"


## CLEVERTAP HEADER INFO
headers = {
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json',
    'X-CleverTap-Account-Id': environ.get("clevertap_project_id"),
    'X-CleverTap-Passcode': environ.get("clevertap_password"),
}


## CATALOG INFO
name = "your_catalog_name"
creator = "catalog_creator"
email = "catalog_creator_email"
csv_file_path = "files/catalog_to_update.csv"


def create_dataframe(query):
    """
    MOUNT DATAFRAME TO CREATE CSV FILE
    :param query: QUERY TO SEARCH CATALOG ITEMS INFORMATIONS
    :return: DATAFRAME MOUNTED
    """
    print('Starting dataframe`s creation')
    print('...')
    engine = create_engine('mysql+mysqlconnector://%s:%s@%s' % (environ.get("db_user"), environ.get("db_pwd"), environ.get("tns")))
    df = pd.read_sql_query(query, engine)
    print('DataFrame created')
    print('...')

    return df


def create_csv(df):
    """
    CREATE CSV FILE TO IMPORT
    :param df: MOUNTED DATAFRAME
    """
    print('Starting CSV creation')
    print('...')
    catalog = pd.DataFrame(df)
    catalog.to_csv(csv_file_path, index=False)
    print('CSV Created')
    print('...')


def upload_catalog():
    """
    CREATE NEW UPDATE URL > CHECK IF IT'S OK > UPLOAD CSV FILE > DOES THE CONFIRMATION
    """
    print('Starting Upload URL Creation')
    print('...')
    response = requests.post('https://us1.api.clevertap.com/get_catalog_url', headers=headers)

    if response.status_code == 200:
        url_put = response.json()['presignedS3URL']
        print('URL for upload created. Starting Upload')
        print('...')
        upload_csv = requests.put(url_put, data=open(csv_file_path, 'rb'))

        if upload_csv.status_code == 200:
            print('Upload completed... Sending confirmation that the CSV has been uploaded')
            print('...')
            catalog_data = {'name': name, 'creator': creator, 'email': email, 'url': url_put, 'replace': 'true'} #Replace - true for overwrite existent catalog. false for new catalog.
            confirmation = requests.post('https://us1.api.clevertap.com/upload_catalog_completed', headers=headers, data=str(catalog_data))

            if confirmation.status_code == 200:
                print('Confirmation finished')
            else:
                print('Confirmation not done. Status: ', confirmation.status_code)
        else:
            print('Upload not done. Status: ', upload_csv.status_code)
    else:
        print('URL creation not done. Status: ', response.status_code)


if __name__ == '__main__':

    query_result = create_dataframe(query)
    create_csv(query_result)
    upload_catalog()
