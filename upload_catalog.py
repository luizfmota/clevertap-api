import pandas as pd
import requests
from sqlalchemy import create_engine


## CONNECTION INFORMATION
uid = "your_db_user"
pwd = "your_db_password"
db = "your_db_schema"
tns = "your_db_host" + db


## QUERY TO GET CATALOG INFO
columns = "your_columns_from_db___dont_forget_the_required_columns"
query = "select "+columns+" from your_source_table"


## CLEVERTAP HEADER INFO
project_id = "your_project_id"
token = "your_token"
password = "your_password"
headers = {
    'Cache-Control': 'no-cache',
    'Content-Type': 'application/json',
    'X-CleverTap-Account-Id': project_id,
    'X-CleverTap-Passcode': password,
}


## CATALOG INFO
name = "your_catalog_name"
creator = "catalog_creator"
email = "catalog_creator_email"
csv_file_name = "catalog_to_update.csv"


## MOUNT DATAFRAME TO CREATE CSV FILE
def create_dataframe(query):

    print('Starting dataframe`s creation')
    print('...')
    engine = create_engine('mysql+mysqlconnector://%s:%s@%s' % (uid, pwd, tns))
    df = pd.read_sql_query(query, engine)
    print('DataFrame created')
    print('...')

    return df


## CREATE CSV FILE
def create_csv(df):
  
    print('Starting CSV creation')
    print('...')
    catalog = pd.DataFrame(df)
    catalog.to_csv(csv_file_name, index=False)
    print('CSV Created')
    print('...')


## UPLOAD CSV TO CATALOG
def upload_catalog():

    print('Starting Upload URL Creation')
    print('...')
    response = requests.post('https://us1.api.clevertap.com/get_catalog_url', headers=headers)

    if response.status_code == 200:
        url_put = response.json()['presignedS3URL']
        print('URL for upload created. Starting Upload')
        print('...')
        upload_csv = requests.put(url_put, data=open(csv_file_name, 'rb'))

        if upload_csv.status_code == 200:
            print('Upload completed... Sending confirmation that the CSV has been uploaded')
            print('...')
            confirmation = requests.post('https://us1.api.clevertap.com/upload_catalog_completed', headers=headers, data="{'name': '"+name+"', 'creator': '"+creator+"', 'email': '"+email+"', 'url': '"+url_put+"', 'replace':false}") #Replace - true if the catalog exists. false for new catalog.

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
