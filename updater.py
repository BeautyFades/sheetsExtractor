import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

import sqlalchemy
import pandas

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
SPREADSHEET_ID = 'YOUR_SPREADSHEET_ID'
RANGE = 'OrderTable'

DB_NAME = 'YOUR_DB_NAME'
DB_USER = 'YOUR_DB_USER'
DB_PASSWORD = 'YOUR_DB_USER_PASSWORD'
DB_PUBLIC_IP = 'YOUR_DB_IP_ADDRESS'
DB_PORT = 'YOUR_DB_PORT'
DB_TABLE = 'YOUR_DB_TABLE'

DATA_TYPE = {
            'OrderDate': sqlalchemy.types.INTEGER(),
            'OrderID': sqlalchemy.types.NUMERIC(),
            'CustomerRegion': sqlalchemy.types.VARCHAR(length=40),
            'ProductList': sqlalchemy.types.VARCHAR(length=128),
            'ContactInfo': sqlalchemy.types.VARCHAR(length=40),
            'ReviewRating': sqlalchemy.types.NUMERIC(),
            'Observations': sqlalchemy.types.VARCHAR(length=100),
            }

# The ID and range of a sample spreadsheet
def extract_from_gsheets():
    """Extracts all data inside the predetermined RANGE from the selected
    SPREADSHEET_ID
    """
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=RANGE).execute()
    values = result.get('values', [])
    data = pandas.DataFrame.from_records(values[1:], columns=values[0])

    return data


def update(data, db_name, db_user, db_password, db_public_ip, db_port, 
           db_table):
    """Attempts log into a database and replaces all data inside the 
    predetermined Table
    """
    db = sqlalchemy.create_engine(f'postgresql://{db_user}:{db_password}@{db_public_ip}:{db_port}/{db_name}')
    data.to_sql(db_table, db, index=False, if_exists='replace', dtype=DATA_TYPE)

print(extract_from_gsheets().to_string())

ans = input('Update? (y/n):')
if ans == 'y' or ans == 'Y':
    update(extract_from_gsheets(), DB_NAME, DB_USER, DB_PASSWORD, DB_PUBLIC_IP, DB_PORT, DB_TABLE)
    print('Updated')
else:
    print('Aborted')

