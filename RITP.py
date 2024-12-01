from google.oauth2 import service_account
import googleapiclient.discovery
import os
from dotenv import load_dotenv
import pandas as pd
import gspread

# Load environment variables from .env
load_dotenv()

# Get the path to the service account JSON file
service_account_file = os.getenv('SERVICE_ACCOUNT_FILE')

# Validate the file path
if not service_account_file or not os.path.exists(service_account_file):
    raise FileNotFoundError(f"Service account file not found: {service_account_file}")

# Use the credentials
credentials = service_account.Credentials.from_service_account_file(service_account_file)

spreadsheet_id = os.getenv("SPREADSHEET_ID")

if not spreadsheet_id:
    raise ValueError("SPREADSHEET_ID is not set in the .env file")


# Build the Sheets API service
service = googleapiclient.discovery.build('sheets', 'v4', credentials=credentials)

# Use the spreadsheet ID to interact with the Sheets API
spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

sheet_range = "RITP!A:Y"
responses = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=sheet_range).execute()

# Extract the data from the 'values' key if it exists in the dictionary
if isinstance(responses, dict) and 'values' in responses:
    data = responses['values']

    # Check if there is data to work with
    if data:
        # Convert the list of rows into a pandas DataFrame
        df = pd.DataFrame(data)

        # Use the first row (index 0) as the column headers
        new_header = df.iloc[0]  # The first row
        df = df[1:]  # Remove the first row (which is now the header)
        df.columns = new_header  # Set the header

        # Now, let's inspect the DataFrame and print the first few rows
        print("DataFrame with updated headers:")
        print(df.head())  # Print first 5 rows to check

        # If needed, adjust missing values
        df.fillna("N/A", inplace=True)  # Example: Fill N/A for missing values
    else:
        print("The 'values' list is empty.")
else:
    print("The expected 'values' key is missing in the responses dictionary.")


# Filter rows based on the column value
df_1 = df[df["Is this your first time submitting this form for a Credit Note?"] == "Yes"].iloc[:, 7:24]
df_2 = df[df["Is this your first time submitting this form for a Credit Note?"] == "No"].iloc[:, 3:5]

# Display the resulting DataFrames
print("DataFrame df_1 (H to Y for 'yes'):")
print(df_1)

print("\nDataFrame df_2 (D to G for others):")
print(df_2)
