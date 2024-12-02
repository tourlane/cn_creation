from google_sheet_processor import GoogleSheetUtils, DataFrameUtils
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables from .env
load_dotenv()

# Retrieve the spreadsheet ID from the .env file
spreadsheet_id = os.getenv("SPREADSHEET_ID")

gsheet_utils = GoogleSheetUtils()
dataframe_utils = DataFrameUtils()

# Get the path to the service account JSON file
credentials = gsheet_utils.load_credentials("inv-cn-creation.json")

# Build the Sheets API service
service_api = gsheet_utils.build_service(credentials)

# Fetch data from the Google Sheet
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB", range_="A:AC")

df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)
df_false, df_true = dataframe_utils.filter_dataframe(
    df_raw,  # The DataFrame you're working with
    "Invoice No.",  # Column to check
    "",  # Filter for empty values
    (0, 29),  # Optional: Range for rows to consider
    (0, 29)   # Optional: Additional range for column indices, if needed
)

# Remove rows where "Invoice No." is empty or contains specific invalid values like "N/A"
df_true = df_true[~df_true["Invoice No."].isin(["", "N/A"])]

# Group the DataFrame by the "Timestamp" column
df_grouped = df_true.groupby("Timestamp").agg(lambda x: list(x))  # Aggregates the data into lists

# Print the DataFrame with valid "Invoice No." values
print(df_true)
print(df_grouped)
