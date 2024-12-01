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
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "RITP", range_="A:AC")

df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)
df_true, df_false= dataframe_utils.filter_dataframe(df_raw, "Is this your first time submitting this form for a Credit Note?",
                                 "Yes", (7,25), (3,5))
# Output the resulting DataFrames
print("Filtered DataFrame (True Condition):")
print(df_true)

print("\nFiltered DataFrame (False Condition):")
print(df_false)

df_true = dataframe_utils.handle_trip_ids(df_true, "Trip ID")
print(df_true)

sf_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "OptInvID", range_="A:J")
sf_df = dataframe_utils.process_data_to_dataframe(sf_data)

df_true_2 = dataframe_utils.match_trip_details(df_true, sf_df, "trip id")  # Correct trip column here
print(df_true_2)

gsheet_utils.update_sheet_with_dataframe(service_api,df_true_2,spreadsheet_id,"DB")
