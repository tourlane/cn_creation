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
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "RITP", range_="A:AK")

df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)
# Apply the filter to get df_true and df_false
df_true, df_false = dataframe_utils.filter_dataframe(
    df_raw,
    "Is this your first time submitting this form for a Credit Note?",
    "Yes",
    (7, 25),  # Row range from 7 to 25
    (3, 6)    # Initial column range
)

# Adjust df_true to include column 0
df_true = df_raw.iloc[:, [0] + list(range(7, 25))]  # Include column 0 and columns 7 to 25
df_false = df_raw.iloc[:, [0] + list(range(3,6))]  # Include column 0 and columns 7 to 25

# Output the resulting DataFrames
print("Filtered DataFrame (True Condition):")
print(df_true)

print("\nFiltered DataFrame (False Condition):")
print(df_false)

if "Trip ID" not in df_true.columns:
    print("Column 'Trip ID' not found in DataFrame")


df_true = dataframe_utils.handle_trip_ids(df_true, "Trip ID")
print(df_true)

sf_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "Opportunties ID + Invoice ID", range_="A:R")
sf_df = dataframe_utils.process_data_to_dataframe(sf_data)

df_true_2 = dataframe_utils.match_trip_details(df_true, sf_df, "trip id")

gsheet_utils.update_sheet_with_dataframe(service_api,df_true_2,spreadsheet_id,"DB")


# Fetch data from the Google Sheet
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB", range_="A:AH")

# Process the data into a DataFrame
df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)

# Display column names in the DataFrame
print("Column names in the DataFrame:")
print(df_raw.columns.tolist())

# Optional: Save column names for further use
column_names = df_raw.columns.tolist()

# Check for specific column existence (example: 'Trip ID' and 'Grand Total')
required_columns = ["trip id", "grand total"]
for col in required_columns:
    if col in column_names:
        print(f"Column '{col}' is present in the DataFrame.")
    else:
        print(f"Column '{col}' is missing in the DataFrame.")

# Group the data by 'Trip ID' and find the row with the largest 'Grand Total' for each group
df_filtered = df_raw.loc[df_raw.groupby("trip id")["grand total"].idxmax()]

# Reset the index for a clean DataFrame
df_filtered.reset_index(drop=True, inplace=True)

# Output the filtered DataFrame
print("Filtered DataFrame with largest 'Grand Total' for each 'Trip ID':")
print(df_filtered)

# Optional: Update the 'DB' tab with the filtered data
gsheet_utils.update_sheet_with_dataframe(service_api, df_filtered, spreadsheet_id, "DB")
