from google_sheet_processor import GoogleSheetUtils, DataFrameUtils
from dotenv import load_dotenv
import os
import pandas as pd
import re

# Load environment variables from .env
load_dotenv()

# Retrieve the spreadsheet ID
spreadsheet_id = os.getenv("SPREADSHEET_ID")

gsheet_utils = GoogleSheetUtils()
dataframe_utils = DataFrameUtils()

# Load service account credentials and build the Sheets API service
credentials = gsheet_utils.load_credentials("inv-cn-creation.json")
service_api = gsheet_utils.build_service(credentials)

# Fetch data from "RITP" tab
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "RITP", range_="A:Y")
print(f"Fetched {len(sheet_data)} rows from the RITP sheet.")

# Convert to DataFrame
df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)
print(f"DataFrame shape: {df_raw.shape}")

# Standardize column names
df_raw.columns = df_raw.columns.str.strip().str.lower().str.replace(" ", "_")
print("Standardized columns:", df_raw.columns.tolist())

# Dynamically rename columns if they exist (prevent misalignment)
column_names = df_raw.columns.tolist()

if len(df_raw.columns) > 17:  # Ensure the column exists
    df_raw.columns.values[17] = 'trip_id_2'
    df_raw.columns.values[8] = 'first_name_2'
    df_raw.columns.values[9] = 'last_name_2'

# Dynamically rename columns if they exist (prevent misalignment)
column_names = df_raw.columns.tolist()

# Merge 'First Name', 'Last Name', and 'Trip ID' based on the condition
df_raw['first_name'] = df_raw.apply(
    lambda row: row['first_name_2'] if row['is_this_your_first_time_submitting_this_form_for_a_credit_note?'] == "Yes" else row['first_name'], axis=1
)

df_raw['last_name'] = df_raw.apply(
    lambda row: row['last_name_2'] if row['is_this_your_first_time_submitting_this_form_for_a_credit_note?'] == "Yes" else row['last_name'], axis=1
)

df_raw['trip_id'] = df_raw.apply(
    lambda row: row['trip_id_2'] if row['is_this_your_first_time_submitting_this_form_for_a_credit_note?'] == "Yes" else row['trip_id'], axis=1
)

# Drop the duplicate columns
df_raw.drop(columns=['first_name_2', 'last_name_2', 'trip_id_2'], inplace=True)

# Function to expand rows based on Trip IDs
def expand_trip_id_rows(row):
    trip_ids = re.findall(r'T-\d{6}-\d+', str(row['trip_id']))  # Regex to find trip_id
    if not trip_ids:
        return [row]
    expanded_rows = []
    for trip_id in trip_ids:
        new_row = row.copy()
        new_row['trip_id'] = trip_id
        expanded_rows.append(new_row)
    return expanded_rows

# Ensure 'trip_id' contains all possible trip_ids by splitting and expanding them
expanded_rows = []
for _, row in df_raw.iterrows():
    expanded_rows.extend(expand_trip_id_rows(row))

# Create the expanded DataFrame
df_raw_expanded = pd.DataFrame(expanded_rows)

# Normalize the trip_id by stripping spaces and converting to lowercase
df_raw_expanded['trip_id'] = df_raw_expanded['trip_id'].str.strip().str.lower()

# Remove rows where trip_id is 'None' or empty
df_raw_expanded = df_raw_expanded[df_raw_expanded['trip_id'].notna()]
df_raw_expanded = df_raw_expanded[df_raw_expanded['trip_id'] != '']
df_raw_expanded = df_raw_expanded[df_raw_expanded['trip_id'] != 'none']  # Ensure 'None' is removed

# Remove duplicates based on 'trip_id'
df_raw_expanded = df_raw_expanded.drop_duplicates(subset=['trip_id'])

# Normalize the trip_id by stripping spaces and converting to lowercase
df_raw_expanded['trip_id'] = df_raw_expanded['trip_id'].str.strip().str.lower()

# Replace any internal spaces or unwanted characters (e.g., commas or extra spaces) in trip_id
df_raw_expanded['trip_id'] = df_raw_expanded['trip_id'].apply(lambda x: re.sub(r'\s+', '', x))

# Remove rows where trip_id is 'None' or empty
df_raw_expanded = df_raw_expanded[df_raw_expanded['trip_id'].notna()]
df_raw_expanded = df_raw_expanded[df_raw_expanded['trip_id'] != '']
df_raw_expanded = df_raw_expanded[df_raw_expanded['trip_id'] != 'none']  # Ensure 'None' is removed

# Remove duplicates based on 'trip_id'
df_raw_expanded = df_raw_expanded.drop_duplicates(subset=['trip_id'])

# Check the shape and unique trip_ids
print(f"DataFrame shape after cleaning: {df_raw_expanded.shape}")
print(df_raw_expanded['trip_id'].unique())  # To see unique trip_ids remaining

# Ensure 'trip_id' contains all possible trip_ids by splitting and expanding them
expanded_rows = []
for _, row in df_raw.iterrows():
    expanded_rows.extend(expand_trip_id_rows(row))

# Create the expanded DataFrame
df_raw_expanded = pd.DataFrame(expanded_rows)
print(f"Expanded DataFrame shape: {df_raw_expanded.shape}")


# Fetch Salesforce data
try:
    sf_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "Opportunties ID + Invoice ID", range_="A2:R")
except Exception as e:
    print("Failed to fetch data from 'Opportunities ID + Invoice ID'. Please check the range or sheet name.")
    print(e)
    exit(1)

sf_df = dataframe_utils.process_data_to_dataframe(sf_data)

# Rename column 'Trip' in sf_df to match 'trip_id' in df_raw_expanded
sf_df.rename(columns={"Trip": "trip_id"}, inplace=True)

# Check for the existence of 'trip_id'
if 'trip_id' not in df_raw_expanded.columns or 'trip_id' not in sf_df.columns:
    raise ValueError('Required columns "trip_id" are missing in one or both DataFrames.')

print("Columns in df_raw_expanded:", df_raw_expanded.columns)
print("Columns in sf_df:", sf_df.columns)


df_combined = df_raw_expanded.merge(
    sf_df,
    on="trip_id",  # Replace with the appropriate join key if different
    how="left",
    suffixes=('', '_sf')  # To distinguish columns from "Opportunities ID + Invoice ID"
)


# Debugging: Print column names to verify renaming
print("Updated 'Opportunities ID + Invoice ID' columns:", sf_df.columns.tolist())


# Remove duplicates based on 'trip_id' before updating DB
df_combined = df_combined.drop_duplicates(subset=['trip_id'])


# Debugging: Check merge output
print("Combined DataFrame shape:", df_combined.shape)
print(df_combined.head())

# Remove leading single quotes and convert to proper datetime format
if 'timestamp' in df_combined.columns:
    df_combined['timestamp'] = df_combined['timestamp'].str.lstrip("'")  # Remove leading single quotes
    df_combined['timestamp'] = pd.to_datetime(df_combined['timestamp'], errors='coerce')  # Ensure it's in datetime format
    df_combined['timestamp'] = df_combined['timestamp'].dt.strftime('%m/%d/%Y %H:%M:%S')  # Convert back to string format



# Check the shape and unique trip_ids after deduplication
print(f"Combined DataFrame shape after deduplication: {df_combined.shape}")
print(df_combined['trip_id'].unique())  # To see unique trip_ids remaining
df_combined = df_combined.fillna("")

# Now update the "DB" tab with the cleaned and deduplicated data
gsheet_utils.update_sheet_with_dataframe(service_api, df_combined, spreadsheet_id, "DB")
print("Updated the DB tab successfully.")



# Columns to retrieve when 'is_this_your_first_time_submitting_this_form_for_a_credit_note?' is 'No'
columns_to_update = [
    'location', 'address_line_1', 'city', 'post_code/zip_code', 'country',
    'file_of_contract', 'signed_date', 'tax_status',
    'taxpayer_identification_number_(tin)', 'vat_id', 'iban', 'bic', 'account_number', 'swift'
]

# Create a copy of the DataFrame to avoid modifying the original
df_combined_copy = df_combined.copy()

# Iterate through rows where 'is_this_your_first_time_submitting_this_form_for_a_credit_note?' is 'No'
for index, row in df_combined_copy.iterrows():
    if row['is_this_your_first_time_submitting_this_form_for_a_credit_note?'] == 'No':
        email = row['email_address']  # Get the email address
        # Find the first matching row in DB with the same email
        matching_row = df_combined_copy[df_combined_copy['email_address'] == email].iloc[0]

        # Update the corresponding columns in the current row
        for col in columns_to_update:
            if col in matching_row:
                df_combined_copy.at[index, col] = matching_row[col]

# Verify the updated DataFrame
print(df_combined_copy.head())

# Save the updated combined DataFrame to Google Sheets or CSV

# Update the "DB" tab with combined data
gsheet_utils.update_sheet_with_dataframe(service_api, df_combined_copy, spreadsheet_id, "DB")
print("Updated the DB tab successfully.")




