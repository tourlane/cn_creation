from google.oauth2 import service_account
import googleapiclient.discovery
import os
from dotenv import load_dotenv
import pandas as pd
import gspread

# Load environment variables from .env
load_dotenv()

# Get the path to the service account JSON file
service_account_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# Get the spreadsheet ID from the environment variables
spreadsheet_id = os.getenv('SPREADSHEET_ID')

# Authenticate using the service account JSON file
credentials = service_account.Credentials.from_service_account_file(service_account_file)

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

# Identify duplicated column names
duplicated_columns = df.columns[df.columns.duplicated()].unique()

print(f"Duplicated columns: {duplicated_columns}")

# Loop through each duplicated column and merge them
for col in duplicated_columns:
    # Find all columns that have the same name
    cols_to_merge = df.columns[df.columns == col].tolist()  # Convert to list for consistency

    if len(cols_to_merge) > 1:
        print(f"Merging the following columns: {cols_to_merge}")

        # Merge the values of the duplicated columns, keeping non-NaN values
        merged_values = df[cols_to_merge].apply(
            lambda row: row.dropna().iloc[0] if not row.dropna().empty else None,
            axis=1
        )

        # Check if merged_values has the correct length and assign to the first column of the duplicates
        if len(merged_values) == len(df):
            # Use .iloc to reference the first occurrence of the duplicated column
            df.iloc[:, df.columns.get_loc(cols_to_merge[0])] = merged_values

            # Drop the other duplicated columns (all but the first one)
            cols_to_drop = cols_to_merge[1:]  # Drop all but the first column
            df.drop(columns=cols_to_drop, inplace=True)

            print(f"Column '{col}' merged successfully.")
        else:
            print(f"Warning: Merged values length mismatch for column '{col}'.")

# Print the DataFrame after merging columns
print("DataFrame after merging duplicated columns:")
print(df)

# Identify columns containing 'Trip ID'
trip_id_columns = [col for col in df.columns if 'Trip ID' in col]

print(f"Columns containing 'Trip ID': {trip_id_columns}")

# Loop through each column containing 'Trip ID' and merge them
for col in trip_id_columns:
    # Find all columns that contain 'Trip ID'
    cols_to_merge = df.columns[df.columns.str.contains('Trip ID')].tolist()  # List all 'Trip ID' columns

    if len(cols_to_merge) > 1:
        print(f"Merging the following columns: {cols_to_merge}")

        # Merge the values of the duplicated columns, keeping non-NaN values
        merged_values = df[cols_to_merge].apply(
            lambda row: row.dropna().iloc[0] if not row.dropna().empty else None,
            axis=1
        )

        # Assign merged values to the first occurrence of the 'Trip ID' column
        df[cols_to_merge[0]] = merged_values

        # Drop the other 'Trip ID' columns (all but the first one)
        cols_to_drop = cols_to_merge[1:]  # Drop all but the first column
        df.drop(columns=cols_to_drop, inplace=True)

        print(f"Columns containing 'Trip ID' merged successfully.")

# Print the DataFrame after merging 'Trip ID' columns
print("DataFrame after merging 'Trip ID' columns:")
print(df)

# Try to open the 'GFM' worksheet, if it doesn't exist, create it
try:
    gfm_sheet = spreadsheet.worksheet('GFM')
except gspread.exceptions.WorksheetNotFound:
    gfm_sheet = spreadsheet.add_worksheet(title='GFM', rows="100", cols="26")

# Convert the DataFrame to a list of lists (gspread format)
data_to_upload = df.values.tolist()

# Insert the header row
gfm_sheet.insert_row(df.columns.tolist(), 1)

# Upload the data starting from row 2
gfm_sheet.insert_rows(data_to_upload, 2)

print("Data successfully uploaded to the 'GFM' tab.")
