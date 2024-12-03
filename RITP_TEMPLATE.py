from google_sheet_processor import GoogleSheetUtils, DataFrameUtils
from dotenv import load_dotenv
import os
import time
import pandas as pd
from datetime import datetime

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
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB", range_="A:AF")

# Process the data into a DataFrame
df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)

# Filter data based on 'Invoice No.' column being empty
df_false_db, df_true_db = dataframe_utils.filter_dataframe(
    df_raw,  # The DataFrame you're working with
    "Invoice No.",  # Column to check
    "",  # Filter for empty values
    (0, 29),  # Optional: Range for rows to consider
    (0, 29)   # Optional: Additional range for column indices, if needed
)

# Initialize the starting credit note number
credit_note_counter = 1405

# Define the cell mapping for the "Template"
cell_mapping = {
    "A4": "Sales Agent",
    "A5": "Address Line 1",
    "A6": "City",
    "A7": "Post Code/ZIP Code",
    "A8": "Country"
}

multi_row_fields = {
    "Invoice No.": "A27",  # Starting cell for Invoice No.
}



def get_dynamic_value(cell, group):
    """
    Determine the value to populate for a specific cell dynamically.
    """
    if cell == "G7":  # Example: Today's date
        return datetime.today().strftime("%Y-%m-%d")
    elif cell == "G8":  # Example: Extract month from "Created Date" column
        created_date = group.iloc[0].get("Created Date", None)
        if created_date:  # Ensure the column exists and has a value
            try:
                # Convert to datetime and extract the month name
                return pd.to_datetime(created_date).strftime("%B")
            except Exception as e:
                print(f"Error processing date in 'Created Date': {e}")
                return "Invalid Date"
    return None  # Default for unmapped cells


# Function to update a cell with a delay to avoid rate limits
def update_cell_with_delay(sheet_id, cell_range, value, credentials):
    service = gsheet_utils.build_service(credentials)
    body = {"range": cell_range, "values": [[value]], "majorDimension": "ROWS"}
    service.spreadsheets().values().update(
        spreadsheetId=sheet_id,
        range=cell_range,
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()
    time.sleep(2)  # Add a delay of 2 seconds to avoid rate limits


# Group the DataFrame by "Timestamp"
grouped = df_true_db.groupby("Timestamp")

for timestamp, group in grouped:
    print(f"Processing group for timestamp: {timestamp}")

    if group["Invoice No."].notnull().all():
        # Generate credit note number
        credit_note_number = f"CN-{credit_note_counter:06}"
        credit_note_counter += 1
        print(f"Generated credit note number: {credit_note_number}")

        # Copy the template sheet
        sheet_copy_name = f"CN-{credit_note_number}"
        gsheet_utils.copy_sheet(service_api, spreadsheet_id, "Template", sheet_copy_name)
        print(f"Copied template to: {sheet_copy_name}")

        # Update fixed fields (static mappings) in the template
        first_row = group.iloc[0]
        for template_cell, db_column in cell_mapping.items():
            if db_column in first_row.index:  # Ensure the column exists in the DataFrame
                value = first_row[db_column]
                print(f"Updating {template_cell} in {sheet_copy_name} with value '{value}' from column '{db_column}'")
                update_cell_with_delay(
                    spreadsheet_id,
                    f"{sheet_copy_name}!{template_cell}",
                    value,
                    credentials
                )

        # Update dynamic fields
        dynamic_cells = ["G7", "G8"]  # Example: Add cells requiring dynamic values
        for cell in dynamic_cells:
            value = get_dynamic_value(cell, group)
            if value:
                print(f"Updating {cell} in {sheet_copy_name} with dynamic value '{value}'")
                update_cell_with_delay(
                    spreadsheet_id,
                    f"{sheet_copy_name}!{cell}",
                    value,
                    credentials
                )

        # Update multi-row fields in the template (process each row)
        for field, start_cell in multi_row_fields.items():
            if field in group.columns:  # Ensure the column exists in the DataFrame
                for i, (_, row) in enumerate(group.iterrows()):  # Iterate through the group rows
                    target_cell = f"{start_cell[0]}{int(start_cell[1:]) + i}"  # Adjust cell based on index
                    value = row[field]
                    print(f"Updating {target_cell} in {sheet_copy_name} with value '{value}' for field '{field}'")
                    update_cell_with_delay(
                        spreadsheet_id,
                        f"{sheet_copy_name}!{target_cell}",
                        value,
                        credentials
                    )
