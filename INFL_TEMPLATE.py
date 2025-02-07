# Function to format amounts in European format (with period as decimal separator and comma for thousand separator)
def format_european_amount(amount):
    try:
        # Format the amount with the comma as thousand separator
        formatted_amount = locale.format_string('%.2f', amount, grouping=True)
        # Replace the comma (decimal separator) with a period
        return formatted_amount.replace(',', '.')
    except Exception as e:
        print(f"Error formatting amount: {e}")
        return str(amount)  # Return the raw value if formatting fails


# Your existing code starts here...
from CC import db_cc_df_expanded
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
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB-CC_2", range_="A:AK")

# Process the data into a DataFrame
df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)

# Initialize the starting credit note number
credit_note_counter = 1426

# Define the cell mapping for the "Template-CC"
cell_mapping = {
    "A4": "full_name",
    "A5": "address_line_1",
    "A6": "city_postal",
    "A7": "country",
    "B8": "tin",
    "B9": "vat_id",
    "A11": "iban",
    "A12": "bic",
    "B16": "signed_date",
    "F32": "vat_percentage",
    "G32": "vat_amount",
}

multi_row_fields = {
    "type" :"A29",
    "Invoice: Invoice No." : "C29",
    "Amount" : "F29",
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


# Group the DataFrame by "email_address"
grouped = df_raw.groupby("email_address")

for email_address, group in grouped:
    print(f"Processing group for email_address: {email_address}")

    if group["Invoice: Invoice No."].notnull().all():
        # Generate credit note number
        credit_note_number = f"CN-CC-{credit_note_counter:06}"
        credit_note_counter += 1
        print(f"Generated credit note number: {credit_note_number}")

        # Copy the template sheet
        sheet_copy_name = f"{credit_note_number}"
        gsheet_utils.copy_sheet(service_api, spreadsheet_id, "Template-CC", sheet_copy_name)
        print(f"Copied template to: {sheet_copy_name}")

        # Update G6 with the credit note number
        print(f"Updating G6 in {sheet_copy_name} with credit note number 'CN.{credit_note_number}'")
        update_cell_with_delay(
            spreadsheet_id,
            f"{sheet_copy_name}!G6",
            credit_note_number,
            credentials
        )

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

        for field, start_cell in multi_row_fields.items():
            if field in group.columns:
                print(f"Updating multi-row field: {field} (starting at {start_cell})")
                for i, (_, row) in enumerate(group.iterrows()):
                    target_cell = f"{start_cell[0]}{int(start_cell[1:]) + i}"
                    value = row[field]

                    # If it's the "Amount" field, format it as European style
                    if field == "Amount":
                        value = format_european_amount(value)

                    print(f"Updating {target_cell} with value '{value}' for field '{field}'")
                    update_cell_with_delay(
                        spreadsheet_id,
                        f"{sheet_copy_name}!{target_cell}",
                        value,
                        credentials
                    )
