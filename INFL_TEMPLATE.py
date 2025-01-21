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
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB-INFL", range_="A:AZ")

# Process the data into a DataFrame
df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)

# Initialize the starting credit note number
credit_note_counter = 1432

# Define the cell mapping for the "Template-CC"
cell_mapping = {
    "A4": "full_name",
    "A5": "Address Line 1",
    "A6": "city_postal",
    "A7": "Country",
    "B8": "Taxpayer Identification Number (TIN)",
    "B9": "VAT ID",
    "A11": "IBAN",
    "A12": "BIC",
    "B16": "Signed Date",
    "F32": "vat_percentage",
    "G32": "vat_amount",
    "C20" : "IG Handle",
    "A21" : "Reason for refund (Hotel change, car rental, etc)",
    "F21" : "Refund Amount",

}

multi_row_fields = {
    "Invoice: Trip Detail: Record Type" :"A31",
    "Invoice: Invoice No." : "C31",
     "Amount" : "F31",
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

df_raw = df_raw[df_raw['full_name'] == "Yulia Slavinskaya"]
# Group the DataFrame by "Timestamp"
grouped = df_raw.groupby("Timestamp")

for Timestamp, group in grouped:
    print(f"Processing group for Timestamp : {Timestamp}")

    if group["Invoice: Invoice No."].notnull().all():
        # Generate credit note number
        credit_note_number = f"CN-INFL-{credit_note_counter:06}"
        credit_note_counter += 1
        print(f"Generated credit note number: {credit_note_number}")

        # Copy the template sheet
        sheet_copy_name = f"{credit_note_number}"
        gsheet_utils.copy_sheet(service_api, spreadsheet_id, "Template-INFL", sheet_copy_name)
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
                    print(f"Updating {target_cell} with value '{value}' for field '{field}'")
                    update_cell_with_delay(
                        spreadsheet_id,
                        f"{sheet_copy_name}!{target_cell}",
                        value,
                        credentials
                    )


def locate_and_calculate_tax(template_data, cell_mapping):
    """
    Calculate tax dynamically based on the country and net total.

    :param template_data: Dictionary with template data, e.g., cell values.
    :param cell_mapping: Mapping of template cells to data fields.
    """
    # Extract country from the template
    country_cell = cell_mapping.get("A7", None)
    country = template_data.get(country_cell, "").strip().lower()

    # Locate the base amount next to "Net Total"
    base_amount = None
    for cell, value in template_data.items():
        if value == "Net Total":  # Look for the "Net Total" label
            adjacent_cell = get_adjacent_cell(cell, direction="right")  # Define this helper function
            base_amount = template_data.get(adjacent_cell, 0)
            break

    # If base amount is found, calculate tax
    if base_amount is not None and isinstance(base_amount, (int, float)):
        tax_value = base_amount * 0.19 if country == "Germany" or "Deutschland" else 0
        # Update the calculated tax in cell A32
        template_data["A32"] = tax_value
        print(f"Calculated tax for {country}: {tax_value} written to A32")
    else:
        print("Base amount not found or invalid. Cannot calculate tax.")


def get_adjacent_cell(current_cell, direction="right"):
    """
    Get the adjacent cell reference based on direction (right, left, up, down).
    :param current_cell: Current cell reference, e.g., "G31".
    :param direction: Direction to find the adjacent cell.
    :return: Adjacent cell reference, e.g., "H31".
    """
    import re
    match = re.match(r"([A-Z]+)(\d+)", current_cell)
    if not match:
        return None
    col, row = match.groups()
    row = int(row)
    if direction == "right":
        col = chr(ord(col) + 1)
    elif direction == "left":
        col = chr(ord(col) - 1)
    elif direction == "down":
        row += 1
    elif direction == "up":
        row -= 1
    return f"{col}{row}"


# Example Usage
template_data = {
    "A7": "Germany",
    "G31": "Net Total",
    "H31": 1000,  # Base amount is in H31
    "A32": None
}
locate_and_calculate_tax(template_data, cell_mapping)