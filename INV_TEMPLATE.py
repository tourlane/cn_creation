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
sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "InvDB", range_="A:Z")

# Process the data into a DataFrame
df_raw = dataframe_utils.process_data_to_dataframe(sheet_data)

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

# Define the cell mappings
cell_mapping = {
    "A1": "Entity",
    "A4": "Customer Name",
    "A5": "Address Line 1",
    "A6": "City Postal",
    "A7": "Country",
    "B8": "Taxpayer Identification Number (TIN)",
    "B9": "VAT ID",
    "F11": "Service Period",
    "F8": "Inv Number",
    "A26": "Vat Percentage",
    "F26": "Vat Amount",
    "A42": "Requester Name",
    "A43": "Title/Position",
    "F25": "Subtotal"
}

multi_row_fields = {
    "Product": "A21",
    "Quantity": "D21",
    "Unit Price": "E21",
}

# Function to mark rows as "Done" in RINV and InvDB tabs
def mark_as_done(sheet_id, tab_name, row_index, credentials):
    cell_range = f"{tab_name}!Status{row_index + 2}"  # Assuming "Status" is in the correct column
    update_cell_with_delay(sheet_id, cell_range, "Done", credentials)

# Modified function to create and update invoices
def create_invoice(df_raw, row_idx, last_invoice_number, credentials):
    # Skip rows already marked as "Done"
    if df_raw.iloc[row_idx]["Status"] == "Done":
        print(f"Row {row_idx + 1} already processed. Skipping...")
        return last_invoice_number

    # Copy the "Inv-Template" tab
    sheet_copy_name = f"Invoice-{last_invoice_number}"
    gsheet_utils.copy_sheet(service_api, spreadsheet_id, "Inv-Template", sheet_copy_name)
    print(f"Copied 'Inv-Template' to: {sheet_copy_name}")

    # Get the data for the invoice from the dataframe (use the current row)
    invoice_data = df_raw.iloc[row_idx]  # Get data for the current row

    # Fill out static fields based on the cell_mapping
    for template_cell, db_column in cell_mapping.items():
        if db_column in invoice_data.index:
            value = invoice_data[db_column]
            print(f"Updating {template_cell} in {sheet_copy_name} with value '{value}' from column '{db_column}'")
            update_cell_with_delay(
                spreadsheet_id,
                f"{sheet_copy_name}!{template_cell}",
                value,
                credentials
            )
    # Sum values from F21 (row 21, column 'F') and F22 (row 22, column 'F')
    subtotal = float(invoice_data.iloc[20, invoice_data.columns.get_loc('F')]) + float(
        invoice_data.iloc[21, invoice_data.columns.get_loc('F')])

    # Assuming df_raw is the DataFrame and invoice_data is a row in that DataFrame
    invoice_data = df_raw.iloc[row_idx]  # Get the row of interest as a Series

    # Calculate subtotal: sum of F21 and F22
    subtotal = float(invoice_data['F21']) + float(invoice_data['F22'])

    # Output the subtotal value
    print(f"Setting Subtotal (F25) to {subtotal} in {sheet_copy_name}")
    update_cell_with_delay(
        spreadsheet_id,
        f"{sheet_copy_name}!F25",
        subtotal,
        credentials
    )

    # Handle VAT logic
    tax_status = invoice_data["Tax Status"]
    if tax_status == "Within Germany":
        vat_percentage = "VAT 19%"
        vat_amount = subtotal * 19 / 100  # Calculate VAT based on subtotal
    else:
        vat_percentage = "VAT 0%"
        vat_amount = 0

    # Set today's date in cell F9 as the billing date
    today_date = datetime.today().strftime('%Y-%m-%d')
    print(f"Setting billing date (F9) in {sheet_copy_name} to {today_date}")
    update_cell_with_delay(
        spreadsheet_id,
        f"{sheet_copy_name}!F9",
        today_date,
        credentials
    )

    # Handle VAT logic
    tax_status = invoice_data["Tax Status"]
    if tax_status == "Within Germany":
        vat_percentage = "VAT 19%"
        vat_amount = float(invoice_data["F25"]) * 19 / 100  # Assuming amount is in F25
    else:
        vat_percentage = "VAT 0%"
        vat_amount = 0

    # Update VAT percentage and amount
    print(f"Setting VAT percentage (A26) to {vat_percentage} in {sheet_copy_name}")
    update_cell_with_delay(
        spreadsheet_id,
        f"{sheet_copy_name}!A26",
        vat_percentage,
        credentials
    )

    print(f"Setting VAT amount (F25) to {vat_amount} in {sheet_copy_name}")
    update_cell_with_delay(
        spreadsheet_id,
        f"{sheet_copy_name}!F26",
        vat_amount,
        credentials
    )

    # Fill out multi-row fields (e.g., Product, Quantity, Unit Price)
    for field, start_cell in multi_row_fields.items():
        if field in invoice_data.index:
            value = invoice_data[field]
            print(f"Updating {start_cell} in {sheet_copy_name} with value '{value}' for {field}")
            update_cell_with_delay(
                spreadsheet_id,
                f"{sheet_copy_name}!{start_cell}",
                value,
                credentials
            )

    # Mark the row as "Done" in both RINV and InvDB tabs
    mark_as_done(spreadsheet_id, "RINV", row_idx, credentials)
    mark_as_done(spreadsheet_id, "InvDB", row_idx, credentials)

    # Return the next invoice number
    return generate_invoice_number(last_invoice_number)

# Start with the first invoice number
last_invoice_number = "RE-240171"

# Create invoices, updating the invoice number sequentially for each row in the 'InvDB' tab
for row_idx in range(len(df_raw)):  # Loop through each row in the DataFrame
    print(f"Processing row {row_idx + 1}...")
    last_invoice_number = create_invoice(df_raw, row_idx, last_invoice_number, credentials)