from google_sheet_processor import GoogleSheetUtils, DataFrameUtils
from dotenv import load_dotenv
import os
import pandas as pd
import time
import re

from googleapiclient.http import MediaIoBaseDownload
import io
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials


load_dotenv()


# Load environment variables from .env
spreadsheet_id = os.getenv("SPREADSHEET_ID")

# Retrieve the spreadsheet ID from the .env file
gsheet_utils = GoogleSheetUtils()

# Initialize utility classes
dataframe_utils = DataFrameUtils()
credentials = gsheet_utils.load_credentials("inv-cn-creation.json")

# Load credentials and build the Sheets API service
service_api = gsheet_utils.build_service(credentials)
def build_drive_service(credentials):
    """
    Build and return the Google Drive service using the provided credentials.
    """
    return build('drive', 'v3', credentials=credentials)

# Build the Google Drive service
service_drive = build_drive_service(credentials)  # Using the function to build the Drive service

sheet_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB", range_="A:AC")

# Fetch and process the data from the "DB" tab in Google Sheets
def export_to_pdf(service_drive, file_id, output_filename):
    """
    Export a Google Sheet to PDF using Google Drive API.

    :param service_drive: Google Drive API service instance.
    :param file_id: The ID of the Google Sheet to export.
    :param output_filename: The name of the output PDF file.
    :return: The path to the saved PDF file.
    """
    try:
        # Specify the MIME type for PDF export
        request = service_drive.files().export_media(fileId=file_id, mimeType='application/pdf')

        # Download the exported PDF
        file_path = f"{output_filename}.pdf"
        with io.FileIO(file_path, 'wb') as pdf_file:
            downloader = MediaIoBaseDownload(pdf_file, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"Download progress: {int(status.progress() * 100)}%")

        print(f"PDF exported successfully: {file_path}")
        return file_path

    except Exception as e:
        print(f"Error exporting to PDF: {e}")
        raise
df = dataframe_utils.process_data_to_dataframe(sheet_data)

# Define the template sheet name
template_sheet_name = "Template"


def generate_unique_sheet_name(service, spreadsheet_id, base_name):
    """
    Generates a unique sheet name by checking if the sheet already exists and incrementing the suffix if needed.
    """
    try:
        # Get all the sheet names in the spreadsheet
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_names = [sheet['properties']['title'] for sheet in sheet_metadata['sheets']]

        # Check if the sheet name already exists
        if base_name not in sheet_names:
            return base_name

        # If the sheet name exists, increment the number at the end of the base name
        sheet_number = int(base_name.split('-')[1])
        new_name = f"{base_name.split('-')[0]}-{sheet_number + 1:06d}"

        # Ensure the new name is unique
        return generate_unique_sheet_name(service, spreadsheet_id, new_name)

    except Exception as e:
        print(f"Error generating unique sheet name: {e}")
        raise

current_cn_number = "CN-001404"
# Inside the loop where you are generating sheet names
new_sheet_name = generate_unique_sheet_name(service_api, spreadsheet_id, current_cn_number)

# Now proceed with copying the sheet
gsheet_utils.copy_sheet(service_api, spreadsheet_id, template_sheet_name, new_sheet_name)



# Loop through the rows in the DataFrame
for index, row in df.iterrows():
    # Check if "Invoice No." is missing or empty
    invoice_no = row.get("Invoice No.", "").strip()
    if not invoice_no:
        print(f"Skipping Trip ID {row.get('Trip ID', 'Unknown')} due to missing Invoice No.")
        continue  # Skip this iteration if Invoice No. is missing

    # Generate the next CN Number
    current_cn_number = get_next_cn_number(current_cn_number)

    # Generate a unique sheet name based on the CN Number
    new_sheet_name = generate_unique_sheet_name(service_api, spreadsheet_id, current_cn_number)

    # Copy the template sheet for the new Credit Note
    gsheet_utils.copy_sheet(service_api, spreadsheet_id, template_sheet_name, new_sheet_name)

    # Define the values to update in the new Credit Note
    value_dict = {
        "A4": row.get("Sales Agent", ""),  # Replace with actual column name
        "A5": row.get("Address Line 1", ""),
        "A6": row.get("City", ""),
        "A7": row.get("Post Code/ZIP Code", ""),
        "A8": row.get("Country", ""),
        "A11": row.get("IBAN", ""),
        "G6": current_cn_number,  # Assign the new CN Number
        "C27": invoice_no,  # Invoice Number from the row
        "D28": row.get("Grand Total", ""),  # Replace with actual column name for the total amount
        "E18": row.get("Trip ID", "")  # Trip ID from the row
    }

    # Update the new sheet with the row data
    update_values(service_api, spreadsheet_id, new_sheet_name, value_dict)

    # Save the Credit Note to a PDF file
    pdf_file = export_to_pdf(service_drive, spreadsheet_id, new_sheet_name)

    # Print confirmation
    print(f"Credit Note {current_cn_number} created for Trip ID {row.get('Trip ID', 'Unknown')} with Invoice No. {invoice_no}. PDF saved as: {pdf_file}")

print("All valid Credit Notes have been created.")
