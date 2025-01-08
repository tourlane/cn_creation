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

class CreditNoteGenerator:
    def __init__(self, starting_credit_note_number=1405):
        self.spreadsheet_id = spreadsheet_id
        self.service_api = service_api
        self.credit_note_counter = starting_credit_note_number

    def group_data_by_timestamp(self, df):
        """Groups the DataFrame by 'Timestamp'."""
        return df.groupby("Timestamp")

    def check_invoice_no(self, group):
        """Checks if all rows in the group have non-null 'Invoice No.' values."""
        return group["Invoice No."].notnull().all()

    def generate_credit_note_number(self):
        """Generates a new credit note number."""
        credit_note_number = f"CN-{self.credit_note_counter:06}"
        self.credit_note_counter += 1
        return credit_note_number

    def update_cn_number_in_df(self, df, group, credit_note_number):
        """Assigns the generated credit note number to the DataFrame."""
        df.loc[group.index, "CN Number"] = credit_note_number

    def get_db_values(self, sheet_range):
        """Fetches the values from the 'DB' sheet."""
        response = self.service_api.spreadsheets().values().get(
            spreadsheetId=self.spreadsheet_id, range=sheet_range
        ).execute()
        return response.get("values", [])

    def update_cn_number_in_db(self, db_values, timestamp, credit_note_number):
        """Updates the 'CN Number' in the 'DB' sheet for the matching timestamp."""
        for row_idx, row in enumerate(db_values):
            if row[0] == timestamp:
                while len(row) <= 32:
                    row.append("")  # Ensure enough columns exist
                row[32] = credit_note_number  # Update the "CN Number" column
        return db_values

    def write_to_db(self, db_values, sheet_range):
        """Writes the updated values back to the 'DB' sheet."""
        self.service_api.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=sheet_range,
            valueInputOption="RAW",
            body={"values": db_values}
        ).execute()

    def process_groups(self, df):
        """Processes each group in the DataFrame and handles credit note assignment."""
        grouped = self.group_data_by_timestamp(df)

        for timestamp, group in grouped:
            print(f"Processing group for timestamp: {timestamp}")

            if self.check_invoice_no(group):
                credit_note_number = self.generate_credit_note_number()
                self.update_cn_number_in_df(df, group, credit_note_number)
                print(f"Assigned credit note number {credit_note_number} to group with timestamp {timestamp}")

                # Update "DB" sheet with the new credit note number
                sheet_range = "DB!A:AA"
                db_values = self.get_db_values(sheet_range)
                db_values = self.update_cn_number_in_db(db_values, timestamp, credit_note_number)
                self.write_to_db(db_values, sheet_range)
            else:
                print(f"Group with timestamp {timestamp} has missing 'Invoice No.' values. Skipping assignment.")

        print(df)  # Display the updated DataFrame


