expanded_rows = []
import pandas as pd
from google.oauth2 import service_account
import googleapiclient.discovery
import os
import re
import time


class GoogleSheetUtils:
    @staticmethod
    def load_credentials(service_account_file: str):
        """Load credentials from the service account file."""
        if not service_account_file or not os.path.exists(service_account_file):
            raise FileNotFoundError(f"Service account file not found: {service_account_file}")
        return service_account.Credentials.from_service_account_file(service_account_file)

    @staticmethod
    def build_service(credentials):
        """Build the Google Sheets API service."""
        return googleapiclient.discovery.build('sheets', 'v4', credentials=credentials)

    @staticmethod
    def fetch_sheet_data(service, spreadsheet_id, tab_name, range_):
        """Fetch data from a specific tab and range."""
        sheet_range = f"{tab_name}!{range_}"
        response = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=sheet_range
        ).execute()
        if isinstance(response, dict) and 'values' in response:
            return response['values']
        return []

    @staticmethod
    def update_sheet_with_dataframe(service, df, spreadsheet_id, tab_name, start_cell="A2"):
        """Updates a Google Sheet with a DataFrame."""
        try:
            # Convert DataFrame to list of lists (for use with Google Sheets API)
            # Replace NaN and None values with an empty string or suitable value
            df_cleaned = df.fillna("").replace([None, "NaN"], "")

            # Convert the cleaned DataFrame to a list of lists
            values = df_cleaned.values.tolist()

            # Prepare the range for the update (based on the starting cell and size of the DataFrame)
            sheet_range = f"{tab_name}!{start_cell}"

            # Prepare the request to update the sheet
            body = {
                'values': values
            }

            # Use the Sheets API to update the sheet
            sheet = service.spreadsheets()
            sheet.values().update(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                valueInputOption="RAW",
                body=body
            ).execute()

            print(f"Data successfully written to {tab_name} at {start_cell}")
        except Exception as e:
            print(f"An error occurred: {e}")

    @staticmethod
    def update_cells(service_api, spreadsheet_id, sheet_name, value_dict):
        for cell, value in value_dict.items():
            body = {
                "values": [[value]]
            }
            service_api.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!{cell}",
                valueInputOption="RAW",
                body=body
            ).execute()

    @staticmethod
    def copy_sheet(service, spreadsheet_id, template_sheet_name, new_sheet_name):
        # Get the sheet ID of the template sheet
        sheets = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_id = None
        for sheet in sheets["sheets"]:
            if sheet["properties"]["title"] == template_sheet_name:
                sheet_id = sheet["properties"]["sheetId"]
                break

        # Copy the template sheet to create a new sheet with the given name
        request = {
            "requests": [{
                "duplicateSheet": {
                    "sourceSheetId": sheet_id,
                    "newSheetName": new_sheet_name
                }
            }]
        }
        service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=request).execute()

    @staticmethod
    def update_cell_with_delay(sheet_id, cell_range, value):
        service = googleapiclient.discovery.build('sheets', 'v4', credentials=credentials)
        body = {"range": cell_range, "values": [[value]], "majorDimension": "ROWS"}
        service.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=cell_range,
            valueInputOption="USER_ENTERED",
            body=body
        ).execute()
        time.sleep(4)  # Add a delay of 1 second


class DataFrameUtils:
    @staticmethod
    def process_data_to_dataframe(raw_data):
        """Process raw sheet data into a pandas DataFrame."""
        if raw_data:
            df = pd.DataFrame(raw_data)
            new_header = df.iloc[0]  # Use the first row as header
            df = df[1:]  # Remove the first row (now the header)
            df.columns = new_header  # Set the header
            df.fillna("N/A", inplace=True)  # Fill missing values with "N/A"
            return df
        raise ValueError("No data available to process into DataFrame.")

    @staticmethod
    def filter_dataframe(df, column, value, col_range_true, col_range_false):
        """
        Filter rows based on a specific column value and create two DataFrames:
        - Rows that meet the condition (True) with one range of columns.
        - Rows that do not meet the condition (False) with another range of columns.

        Args:
            df (pd.DataFrame): The input DataFrame.
            column (str): Column to apply the filter on.
            value (Any): Value to filter by.
            col_range_true (tuple): (start, end) column range for rows matching the condition.
            col_range_false (tuple): (start, end) column range for rows not matching the condition.

        Returns:
            tuple: (DataFrame for rows meeting condition, DataFrame for rows not meeting condition)
        """
        # Rows where the condition is True
        df_true = df[df[column] == value].iloc[:, col_range_true[0]:col_range_true[1]]

        # Rows where the condition is False
        df_false = df[df[column] != value].iloc[:, col_range_false[0]:col_range_false[1]]

        return df_true, df_false

    @staticmethod
    def merge_columns(df, columns_to_merge):
        """Merge duplicated columns into one column, either with or without values."""
        for column in columns_to_merge:
            if column in df.columns:
                df[column] = df[columns_to_merge].apply(lambda row: ' '.join([str(x) for x in row if pd.notnull(x)]),
                                                        axis=1)
                df = df.drop(columns=columns_to_merge[1:])  # Drop the original duplicate columns
        return df

    @staticmethod
    def handle_trip_ids(df, trip_column):
        """
        Process a DataFrame by splitting trip IDs into individual rows,
        then filter out rows with invalid Trip ID values.
        """
        expanded_rows = []  # Initialize a list to store expanded rows

        for index, row in df.iterrows():
            try:
                # Split trip IDs by ',' or ' ' and handle possible formatting issues
                trip_ids = re.split(r'[,\s]+', str(row[trip_column]).strip())  # Ensure no leading/trailing spaces

                # Create a new row for each trip ID
                for trip_id in trip_ids:
                    new_row = row.copy()
                    new_row[trip_column] = trip_id  # Replace the trip column with the individual ID
                    expanded_rows.append(new_row)

            except Exception as e:
                print(f"Error processing row at index {index}: {e}")

        # Convert the expanded rows back into a DataFrame
        expanded_df = pd.DataFrame(expanded_rows)

        # Filter rows where the Trip ID doesn't start with "T_"
        filtered_df = expanded_df[expanded_df[trip_column].str.startswith("T-")]

        return filtered_df

    @staticmethod
    def match_trip_details(df_1, optinv_df, trip_column):
        """Match trip details from another DataFrame based on a trip column."""
        try:
            # Strip whitespaces from column names for both DataFrames
            df_1.columns = df_1.columns.str.strip()
            optinv_df.columns = optinv_df.columns.str.strip()

            # Ensure column names are consistent
            df_1.columns = df_1.columns.str.lower()
            optinv_df.columns = optinv_df.columns.str.lower()

            # Ensure that 'trip id' in df_1 and 'trip' in optinv_df are present
            if "trip id" not in df_1.columns or "trip" not in optinv_df.columns:
                raise KeyError('The "Trip ID" column was not found in df_1 or "Trip" was not found in optinv_df.')

            # Clean up trip IDs to ensure they are comparable
            df_1[trip_column] = df_1[trip_column].astype(
                str).str.strip()  # Ensure the trip ID is a string and strip spaces
            optinv_df["trip"] = optinv_df["trip"].astype(
                str).str.strip()  # Ensure the trip column is a string and strip spaces

            # Create a new DataFrame to store the results
            result_df = pd.DataFrame(columns=df_1.columns)

            # Iterate over df_1 rows to match trip details
            for idx, row in df_1.iterrows():
                trip_ids = str(row[trip_column]).strip()  # This corresponds to "Trip ID" in df_1

                # Skip matching if trip_ids is null
                if pd.isnull(trip_ids):
                    continue

                # Split the trip_ids into a list of trip ID strings (in case there are multiple trip IDs in the cell)
                trip_ids_list = [trip_id.strip() for trip_id in trip_ids.split(",")]

                # Flag to track if a match was found for any trip ID
                matched_any_trip = False

                # Iterate over the list of trip IDs to match against optinv_df
                for trip_id in trip_ids_list:
                    # Find the matched invoices from optinv_df
                    matched_invoices = optinv_df[optinv_df["trip"] == trip_id]

                    # If matched invoices exist, create a new row for each matched trip
                    if not matched_invoices.empty:
                        for col in matched_invoices.columns:
                            row[col] = matched_invoices.iloc[0][col]  # Merge matched row

                        # Add a new row to the result dataframe for each matched trip ID
                        result_df = pd.concat([result_df, pd.DataFrame([row])], ignore_index=True)
                        matched_any_trip = True
                    else:
                        # If no match is found, leave the columns empty
                        for col in optinv_df.columns:
                            row[col] = None  # Set to None to indicate no match

                # If no match was found for any trip ID, ensure the row is still added but with empty columns
                if not matched_any_trip:
                    result_df = pd.concat([result_df, pd.DataFrame([row])], ignore_index=True)

            # # Remove duplicates based on the 'Trip' column if necessary
            # result_df = result_df.drop_duplicates(subset=['trip'])

            return result_df

        except Exception as e:
            print(f"Error matching trip details: {e}")
            return df_1
    @staticmethod
    def add_cn_number(df, start_cn_number=1000, column="CN Number"):
        """Add a unique Credit Note number in a sequence."""
        df[column] = range(start_cn_number, start_cn_number + len(df))
        return df

    @staticmethod
    def update_status_and_link(df, status_column="Status", link_column="Link", pdf_path_column="PDF Path"):
        """Update the status and add the hyperlink for the saved credit note PDF."""

        # Ensure column names are stripped of any extra spaces
        df.columns = df.columns.str.strip()

        for idx, row in df.iterrows():
            # Ensure the column exists and check the 'PDF Path'
            if pdf_path_column in df.columns and row[pdf_path_column] and row[pdf_path_column] != "N/A":
                df.at[idx, status_column] = "Saved"
                df.at[idx, link_column] = f'=HYPERLINK("{row[pdf_path_column]}", "Credit Note")'
            else:
                df.at[idx, status_column] = "Not Saved"
                df.at[idx, link_column] = "N/A"
        return df
