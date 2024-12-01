import pandas as pd
from google.oauth2 import service_account
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import os
from dotenv import load_dotenv


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
    def update_sheet_with_dataframe(service, df, spreadsheet_id, tab_name, start_cell="A2", include_index=False):
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
        """Handle trip IDs by splitting comma-separated values into individual rows."""
        expanded_rows = []

        for _, row in df.iterrows():
            trip_ids = str(row[trip_column]).split(',')  # Split trip IDs by commas
            trip_ids = [trip_id.strip() for trip_id in trip_ids]  # Remove spaces

            # Create a new row for each trip ID, keeping other data the same
            for trip_id in trip_ids:
                new_row = row.copy()
                new_row[trip_column] = trip_id  # Replace with the new trip ID
                expanded_rows.append(new_row)

        # Convert the expanded rows back into a DataFrame
        expanded_df = pd.DataFrame(expanded_rows)
        return expanded_df

    @staticmethod
    def match_trip_details(df_1, optinv_df, trip_column):
        """Match trip details from another DataFrame based on a trip column."""
        try:
            # Strip whitespaces from column names for both DataFrames
            df_1.columns = df_1.columns.str.strip()
            optinv_df.columns = optinv_df.columns.str.strip()

            # Print column names for debugging
            print(f"df_1 columns: {df_1.columns}")
            print(f"optinv_df columns: {optinv_df.columns}")

            # Ensure column names are consistent
            df_1.columns = df_1.columns.str.lower()
            optinv_df.columns = optinv_df.columns.str.lower()

            # Ensure that 'trip id' in df_1 and 'trip' in optinv_df are present
            if "trip id" not in df_1.columns or "trip" not in optinv_df.columns:
                raise KeyError('The "Trip ID" column was not found in df_1 or "Trip" was not found in optinv_df.')

            # Iterate over df_1 rows to match trip details
            for idx, row in df_1.iterrows():
                trip_id = row[trip_column]  # This corresponds to "Trip ID" in df_1

                # Skip matching if trip_id is null
                if pd.isnull(trip_id):
                    continue

                trip_id = str(trip_id).strip()  # Ensure trip_id is a string

                # Find the matched invoices from optinv_df
                matched_invoices = optinv_df[optinv_df["trip"] == trip_id]

                # If matched invoices exist, merge all columns from optinv_df to df_1
                if not matched_invoices.empty:
                    # We will add all matched columns from optinv_df to df_1
                    for col in matched_invoices.columns:
                        # If the column exists in df_1, we overwrite, otherwise, we add it
                        df_1.at[idx, col] = matched_invoices.iloc[0][col]

            return df_1

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
