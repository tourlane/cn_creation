from google_sheet_processor import GoogleSheetUtils, DataFrameUtils
from dotenv import load_dotenv
import os
import pandas as pd
import re

# Load environment variables
load_dotenv()

# Retrieve the spreadsheet ID
spreadsheet_id = os.getenv("SPREADSHEET_ID")

gsheet_utils = GoogleSheetUtils()
dataframe_utils = DataFrameUtils()

# Load credentials and initialize service
credentials = gsheet_utils.load_credentials("inv-cn-creation.json")
service_api = gsheet_utils.build_service(credentials)

# Fetch data from "RICC" tab
db_cc = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB-CC", range_="A:AI")
db_cc_df = dataframe_utils.process_data_to_dataframe(db_cc)

# Fetch data from "SF-INFL" tab
sf_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "SF-INFL", range_="A2:F")

# Convert to DataFrame
sf_df = dataframe_utils.process_data_to_dataframe(sf_data)

# Check the column names after setting the header
print(f"SF-INFL columns after setting header: {sf_df.columns.tolist()}")

# Standardize column names for matching
sf_df.rename(columns={"Invoice: Trip Detail: Trip Confirmation: Trip": "trip_id"}, inplace=True)
sf_df.rename(columns={"Invoice: Trip Detail: Record Type": "type"}, inplace=True)



# Check the column names after renaming
print(f"SF-INFL columns after renaming: {sf_df.columns.tolist()}")

# Ensure trip_id normalization
db_cc_df["trip_id"] = db_cc_df["trip_id"].str.strip().str.lower()
sf_df["trip_id"] = sf_df["trip_id"].str.strip().str.lower()

# Merge SF-INFL data into DB-INFL
db_infl_combined = db_cc_df.merge(
    sf_df,
    on="trip_id",
    how="left",
    suffixes=("", "_sf")
)

# Expand rows for multiple "Invoice: Invoice No." values
def expand_invoice_rows(row):
    invoice_nos = str(row["Invoice: Invoice No."]).split(",")  # Split on commas
    if len(invoice_nos) == 1:
        return [row]
    expanded = []
    for invoice in invoice_nos:
        new_row = row.copy()
        new_row["Invoice: Invoice No."] = invoice.strip()
        expanded.append(new_row)
    return expanded

expanded_rows = []
for _, row in db_infl_combined.iterrows():
    expanded_rows.extend(expand_invoice_rows(row))

db_cc_df_expanded = pd.DataFrame(expanded_rows)

# Remove duplicates and fill missing values
db_cc_df_expanded.drop_duplicates(subset=["trip_id", "Invoice: Invoice No."], inplace=True)
db_cc_df_expanded.fillna("", inplace=True)

# Update "DB-INFL" with final data
gsheet_utils.update_sheet_with_dataframe(service_api, db_cc_df_expanded, spreadsheet_id, "DB-CC_2")
print("Updated DB-INFL with matched and expanded data.")
