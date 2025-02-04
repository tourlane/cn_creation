from google_sheet_processor import GoogleSheetUtils, DataFrameUtils
from dotenv import load_dotenv
import os
import pandas as pd

# Load environment variables
load_dotenv()
spreadsheet_id = os.getenv("SPREADSHEET_ID")

gsheet_utils = GoogleSheetUtils()
dataframe_utils = DataFrameUtils()

# Load service account credentials and initialize Sheets API
credentials = gsheet_utils.load_credentials("inv-cn-creation.json")
service_api = gsheet_utils.build_service(credentials)

### **Step 1: Fetch Data from Performance Tab** ###
performance_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "Performance", range_="B4:I")
df_performance = dataframe_utils.process_data_to_dataframe(performance_data)

# Standardize column names
# df_performance.columns = df_performance.iloc[3]  # Set row 4 as header
# df_performance = df_performance[4:].reset_index(drop=True)  # Remove header row
df_performance.columns = df_performance.columns.str.strip().str.lower().str.replace(" ", "_")

### **Step 2: Fetch Data from Opportunities ID + Invoice ID Tab** ###
opportunity_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "Opportunties ID + Invoice ID", range_="A2:R")
df_opportunities = dataframe_utils.process_data_to_dataframe(opportunity_data)

# Standardize column names
# df_opportunities.columns = df_opportunities.iloc[0]  # Set first row as header
# df_opportunities = df_opportunities[1:].reset_index(drop=True)
df_opportunities.columns = df_opportunities.columns.str.strip().str.lower().str.replace(" ", "_")

### **Step 3: Merge Performance Data with Opportunities Data** ###
df_combined = df_performance.merge(
    df_opportunities,
    on="opportunity_id",
    how="left"
)

### **Step 4: Expand Rows for Multiple Invoice Numbers per Opportunity ID** ###
if 'invoice_no' in df_opportunities.columns:
    df_expanded = df_opportunities.explode("invoice_no")
    df_combined = df_performance.merge(
        df_expanded,
        on="opportunity_id",
        how="left"
    )

### **Step 5: Save the Merged Data to DB Tab** ###
df_combined.fillna("", inplace=True)  # Replace NaNs with empty strings
gsheet_utils.update_sheet_with_dataframe(service_api, df_combined, spreadsheet_id, "DB")

print("Updated 'DB' tab successfully.")


# Fetch data from "RITP" tab
ritp_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "RITP", range_="A:Z")
df_ritp = dataframe_utils.process_data_to_dataframe(ritp_data)

# Standardize column names
df_ritp.columns = df_ritp.columns.str.strip().str.lower().str.replace(" ", "_")

# Select the required columns from RITP
columns_needed = [
    'email_address', 'location', 'address_line_1', 'city', 'post_code/zip_code', 'country',
    'file_of_contract', 'signed_date', 'tax_status', 'taxpayer_identification_number_(tin)',
    'vat_id', 'iban', 'bic', 'account_number', 'swift', 'sales_agent'
]

df_ritp_filtered = df_ritp[columns_needed].copy()

# Fetch current data from "DB"
db_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "DB", range_="A:Y")
df_db = dataframe_utils.process_data_to_dataframe(db_data)

# Standardize column names in DB as well
df_db.columns = df_db.columns.str.strip().str.lower().str.replace(" ", "_")

# Merge based on 'email_address', ensuring we don’t overwrite existing values in DB
df_db_updated = df_db.merge(
    df_ritp_filtered,
    on="sales_agent",
    how="left",
)

# Fill missing values in DB with values from RITP (only if the field was empty)
for col in columns_needed[1:]:  # Exclude 'email_address' since it's the key
    df_db_updated.fillna("", inplace=True)  # Replace NaN with an empty string


# Drop the extra columns created by merging
df_db_updated.drop(columns=[f"{col}_ritp" for col in columns_needed[1:] if f"{col}_ritp" in df_db_updated], inplace=True)

# Upload the updated data back to Google Sheets
gsheet_utils.update_sheet_with_dataframe(service_api, df_db_updated, spreadsheet_id, "DB")
print("Updated the DB tab successfully with RITP data.")
