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
performance_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "Performance", range_="A4:I")
df_performance = dataframe_utils.process_data_to_dataframe(performance_data)

# Standardize column names
df_performance.columns = df_performance.columns.str.strip().str.lower().str.replace(" ", "_")

# Fetch data from "RITP" tab
ritp_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "RITP", range_="A:AA")
df_ritp = dataframe_utils.process_data_to_dataframe(ritp_data)

# Standardize column names
df_ritp.columns = df_ritp.columns.str.strip().str.lower().str.replace(" ", "_")

# Ensure 'agent_code' exists in both DataFrames before merging
if 'agent_code' not in df_performance.columns or 'agent_code' not in df_ritp.columns:
    raise ValueError("Missing 'agent_code' column in either Performance or RITP tab.")

# Select required columns from RITP
columns_needed = [
    'email_address', 'location', 'address_line_1', 'city', 'post_code/zip_code', 'country',
    'file_of_contract', 'signed_date', 'tax_status', 'taxpayer_identification_number_(tin)',
    'vat_id', 'iban', 'bic', 'account_number', 'swift', 'sales_agent', 'agent_code'
]

df_ritp_filtered = df_ritp[columns_needed].copy()

# Merge based on 'agent_code'
df_db_updated = df_performance.merge(df_ritp_filtered, on="agent_code", how="left")

### **Step 2: Fetch "Trip" Column from "Opportunities ID + Invoice ID" Tab** ###
opportunities_data = gsheet_utils.fetch_sheet_data(service_api, spreadsheet_id, "Opportunties ID + Invoice ID", range_="A2:R")
df_opportunities = dataframe_utils.process_data_to_dataframe(opportunities_data)

# Standardize column names
df_opportunities.columns = df_opportunities.columns.str.strip().str.lower().str.replace(" ", "_")

# Ensure required columns exist
if "opportunity_id" not in df_db_updated.columns or "opportunity_id" not in df_opportunities.columns:
    raise ValueError("Missing 'Opportunity ID' column in either DB or Opportunities tab.")

# Select only the "Trip" column and drop duplicates
df_opportunities_filtered = df_opportunities[["opportunity_id", "trip"]].drop_duplicates(subset=["opportunity_id"], keep="first")

# Merge based on 'opportunity_id'
df_db_updated = df_db_updated.merge(df_opportunities_filtered, on="opportunity_id", how="left")

# Replace NaN values with empty strings
df_db_updated.fillna("", inplace=True)

# Upload the updated data back to Google Sheets
gsheet_utils.update_sheet_with_dataframe(service_api, df_db_updated, spreadsheet_id, "DB")
print("Updated the DB tab successfully with RITP and Opportunities data.")
