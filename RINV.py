import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

def clean_rinv_to_invdb():
    # Authenticate and connect to Google Sheets
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    creds = ServiceAccountCredentials.from_json_keyfile_name('inv-cn-creation.json', scope)
    client = gspread.authorize(creds)

    # Open the sheet and get data
    spreadsheet = client.open('Inv/CN Creation')
    rinv_sheet = spreadsheet.worksheet('RINV')
    invdb_sheet = spreadsheet.worksheet('InvDB')

    # Get the raw data from the worksheet
    rinv_data = rinv_sheet.get_all_values()

    # Define the headers manually
    headers = [
        'Timestamp', 'Email Address', 'First Name', 'Last Name', 'Title/Position',
        'Which entity should generate the invoice?', 'Customer Name', 'Address Line 1',
        'City', 'Post Code/ZIP Code', 'Country', "Customer's Email Address",
        'Tax Status', 'Taxpayer Identification Number (TIN)', 'VAT ID',
        'Name of service / product', 'Service Period', 'Quantity', 'Price per quantity', 'Currency',
        'More than one service or products?', 'Name of service / product.1', 'Service Period.1',
        'Quantity.1', 'Price per quantity.1', 'Currency.1', 'Status'
    ]

    # Convert the raw data into a DataFrame with the manually defined headers
    # Convert the raw data into a DataFrame with the manually defined headers
    rinv_df = pd.DataFrame(rinv_data[1:], columns=headers)  # Skip the header row in the raw data

    # Combine First Name and Last Name into Requester Name
    rinv_df['Requester Name'] = rinv_df['First Name'] + ' ' + rinv_df['Last Name']

    # Combine City and Post Code/ZIP Code into City Postal
    rinv_df['City Postal'] = rinv_df['City'] + ' ' + rinv_df['Post Code/ZIP Code']

    # Prepare the main part of the DataFrame for InvDB
    base_columns = [
        'Timestamp', 'Email Address', 'Requester Name', 'Title/Position',
        'Which entity should generate the invoice?', 'Customer Name', 'Address Line 1',
        'City Postal', 'Country', "Customer's Email Address", 'Tax Status',
        'Taxpayer Identification Number (TIN)', 'VAT ID', 'Status'
    ]
    invdb_df = rinv_df[base_columns].copy()

    # Rename columns to match InvDB format
    invdb_df.rename(columns={
        'Which entity should generate the invoice?': 'Entity'
    }, inplace=True)

    # Handle Single Service Data
    single_service_df = rinv_df[rinv_df['More than one service or products?'] != 'Yes'][[
        'Timestamp', 'Name of service / product', 'Service Period', 'Quantity', 'Price per quantity', 'Currency' , 'Status'
    ]]

    # Handle Multi-Service Data
    multi_service_base = rinv_df[rinv_df['More than one service or products?'] == 'Yes']

    additional_products = multi_service_base[[
        'Timestamp', 'Name of service / product.1', 'Service Period.1', 'Quantity.1', 'Price per quantity.1', 'Currency.1'
    ]].rename(columns={
        'Name of service / product.1': 'Name of service / product',
        'Service Period.1': 'Service Period',
        'Quantity.1': 'Quantity',
        'Price per quantity.1': 'Price per quantity',
        'Currency.1': 'Currency'
    })

    # Combine Single and Multi-Service Data
    product_df = pd.concat([single_service_df, additional_products], ignore_index=True)

    # Rename product-related columns to match the desired output
    product_df.rename(columns={
        'Name of service / product': 'Product',
        'Price per quantity': 'Unit Price'
    }, inplace=True)

    # Merge product data with base invoice details
    final_df = pd.merge(
        invdb_df,
        product_df,
        on='Timestamp',
        how='outer'
    )

    # Write the cleaned data back to InvDB in Google Sheets
    invdb_sheet.clear()
    invdb_sheet.update([final_df.columns.values.tolist()] + final_df.fillna('').values.tolist())

# Run the function
clean_rinv_to_invdb()
