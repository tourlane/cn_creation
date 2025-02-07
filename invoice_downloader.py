from simple_salesforce import Salesforce
import requests
import os

# Salesforce credentials
SF_USERNAME = "your_username"
SF_PASSWORD = "your_password"
SF_SECURITY_TOKEN = "your_security_token"
SF_DOMAIN = "your_domain"

# Connect to Salesforce
sf = Salesforce(username=SF_USERNAME, password=SF_PASSWORD, security_token=SF_SECURITY_TOKEN, domain=SF_DOMAIN)

def download_invoices(output_folder="invoices"):
    """Download the first PDF invoice from Salesforce where Payment Method is 'Influencer Invoice' or 'Marketing'."""
    os.makedirs(output_folder, exist_ok=True)

    # Query invoices with required Payment Methods
    query = """
    SELECT Id, Name, (SELECT Id, Name, ContentType, Body FROM Attachments ORDER BY CreatedDate ASC)
    FROM Invoice__c
    WHERE Payment_Method__c IN ('Influencer Invoice', 'Marketing')
    """
    invoices = sf.query_all(query)['records']

    for invoice in invoices:
        invoice_id = invoice['Id']
        invoice_name = invoice['Name']
        attachments = invoice.get('Attachments', {}).get('records', [])

        if attachments:
            first_attachment = attachments[0]
            attachment_id = first_attachment['Id']
            attachment_name = first_attachment['Name']
            content_type = first_attachment['ContentType']

            # Check if it's a PDF
            if "pdf" in content_type.lower():
                file_path = os.path.join(output_folder, f"{invoice_name}_{attachment_name}")
                pdf_url = f"{sf.base_url}/sobjects/Attachment/{attachment_id}/Body"

                # Download the file
                response = requests.get(pdf_url, headers={"Authorization": f"Bearer {sf.session_id}"}, stream=True)
                if response.status_code == 200:
                    with open(file_path, "wb") as pdf_file:
                        pdf_file.write(response.content)
                    print(f"Downloaded: {file_path}")
                else:
                    print(f"Failed to download {attachment_name} (Status Code: {response.status_code})")
        else:
            print(f"No attachments found for Invoice {invoice_name}")

# Run function
download_invoices()
