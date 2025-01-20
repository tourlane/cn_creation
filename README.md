# Credit Note Creation with Google Sheet Processor


This project automates data processing and credit note creation in Google Sheets. It leverages Python, Google Sheets API, and Pandas to handle operations such as fetching data, applying templates, updating cells, and calculating taxes dynamically based on input data.

## Features

- Fetches data from Google Sheets and processes it into Pandas DataFrames.
- Filters and groups data for streamlined workflows.
- Dynamically updates Google Sheets using templates.
- Supports credit note creation with auto-incrementing numbers.
- Handles tax calculations based on country and net total.

## Prerequisites

Before running this project, ensure you have the following:

- Python 3.8 or later
- Google Cloud service account JSON file with access to the Google Sheets API
- A `.env` file containing your `SPREADSHEET_ID`
- Installed Python dependencies (see [Installation](#installation))

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/tourlane/cn_creation.git
   cd cn_creation







