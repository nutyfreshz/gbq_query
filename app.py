import streamlit as st
import requests
import tempfile
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Streamlit app title
st.title("BigQuery Data Query with Multi-Column Conditions")

# Link to the JSON file
json_url = "https://drive.google.com/uc?export=download&id=1h36YKL7ZalJzEVnPF6AXZaV5ugriSYbs"

# Download button for gspread JSON
if st.button("Download gspread JSON file"):
    response = requests.get(json_url)
    if response.status_code == 200:
        # Save the file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            temp_file.write(response.content)
            service_account_key_path = temp_file.name
        st.success("gspread JSON downloaded successfully!")
    else:
        st.error("Failed to download JSON. Please try again.")

# Use the downloaded JSON file for gspread authentication
if 'service_account_key_path' in locals():
    # Define the scope for Google Sheets API
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    # Authenticate using the service account
    creds = ServiceAccountCredentials.from_json_keyfile_name(service_account_key_path, scope)
    client = gspread.authorize(creds)

    # Now you can access Google Sheets
    sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1PjIsU5dqFQf2avGP8coQB4fn_9z9qob-tP1NsjtWmHA/edit?gid=0")
    worksheet = sheet.get_worksheet(0)
    data = worksheet.get_all_records()

    st.write(data)  # Display data from the sheet
