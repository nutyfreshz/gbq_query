import streamlit as st
import requests
import tempfile
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import pandas as pd
import json

# Streamlit app title
st.title("BigQuery Data Query with Multi-Column Conditions")

# Link to the gspread JSON file (service account key)
json_url = "https://drive.google.com/uc?export=download&id=1h36YKL7ZalJzEVnPF6AXZaV5ugriSYbs"

# Download the gspread JSON file automatically on app start
@st.cache_resource
def download_json():
    response = requests.get(json_url)
    if response.status_code == 200:
        # Save the file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
            temp_file.write(response.content)
            return temp_file.name
    else:
        raise Exception("Failed to download JSON. Please try again.")

# Download the JSON and authenticate using gspread
service_account_key_path = download_json()

# Define the scope for Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Authenticate using the service account
creds = ServiceAccountCredentials.from_json_keyfile_name(service_account_key_path, scope)
client = gspread.authorize(creds)

# Access Google Sheet with user and password
sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1PjIsU5dqFQf2avGP8coQB4fn_9z9qob-tP1NsjtWmHA/edit?gid=0")
worksheet = sheet.get_worksheet(0)
data = worksheet.get_all_records()

# Streamlit UI for login
st.subheader("Login to access BigQuery Data Query")

# Input for user and password
username = st.text_input("Username")
password = st.text_input("Password", type="password")

# Function to authenticate user using Google Sheets data
def authenticate_user(username, password):
    for record in data:
        if record["username"] == username and record["password"] == password:
            return True
    return False

# Authenticate if user provides credentials
if authenticate_user(username, password):
    st.success("Login successful!")
    
    # File uploader for .json file (for BigQuery)
    uploaded_file = st.file_uploader("Upload your service account .json file", type=["json"])

    if uploaded_file is not None:
        # Save the uploaded .json file temporarily
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(uploaded_file.read())
            service_account_key_path = temp_file.name

        # Set the environment variable for Google Cloud credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_key_path

        st.success("Service account JSON file uploaded successfully")

    # Initialize BigQuery client only if credentials are available
    if uploaded_file is not None:
        client = bigquery.Client()

        # Get a sample of the data (LIMIT 5 rows) to extract the columns
        sample_query = """
        SELECT *
        FROM `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_info`
        WHERE 1=1
        LIMIT 5
        """
        
        # Run the query to get the sample data
        sample_job = client.query(sample_query)
        sample_results = sample_job.result()

        # Convert the results to a DataFrame
        cols_df = pd.DataFrame([dict(row) for row in sample_results])

        # Extract the column names from the DataFrame
        columns_list = cols_df.columns.tolist()

        # Multi-column dropdown for user to select multiple columns
        selected_columns = st.multiselect("Select columns to add conditions:", columns_list)

        # Dictionary to store selected column and its corresponding value dropdown
        selected_values_dict = {}

        # For each selected column, display a corresponding dropdown for values
        for column in selected_columns:
            # Get distinct values for the selected column
            distinct_values_query = f"""
            SELECT DISTINCT {column}
            FROM `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_info`
            ORDER BY 1 ASC
            """
            distinct_values_job = client.query(distinct_values_query)
            distinct_values_results = distinct_values_job.result()
            distinct_values = [row[column] for row in distinct_values_results]

            # Dropdown to select a value for the column
            selected_values_dict[column] = st.selectbox(f"Select a value for {column}:", distinct_values)

        # Construct SQL query dynamically based on selected columns and values
        base_query = """
        SELECT *
        FROM `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_info`
        WHERE 1=1
        """
        
        # Build the conditions for the selected columns and values
        conditions = []
        for column, value in selected_values_dict.items():
            if value:  # Only add condition if a value is selected
                conditions.append(f"{column} = '{value}'")
        
        # If conditions are added, append them to the base query
        if conditions:
            full_query = base_query + " AND " + " AND ".join(conditions)
        else:
            full_query = base_query  # No conditions if none selected

        # Display the full SQL query
        st.write("Constructed SQL Query:")
        st.code(full_query)

        # Button to run the query
        if st.button("Run Query"):
            try:
                # Execute the query
                query_job = client.query(full_query)
                results = query_job.result()

                # Convert results to a DataFrame
                df = pd.DataFrame([dict(row) for row in results])
                
                # Display results in Streamlit
                st.write(df.head())
                
                # Button to download CSV
                if not df.empty:
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name="query_results.csv",
                        mime="text/csv"
                    )
                else:
                    st.warning("No results to download.")

            except Exception as e:
                st.error(f"Error executing query: {e}")
    else:
        st.warning("Please upload a Google Cloud service account .json file to proceed.")

else:
    st.warning("Invalid username or password. Please try again.")
