import streamlit as st
import requests
import tempfile
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timezone, timedelta

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

# Tracker sheet link
action_tracker_sheet = client.open_by_url("https://docs.google.com/spreadsheets/d/1UJ89lPfLe2IDtRWOpukdfAzAwK7Cj0Zg2I8KTCDqybk/edit#gid=0")
tracker_worksheet = action_tracker_sheet.get_worksheet(0)

# Sidebar UI for login and JSON upload
with st.sidebar:
    st.subheader("Login to access BigQuery Data Query")

    # Input for user and password
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    # Button to trigger login authentication
    if st.button("Login"):
        if authenticate_user(username, password):
            st.success("Login successful!")
            # Log login action (optional)
            # log_user_action(username, "Login")  # Uncomment if you want to log login action
            
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
        else:
            st.warning("Invalid username or password. Please try again.")

# Main section: Ensure user login and JSON upload before using the app
if not authenticate_user(username, password):
    st.warning("Please login and upload the service account JSON file to proceed.")
else:
    if uploaded_file is None:
        st.warning("Please upload the service account JSON file to proceed.")
    else:
        # Initialize BigQuery client only if credentials are available
        client = bigquery.Client()

        # Get schema metadata for the target table
        def get_table_schema(project_id, dataset_id, table_id):
            table_ref = client.dataset(dataset_id).table(table_id)
            table = client.get_table(table_ref)
            return table.schema

        # Specify project, dataset, and table
        project_id = "cdg-mark-cust-prd"
        dataset_id = "CAS_DS_DATABASE"
        table_id = "ca_ds_customer_info"

        # Fetch table schema
        schema = get_table_schema(project_id, dataset_id, table_id)

        # Create a dictionary to hold column names and their data types
        column_type_dict = {field.name: field.field_type for field in schema}

        # Get a sample of the data (LIMIT 5 rows) to extract the columns
        sample_query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
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

        # Dictionary to store selected column and its corresponding values dropdown or range inputs
        selected_values_dict = {}

        # For each selected column, display a corresponding multiselect for values or range input for numerical columns
        for column in selected_columns:
            # Check if the column is numeric based on schema, and handle range input
            if column_type_dict.get(column) in ['INTEGER', 'FLOAT', 'NUMERIC', 'BIGNUMERIC']:
                # If column is numeric, provide range input
                min_val = st.number_input(f"Enter minimum value for {column}:", value=float(cols_df[column].min()), key=f"min_{column}")
                max_val = st.number_input(f"Enter maximum value for {column}:", value=float(cols_df[column].max()), key=f"max_{column}")
                selected_values_dict[column] = (min_val, max_val)
            else:
                # For non-numerical (categorical) columns, provide a dropdown
                distinct_values_query = f"""
                SELECT DISTINCT {column}
                FROM `{project_id}.{dataset_id}.{table_id}`
                ORDER BY 1 ASC
                """
                distinct_values_job = client.query(distinct_values_query)
                distinct_values_results = distinct_values_job.result()
                distinct_values = [row[column] for row in distinct_values_results]

                # Multiselect to select multiple values for the column
                selected_values_dict[column] = st.multiselect(f"Select values for {column}:", distinct_values)

        # Construct SQL query dynamically based on selected columns and values
        base_query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE 1=1
        """

        # Build the conditions for the selected columns and values
        conditions = []
        for column, values in selected_values_dict.items():
            if column_type_dict.get(column) in ['INTEGER', 'FLOAT', 'NUMERIC', 'BIGNUMERIC']:
                # Use range condition for numerical columns
                min_val, max_val = values
                conditions.append(f"{column} BETWEEN {min_val} AND {max_val}")
            else:
                # Use IN clause for categorical columns
                if values:  # Only add condition if values are selected
                    values_str = ", ".join([f"'{value}'" for value in values])
                    conditions.append(f"{column} IN ({values_str})")

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

                # Log the query execution action
                log_user_action(username, "Ran Query")  # Log "Ran Query" action

                # Convert results to a DataFrame
                df = pd.DataFrame([dict(row) for row in results])
                
                # Display results in Streamlit
                st.write(df.head())
                
                # Button to download CSV without tracking the download
                if not df.empty:
                    csv = df.to_csv(index=False)
                    st.download_button(
                        label="Download CSV",
                        data=csv,
                        file_name="query_results.csv",
                        mime="text/csv",
                        on_click=lambda: log_user_action(username, "Downloaded CSV")  # Log "Downloaded CSV" action
                    )
            except Exception as e:
                st.error(f"An error occurred: {e}")
