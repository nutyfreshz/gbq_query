import streamlit as st
from google.cloud import bigquery
import pandas as pd
import json
import tempfile
import os

import subprocess
import pandas_gbq
import json
from google.oauth2 import service_account
import time
from datetime import date
import numpy as np

# Streamlit app title
st.title("BigQuery Data Query with Streamlit")

# File uploader for .json file
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

    # Initial SQL query without any conditions
    base_query = """
    SELECT * 
    FROM `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_info`
    WHERE 1=1
    """

    # Get available columns from the table
    columns_query = """
    SELECT column_name
    FROM `cdg-mark-cust-prd.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = 'ca_ds_customer_info'
    ORDER BY column_name
    """
    columns_job = client.query(columns_query)
    columns_results = columns_job.result()
    columns_list = [row.column_name for row in columns_results]

    # Column dropdown for user to select
    selected_column = st.selectbox("Select a column to add a condition:", columns_list)

    # Get distinct values from the selected column
    distinct_values_query = f"""
    SELECT DISTINCT {selected_column}
    FROM `cdg-mark-cust-prd.CAS_DS_DATABASE.ca_ds_customer_info`
    ORDER BY 1 ASC
    """
    distinct_values_job = client.query(distinct_values_query)
    distinct_values_results = distinct_values_job.result()
    distinct_values = [row[selected_column] for row in distinct_values_results]

    # Dropdown for selecting value from the chosen column
    selected_value = st.selectbox(f"Select a value for {selected_column}:", distinct_values)

    # Dynamically build the WHERE clause if a value is selected
    if selected_value:
        condition = f"AND {selected_column} = '{selected_value}'"
        full_query = base_query + " " + condition
    else:
        full_query = base_query  # No conditions added

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
            st.write(df)
            
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
