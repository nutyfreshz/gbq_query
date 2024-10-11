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

    # SQL query input
    query = st.text_area("Enter your BigQuery SQL query")

    # Button to run query
    if st.button("Run Query"):
        if query:
            try:
                # Execute the query
                query_job = client.query(query)
                results = query_job.result()

                # Convert results to a DataFrame
                df = pd.DataFrame([dict(row) for row in results])
                
                # Display results in Streamlit
                st.write(df)
            except Exception as e:
                st.error(f"Error executing query: {e}")
        else:
            st.error("Please enter a SQL query.")
else:
    st.warning("Please upload a Google Cloud service account .json file to proceed.")
