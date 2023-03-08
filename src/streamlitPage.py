import streamlit as st
import pandas as pd
import pymongo
import sqlite3
from configparser import ConfigParser
from utils.utils import *
from datetime import datetime

CONFIG = ConfigParser()
CONFIG.read('../config/dq.ini')
MONGO_SERVER = CONFIG["MONGO_SERVER"]

HOST = MONGO_SERVER['MONGO_HOST']
PORT = int(MONGO_SERVER['MONGO_PORT'])
DB = MONGO_SERVER['MONGO_DB']
COL = MONGO_SERVER['MONGO_COLLECTIONS']

# UI layout and inputs
st.title("Data Quality Expectations")

connect, rules, results = st.tabs(['Conexão', 'Regras', 'Resultados'])

with connect:
# Select data source type
    data_source = st.selectbox("Select data source type", options=["CSV", "SQL", "MongoDB", "Parquet"])

    # Read data
    if data_source == "CSV":
        file_path = st.file_uploader("Upload CSV file")
        if file_path is not None:
            csv = read_csv(file_path)
            data = csv[0]
            connection = csv[1]
    
    elif data_source == 'Parquet':
        parquet_path = st.text_input("URL to Parquet")

        if st.button("Import Parquet files"):
            parquet = read_parquet(parquet_path)
            data = parquet[0]
            connection = parquet[1]
            print(connection)
    
    else:
        host = st.text_input("Host")
        port = st.number_input("Port", value=0, step=1)

        if data_source == "SQL":
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            database = st.text_input("Database")
            table = st.text_input("Table")
            if host and port and username and password and database and table:
                sql = read_sql(host, port, username, password, database, table)
                data = sql[0]
                connection = sql[1]

        elif data_source == "MongoDB":
            database = st.text_input("Database")
            collection = st.text_input("Collection")
            if host and port and database and collection:
                mongo = read_mongo(host, port, database, collection) 
                data = mongo[0]
                connection = mongo[1]

with rules:
    # Select column to apply expectations
    if "data" in locals():
        col_options = data.columns.tolist()
        selected_col = st.selectbox("Select column to apply expectations", options=col_options)

        # Select expectations to apply
        st.write("Select expectations to apply to the column:")
        expectation_options = ["expect_column_to_exist", "expect_column_values_to_not_be_null", "expect_column_values_to_be_in_set"]
        selected_expectations = st.multiselect("Expectations", options=expectation_options)

        # Create expectations dictionary
        expectations = {
            "file_path": connection,
            "date_time": datetime.now(),
            "column_name": selected_col,
            "expectations": selected_expectations
        }

        # Register expectations
        if st.button("Register expectations"):
            register_expectations_mongo(HOST, PORT, DB, COL, expectations)
            st.success("Expectations registered successfully!")

with results:
    st.header('EM DESENVOLVIMENTO')