import streamlit as st
import pandas as pd
from streamlit_pandas_profiling import st_profile_report
from pandas_profiling import ProfileReport
import pymongo
import sqlite3
from configparser import ConfigParser
from utils.utils import *
from datetime import datetime

st.set_page_config(
    page_title="Serasa - DQ",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={
        'Get Help': 'mailto:guilherme.lourenco@br.experian.com',
        'Report a bug': "mailto:guilherme.lourenco@br.experian.com",
        'About': "# Serasa Data Quality ✅: Register Data Quality Rules and review Data Profilling"
    }
)


CONFIG = ConfigParser()
CONFIG.read('../config/dq.ini')
MONGO_SERVER = CONFIG["MONGO_SERVER"]

HOST = MONGO_SERVER['MONGO_HOST']
PORT = int(MONGO_SERVER['MONGO_PORT'])
DB = MONGO_SERVER['MONGO_DB']
COL = MONGO_SERVER['MONGO_COLLECTIONS']
# data = None

# UI layout and inputs
st.title("Data Quality Expectations")

connect, rules, results = st.tabs(['Conexão', 'Regras', 'Resultados'])

with connect:
# Select data source type
    data_source = st.selectbox("Select data source type", options=["CSV", "SQL", "PARQUET"])

    # Read data
    if data_source == "CSV":
        file_path = st.file_uploader("Upload CSV file")
        if file_path is not None:
            csv = read_csv(file_path)
            data = csv[0]
            connection = csv[1]
            profiling_report = ProfileReport(data)
    
    if data_source == "PARQUET":
        parquet_folder = st.text_input("File Path")

        if parquet_folder is not None:
            parquet = read_parquet(parquet_folder)
            data = parquet[0]
            connection = parquet[1]
            profiling_report = ProfileReport(data)

    if data_source == "SQL":
        host = st.text_input("Host")
        port = st.number_input("Port", value=0, step=1)

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database")
        table = st.text_input("Table")
        if host and port and username and password and database and table:
            sql = read_sql(host, port, username, password, database, table)
            data = sql[0]
            connection = sql[1]
            profiling_report = ProfileReport(data)

    elif data_source == "MongoDB":
        host = st.text_input("Host")
        port = st.number_input("Port", value=0, step=1)

        database = st.text_input("Database")
        collection = st.text_input("Collection")
        if host and port and database and collection:
            mongo = read_mongo(host, port, database, collection) 
            data = mongo[0]
            connection = mongo[1]
            profiling_report = ProfileReport(data)

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

    if 'data' in locals():
    #     # profiling_report = data.profile_report()
        st_profile_report(profiling_report)