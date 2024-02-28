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

def clear_data_state():
    if 'data' in st.session_state.keys():
        del st.session_state['data']

with connect:
    "st.session_state object", st.session_state
    # Select data source type
    data_source = st.selectbox("Select data source type", options=["CSV", "SQL", "PARQUET"])

    # Read data
    if data_source == "CSV":
        file_path = st.text_input("CSV file path")
        file_sep = st.selectbox("Select CSV Separator", options=[',', '|', ';', 'other'])
        if file_sep == 'other':
            file_sep = st.text_input('CSV file separator')

        if st.button('Import data', on_click=clear_data_state()):
            if (file_path is not None) and (file_path != '') and (file_sep is not None) and (file_sep != ''):
                csv = read_csv(file_path, file_sep)
                if 'data' not in st.session_state:
                    st.session_state.data = csv[0]
                
                connection = csv[1]
                profiling_report = ProfileReport(st.session_state.data, minimal=True)
            else:
                st.error('Check file details!')
        
    if data_source == "PARQUET":
        parquet_folder = st.text_input("Parquet file path")

        if st.button('Import data'):
            if parquet_folder is not None and parquet_folder != '':
                parquet = read_parquet(parquet_folder)
                data = parquet[0]
                connection = parquet[1]
                profiling_report = ProfileReport(data, minimal=True)
            else:
                st.error('Check file details!')

    if data_source == "SQL":
        host = st.text_input("Host")
        port = st.number_input("Port", value=0, step=1)

        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database")
        table = st.text_input("Table")

        if st.button('Import data'):
            if host and port and username and password and database and table:
                sql = read_sql(host, port, username, password, database, table)
                data = sql[0]
                connection = sql[1]
                profiling_report = ProfileReport(data, minimal=True)

            else:
                st.error('Check file details!')

    # elif data_source == "MongoDB":
    #     host = st.text_input("Host")
    #     port = st.number_input("Port", value=0, step=1)

    #     database = st.text_input("Database")
    #     collection = st.text_input("Collection")
    #     if host and port and database and collection:
    #         mongo = read_mongo(host, port, database, collection) 
    #         data = mongo[0]
    #         connection = mongo[1]
    #         profiling_report = ProfileReport(data, minimal=True)

    if "data" in st.session_state:
        st.success('Database imported successfully!')
        with st.expander('Check database imported'):
            st.text('Showing top 10 lines:')
            st.dataframe(st.session_state.data.head(10))

with rules:
    # Select column to apply expectations
    if "data" not in st.session_state:
        st.warning('Select database!')

    else:
        "st.session_state object", st.session_state

        col_options = st.session_state.data.columns.tolist()
        selected_col = st.selectbox("Select column to apply expectations", options=col_options)
        
        with st.form(key="register_rules", clear_on_submit= True):
            with st.expander('Completude'):
            # Select expectations to apply
                st.write("Select expectations to apply to the column:")
                completude_rules = ["Não pode haver nulos"]

                completude_selected = st.multiselect("Expectations", options=completude_rules, default=None)

            with st.expander('Unicidade'):
            # Select expectations to apply
                st.write("Select expectations to apply to the column:")
                unicidade_rules = ["Não pode haver duplicatas"] # expect_column_values_to_be_unique

                unicidade_selected = st.multiselect("Expectations", options=unicidade_rules, default=None)

            with st.expander('Acurácia'):
            # Select expectations to apply
                st.write("Select expectations to apply to the column:")
                acuracia_rules = ["Data Válida", "Email Válido"] # expect_column_values_to_be_dateutil_parseable  | expect_column_values_to_contain_valid_email 

                acuracia_selected = st.multiselect("Expectations", options=acuracia_rules, default=None)

            with st.expander('Formato'):
            # Select expectations to apply
                st.write("Select expectations to apply to the column:")
                formato_rules = ["Data com Formato Válido", "E-mail com Formato Válido"] # expect_column_values_to_match_strftime_format | expect_column_values_to_match_regex

                formato_selected = st.multiselect("Expectations", options=formato_rules, default=None)

            # selected_expectations = completude_selected + unicidade_selected + acuracia_selected + formato_selected

                # Create expectations dictionary
            expectations = {
                "file_path": connection,
                "date_time": datetime.now(),
                "column_name": selected_col,
                "expectations": {
                    'completude': completude_selected,
                    'unicidade': unicidade_selected,
                    'acuracia': acuracia_selected,
                    'formato': formato_selected,
                }
            }

            # Register expectations
            if st.form_submit_button("Register expectations"):
                register_expectations_mongo(HOST, PORT, DB, COL, expectations)
                st.success("Expectations registered successfully!")

with results:
    if "data"  not in st.session_state:
        st.warning('Select database!')

    else:
        if st.button('Generate Profiling'):
            st_profile_report(profiling_report)