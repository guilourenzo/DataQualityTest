import streamlit as st
import pandas as pd
import pymongo
import sqlite3

# Function to read data from a CSV file
def read_csv(file_path):
    return pd.read_csv(file_path)

# Function to read data from a SQL database
def read_sql(host, port, username, password, database, table):
    conn = sqlite3.connect(f"sqlite://{host}:{port}/{database}")
    query = f"SELECT * FROM {table}"
    return pd.read_sql(query, conn)

# Function to read data from a MongoDB database
def read_mongo(host, port, database, collection):
    client = pymongo.MongoClient(host, port)
    db = client[database]
    cursor = db[collection].find()
    return pd.DataFrame(list(cursor))

# Function to register expectations in MongoDB
def register_expectations_mongo(host, port, database, collection, expectations):
    client = pymongo.MongoClient(host, port)
    db = client[database]
    db[collection].insert_one(expectations)
    st.success("Expectations registered successfully!")

# UI layout and inputs
st.title("Data Quality Expectations")

# Select data source type
data_source = st.selectbox("Select data source type", options=["CSV", "SQL", "MongoDB"])

# Read data
if data_source == "CSV":
    file_path = st.file_uploader("Upload CSV file")
    if file_path is not None:
        data = read_csv(file_path)
else:
    host = st.text_input("Host")
    port = st.number_input("Port", value=0, step=1)
    if data_source == "SQL":
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        database = st.text_input("Database")
        table = st.text_input("Table")
        if host and port and username and password and database and table:
            data = read_sql(host, port, username, password, database, table)
    elif data_source == "MongoDB":
        database = st.text_input("Database")
        collection = st.text_input("Collection")
        if host and port and database and collection:
            data = read_mongo(host, port, database, collection)

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
        "column_name": selected_col,
        "expectations": selected_expectations
    }

    # Register expectations
    if st.button("Register expectations"):
        register_expectations_mongo(host, port, database, collection, expectations)
