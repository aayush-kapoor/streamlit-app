import re
import os
import json
import datetime
import sqlalchemy
import numpy as np
import pandas as pd
import streamlit as st
import streamlit_elements
import matplotlib.pyplot as plt
from snowflake.sqlalchemy import URL

marico_df = pd.read_excel("maricoData.xlsx")
gmap_df = pd.read_excel("gmap_data.xlsx")

sql_credential = {
            "salespoc":{
                "username":"salesuser",
                "password":"salesuser123",
                "host":"10.121.26.11",
                "database":"SalesReport_Disk"
            },
            "snowflake_prod":{
                "username":"prod_ds",
                "password":"Ds@123",
                "company":"marico-analytics",
                "warehouse":"PRD_DATASCIENCE_REPORTING_WHS",
                "db":"PRD_DB",
                "schema":"PUBLIC",
                "role":"SYSADMIN"
            },
            "snowflake":{
                "username":"dev_ds",
                "password":"Devds@2022",
                "company":"marico-analytics",
                "warehouse":"PRD_BI_COMPUTE_WHS",
                "db":"DEV_DB",
                "schema":"DATA_SCIENCE",
                "role":"DEV_DATA_SCIENTIST"

            }
        }

conn = sqlalchemy.create_engine(URL(
        account=sql_credential['snowflake']['company'],
        user=sql_credential['snowflake']['username'],
        password=sql_credential['snowflake']['password'],
        database=sql_credential['snowflake']['db'],
        schema=sql_credential['snowflake']['schema'],
        warehouse=sql_credential['snowflake']['warehouse']
    ))

if 'input_df' not in st.session_state:
    st.session_state.input_df = pd.read_excel("InputMaster.xlsx")
    query = "select * from ds_gmap_marico_retailer_input"
    # st.session_state.input_df = pd.DataFrame(pd.read_sql(con=conn, sql=query))  ######## Remember to uncomment this line later on in development
    # st.session_state.input_df.rename(columns={
    #                                         "retailercode":'RetailerCode',
    #                                         "place_id":'place_id',
    #                                         "match_percent":'match_percent'}, inplace=True)
    
    # st.session_state.input_df['match_percent'] = st.session_state.input_df['match_percent'].astype(int)

row, col = st.session_state.input_df.shape

st.set_page_config(layout="wide")

if 'i' not in st.session_state:
    st.session_state.i = 0

if 'matching_df' not in st.session_state:
    st.session_state.matching_df = pd.DataFrame(columns=["RetailerName", "name", "Address", "vicinity", "RetailerCode", "place_id", "match_percent"])
    st.session_state.matching_df = st.session_state.matching_df.astype(str)

if 'not_matching_df' not in st.session_state:
    st.session_state.not_matching_df = pd.DataFrame(columns=["RetailerName", "name", "Address", "vicinity", "RetailerCode", "place_id", "match_percent"])
    st.session_state.not_matching_df = st.session_state.not_matching_df.astype(str)

if 'maybe_df' not in st.session_state:
    st.session_state.maybe_df = pd.DataFrame(columns=["RetailerName", "name", "Address", "vicinity", "RetailerCode", "place_id", "match_percent"])
    st.session_state.maybe_df = st.session_state.maybe_df.astype(str)

placeholder = st.empty()
placeholder2 = st.empty()   

def display_tables(marico_i, gmap_i, color, value):

    table_header = """
    <table style="width: 100%">
    <tr>
        <th style="width: 20%; text-align: center; font-size: 20px"><u>Attributes</u></th>
        <th style="width: 40%; text-align: center; font-size: 20px"><u>Marico</u></th>
        <th style="width: 40%; text-align: center; font-size: 20px"><u>3rd Party</u></th>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Retailer Name: </b></td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{1}</td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{6}</td>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Retailer Address: </b></td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{2}</td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{7}</td>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Retailer Pincode: </b></td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{3}</td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{8}</td>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Retailer ID: </b></td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{4}</td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{9}</td>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Retailer Phone Number: </b></td>
        <td style="width: 40%; vertical-align: middle; text-align: center; color: {0}">{5}</td>
        <td style="width: 40%; vertical-align: middle; text-align: center;"></td>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Retailer Images: </b></td>
        <td style="width: 40%; vertical-align: middle; text-align: center;">
            <div class="image-container">
                <div class="image-wrapper">
                    <a href="https://picsum.photos/200" target="_blank"><img class="image-1" src="https://picsum.photos/200"></a>
                    <a href="https://picsum.photos/200" target="_blank"><img class="image-2" src="https://picsum.photos/200"></a>
                </div>
            </div>
        </td>
        <td style="width: 40%; vertical-align: middle; text-align: center;">
            <div class="image-container">
                <div class="image-wrapper">
                    <a href="https://picsum.photos/200" target="_blank"><img class="image-3" src="https://picsum.photos/200"></a>
                    <a href="https://picsum.photos/200" target="_blank"><img class="image-4" src="https://picsum.photos/200"></a>
                </div>
            </div>
        </td>
    </tr>
    <tr>
        <td style="width: 20%; vertical-align: middle; text-align: end"><b>Percent Match: </b></td>
        <td colspan="2" style="width: 80%; vertical-align: middle; text-align: center; color: {0}; font-size: 20px"><b>{10}% match</b></td>
    </tr>
    </table>
    """.format(color, 
               marico_df['RetailerName'].iloc[marico_i], 
               marico_df['Address'].iloc[marico_i], 
               marico_df['Pincode'].iloc[marico_i],
               marico_df['RetailerCode'].iloc[marico_i],
               marico_df['PrimaryMobileNo'].iloc[marico_i],
               gmap_df['name'].iloc[gmap_i],
               gmap_df['vicinity'].iloc[gmap_i],
               gmap_df['plus_code.compound_code'].iloc[gmap_i],
               gmap_df['plus_code.global_code'].iloc[gmap_i],
               value
               )

    # Display the table using st.markdown
    placeholder.markdown(table_header, unsafe_allow_html=True)
    streamlit_css = """
        <style>
            .image-1 {
                transition: transform 0.3s;
            }

            .image-1:hover {
                transform: scale(2);
            }
            .image-2 {
                transition: transform 0.3s;
            }

            .image-2:hover {
                transform: scale(2);
            }
            .image-3 {
                transition: transform 0.3s;
            }

            .image-3:hover {
                transform: scale(2);
            }
            .image-4 {
                transition: transform 0.3s;
            }

            .image-4:hover {
                transform: scale(2);
            }
        </style>
    """
    st.markdown(streamlit_css, unsafe_allow_html=True)
    hide_streamlit_style = """
            <style>
                footer {visibility: hidden;}
            </style>
            """
    st.markdown(hide_streamlit_style, unsafe_allow_html=True)

if st.session_state.i == 0:
    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    
    color = ""
    if value <= 25:
        color = "red"
    elif value > 25 and value <= 50:
        color = "orange"
    elif value > 50 and value <= 75:
        color = "#9ac451"
    elif value > 75:
        color = "green"
    
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]

    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    display_tables(marico_index, gmap_index, color, value)
    
def onMatch():

    global row
    global col
    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]
    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    if str(marico_ret_code) in list(st.session_state.matching_df['RetailerCode']):
        pass
    else:
        st.session_state.matching_df = st.session_state.matching_df.append(pd.concat([marico_df[['RetailerName','Address']].iloc[marico_index], gmap_df[['name','vicinity']].iloc[gmap_index], st.session_state.input_df.iloc[st.session_state.i]]), ignore_index=True)
        st.session_state.input_df = st.session_state.input_df[~(st.session_state.input_df['RetailerCode'] == marico_ret_code)]
        row, col = st.session_state.input_df.shape

    st.session_state.not_matching_df = st.session_state.not_matching_df[~(st.session_state.not_matching_df['RetailerCode'] == marico_ret_code)]
    st.session_state.maybe_df = st.session_state.maybe_df[~(st.session_state.maybe_df['RetailerCode'] == marico_ret_code)]
    
    # st.text("Matching DF")
    # st.dataframe(st.session_state.matching_df)
    # st.text("Not Matching DF")
    # st.dataframe(st.session_state.not_matching_df)
    # st.text("Maybe DF")
    # st.dataframe(st.session_state.maybe_df)

    # <--------- Increase the 'i' value to go to the next row --------->
    st.session_state.i += 1

    if st.session_state.i >= (row-1):
        st.session_state.i = (st.session_state.i % row)

    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    
    color = ""
    if value <= 25:
        color = "red"
    elif value > 25 and value <= 50:
        color = "orange"
    elif value > 50 and value <= 75:
        color = "#9ac451"
    elif value > 75:
        color = "green"
    
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]

    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    display_tables(marico_index, gmap_index, color, value)


def onNoMatch():

    global row
    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]
    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    st.session_state.not_matching_df = st.session_state.not_matching_df.append(pd.concat([marico_df[['RetailerName','Address']].iloc[marico_index], gmap_df[['name','vicinity']].iloc[gmap_index], st.session_state.input_df.iloc[st.session_state.i]]), ignore_index=True)
    
    # st.text("Matching DF")
    # st.dataframe(st.session_state.matching_df)
    # st.text("Not Matching DF")
    # st.dataframe(st.session_state.not_matching_df)
    # st.text("Maybe DF")
    # st.dataframe(st.session_state.maybe_df)

    # <--------- Increase the 'i' value to go to the next row --------->
    st.session_state.i += 1

    if st.session_state.i >= (row-1):
        st.session_state.i = (st.session_state.i % row)

    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    
    color = ""
    if value <= 25:
        color = "red"
    elif value > 25 and value <= 50:
        color = "orange"
    elif value > 50 and value <= 75:
        color = "#9ac451"
    elif value > 75:
        color = "green"
    
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]

    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    display_tables(marico_index, gmap_index, color, value)

def onMaybe():

    global row
    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]
    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    st.session_state.maybe_df = st.session_state.maybe_df.append(pd.concat([marico_df[['RetailerName','Address']].iloc[marico_index], gmap_df[['name','vicinity']].iloc[gmap_index], st.session_state.input_df.iloc[st.session_state.i]]), ignore_index=True)
    
    # st.text("Matching DF")
    # st.dataframe(st.session_state.matching_df)
    # st.text("Not Matching DF")
    # st.dataframe(st.session_state.not_matching_df)
    # st.text("Maybe DF")
    # st.dataframe(st.session_state.maybe_df)

    # <--------- Increase the 'i' value to go to the next row --------->
    st.session_state.i += 1

    if st.session_state.i >= (row-1):
        st.session_state.i = (st.session_state.i % row)

    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    
    color = ""
    if value <= 25:
        color = "red"
    elif value > 25 and value <= 50:
        color = "orange"
    elif value > 50 and value <= 75:
        color = "#9ac451"
    elif value > 75:
        color = "green"
    
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]

    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]

    display_tables(marico_index, gmap_index, color, value)


def onSubmit():
    
    global row
    global col
    drop_table_query = f'DROP TABLE IF EXISTS ds_gmap_marico_retailer_input'
    with conn.connect() as conn1:
        conn1.execute(drop_table_query)
    
    create_table_query = f'''
        create table ds_gmap_marico_retailer_input (
        "RETAILERCODE" STRING,
        "PLACE_ID" STRING,
        "MATCH_PERCENT" STRING
    );
    '''
    with conn.connect() as conn2:
        conn2.execute(create_table_query)

    input_df_copy = st.session_state.input_df.copy(deep=True).rename(columns={'RetailerCode': "RETAILERCODE",
                                                                               'place_id': "PLACE_ID",
                                                                               'match_percent': "MATCH_PERCENT"})
    input_df_copy = input_df_copy.astype(str)
    input_df_copy.to_sql('ds_gmap_marico_retailer_input', schema='DATA_SCIENCE', con=conn, if_exists='append', index=False)

    query = "select * from ds_gmap_marico_retailer_input"
    st.session_state.input_df = pd.DataFrame(pd.read_sql(con=conn, sql=query))

    st.session_state.input_df.rename(columns={
                                            "retailercode":'RetailerCode',
                                            "place_id":'place_id',
                                            "match_percent":'match_percent'}, inplace=True)
    
    row, col = st.session_state.input_df.shape
    
    st.session_state.input_df['match_percent'] = st.session_state.input_df['match_percent'].astype(int)
    value = st.session_state.input_df['match_percent'].iloc[st.session_state.i]
    marico_ret_code = st.session_state.input_df['RetailerCode'].iloc[st.session_state.i]
    marico_index = marico_df.loc[marico_df["RetailerCode"] == marico_ret_code].index[0]
    place_id = st.session_state.input_df['place_id'].iloc[st.session_state.i]
    gmap_index = gmap_df.loc[gmap_df["place_id"] == place_id].index[0]
    
    color = ""
    if value <= 25:
        color = "red"
    elif value > 25 and value <= 50:
        color = "orange"
    elif value > 50 and value <= 75:
        color = "#9ac451"
    elif value > 75:
        color = "green"

    display_tables(marico_index, gmap_index, color, value)
    
    run_query(st.session_state.matching_df, 'ds_gmap_marico_retailer_matched')
    run_query(st.session_state.not_matching_df, 'ds_gmap_marico_retailer_not_matched')
    run_query(st.session_state.maybe_df, 'ds_gmap_marico_retailer_maybe_matched')

    st.session_state.matching_df = st.session_state.matching_df[0:0]
    st.session_state.not_matching_df = st.session_state.not_matching_df[0:0]
    st.session_state.maybe_df = st.session_state.maybe_df[0:0]



def run_query(data, table):
    
    os.chdir(os.path.dirname(__file__))

    df = data.copy(deep=True)
    df.rename(columns={ 'RetailerName':"RETAILERNAME",
                        'name':"NAME",
                        'RetailerCode':"RETAILERCODE",
                        'place_id':"PLACE_ID",
                        'Address':"ADDRESS",
                        'vicinity':"VICINITY",
                        'match_percent':"MATCH_PERCENT"}, inplace=True)
    
    df = df.astype(str)
    snowflake_table_name = table

    df_array = np.array_split(df, (len(df)/15000) + 1)
    current_chunck = 0
    for x in df_array:
        current_chunck = current_chunck + 1
        x.to_sql(snowflake_table_name, schema='DATA_SCIENCE', con=conn, if_exists='append', index=False)
    ############################### From csv ###############################


st.markdown("")
st.markdown("")

st.markdown(
    """
    <style>
        div[data-testid="column"]:nth-of-type(1)
        {
            text-align: end
        } 

        div[data-testid="column"]:nth-of-type(2)
        {
            text-align: end;
        } 
        div[data-testid="column"]:nth-of-type(3)
        {
            text-align: center;
        } 
        div[data-testid="column"]:nth-of-type(5)
        {
            text-align: end;
        } 
    </style>
    """,unsafe_allow_html=True
)

btn_col1, btn_col2, btn_col3, btn_col4, btn_col5 = st.columns((3,2,2,2,3))


btn_nomatch = btn_col2.button("Not a match")
btn_maybe = btn_col3.button("Maybe")
btn_match = btn_col4.button('Match')
btn_submit = btn_col5.button('Submit')

if btn_match:
    onMatch()

if btn_nomatch:
    onNoMatch()

if btn_maybe:
    onMaybe()

if btn_submit:
    onSubmit()
