import re
from altair.vegalite.v4.schema.channels import Column
import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import gspread
from google.oauth2.service_account import Credentials
from bs4 import BeautifulSoup
import requests
import datetime as dt
from gspread_dataframe import set_with_dataframe

def read_spred_keys():
    # スプレッドシートの読み込み
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('service_account.json', scopes=scopes)
    gc = gspread.authorize(credentials)

    SP_SHEET_KEY_NUMBER = '1DBhckcpEKz2mJliq6UAO7BJrHlScvL1oY5QbwObih0U'
    SP_SHEET_KEY_MEMBER = '13QemJFcysc703EpF7PsnJWDQyO6KiXFPWiGBkQ-cw6U'

    sh_number = gc.open_by_key(SP_SHEET_KEY_NUMBER)
    sh_member = gc.open_by_key(SP_SHEET_KEY_MEMBER)

    return sh_number, sh_member

def read_spred_tab():
    SP_SHEET = {
        'SP_SHEET_WHOLE': '全体',
        'SP_SHEET_LP': 'LP',
        'SP_SHEET_EBOOK': 'ebook',
        'SP_SHEET_AGENT': 'agent',
    }
    SP_SHEET_REGISTER = {
        'SP_SHEET_R': '貼付：会員登録',
        'SP_SHEET_L': '貼付：Liny'
    }
    return SP_SHEET, SP_SHEET_REGISTER

@st.cache
def read_spred_number(SP_SHEET_XXX):
    data = pd.DataFrame()
    worksheet = sh_number.worksheet(SP_SHEET[SP_SHEET_XXX])
    temp_data = worksheet.get_all_values()
    data = pd.DataFrame(temp_data[15:], columns=temp_data[14])
    data[['Pageviews', 'Unique Pageviews']] = data[['Pageviews', 'Unique Pageviews']].astype(int)
    data['Date'] = pd.to_datetime(data['Date'])
    data['Month'] = data['Date'].dt.strftime("%Y-%m")
    return data

def get_chart(values_month):
    values_month = values_month.reset_index()
    ymin1 = values_month['UU'].min()
    ymax1 = values_month['UU'].max()
    ymin2 = values_month['登録'].min()
    ymax2 = values_month['登録'].max()

    base = alt.Chart(values_month).encode(
        alt.X('Month:T', axis=alt.Axis(title=None))
    )
    line1 = base.mark_line(color='#ffffff').encode(
        alt.Y('UU',
            axis=alt.Axis(title='UU', titleColor='#ffffff'),
            scale=alt.Scale(domain=[ymin1, ymax1])
            )
    )
    line2 = base.mark_line(stroke='#ff0000', interpolate='monotone').encode(
        alt.Y('登録',
            axis=alt.Axis(),
            scale=alt.Scale(domain=[ymin2, ymax2])
            )
    )
    chart = alt.layer(line1, line2).resolve_scale(
        y = 'independent'
    )
    return chart

sh_number, sh_member = read_spred_keys()
SP_SHEET, SP_SHEET_REGISTER = read_spred_tab()
whole_data = read_spred_number('SP_SHEET_WHOLE')
lp_data = read_spred_number('SP_SHEET_LP')
ebook_data = read_spred_number('SP_SHEET_EBOOK')
agent_data = read_spred_number('SP_SHEET_AGENT')

worksheet = sh_member.worksheet(SP_SHEET_REGISTER['SP_SHEET_R'])
temp_data = worksheet.get_all_values()
register_R = pd.DataFrame(temp_data[1:], columns=temp_data[0])
register_R['登録日時'] = register_R['登録日時'].str.split(' ', expand=True)[0]
register_R['登録日時'] = pd.to_datetime(register_R['登録日時'])
register_R['Month'] = register_R['登録日時'].dt.strftime("%Y-%m")

worksheet = sh_member.worksheet(SP_SHEET_REGISTER['SP_SHEET_L'])
temp_data = worksheet.get_all_values()
register_L = pd.DataFrame(temp_data[2:], columns=temp_data[1])
register_L['登録(フォロー)日時'] = register_L['登録(フォロー)日時'].str.split(' ', expand=True)[0]
register_L['登録(フォロー)日時'] = pd.to_datetime(register_L['登録(フォロー)日時'])
register_L['Month'] = register_L['登録(フォロー)日時'].dt.strftime("%Y-%m")

values_month = pd.DataFrame(whole_data.groupby('Month').sum()['Unique Pageviews']).rename(columns={'Unique Pageviews': 'UU'})
temp_values_l = pd.DataFrame(lp_data.groupby('Month').sum()['Unique Pageviews']).rename(columns={'Unique Pageviews': 'LP'})
temp_values_e = pd.DataFrame(ebook_data.groupby('Month').sum()['Unique Pageviews']).rename(columns={'Unique Pageviews': 'ebook'})
temp_values_a = pd.DataFrame(agent_data.groupby('Month').sum()['Unique Pageviews']).rename(columns={'Unique Pageviews': 'agent'})
temp_values_R = pd.DataFrame(register_R.groupby('Month').count()['ID']).rename(columns={'ID': '登録'})
temp_values_L = pd.DataFrame(register_L.groupby('Month').count()['ID']).rename(columns={'ID': 'LINE'})
values_month = pd.merge(values_month, temp_values_l, left_index=True, right_index=True, how="left")
values_month = pd.merge(values_month, temp_values_e, left_index=True, right_index=True, how="left")
values_month = pd.merge(values_month, temp_values_a, left_index=True, right_index=True, how="left")
values_month = pd.merge(values_month, temp_values_R, left_index=True, right_index=True, how="left")
values_month = pd.merge(values_month, temp_values_L, left_index=True, right_index=True, how="left")
values_month = values_month.fillna(0)

st.write(values_month.T)
chart = get_chart(values_month)
st.altair_chart(chart, use_container_width=True)