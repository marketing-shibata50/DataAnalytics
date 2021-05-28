import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import altair as alt
from gspread_dataframe import set_with_dataframe
import gspread
from google.oauth2.service_account import Credentials

st.title('Liny数字管理')

st.write("""
# 設定
以下のオプションから表示する範囲を指定
""")

def get_spred():
    # スプレッドシートの読み込み
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('service_account.json', scopes=scopes)
    gc = gspread.authorize(credentials)
    SP_SHEET_KEY_MEMBER = '13QemJFcysc703EpF7PsnJWDQyO6KiXFPWiGBkQ-cw6U'
    sh_member = gc.open_by_key(SP_SHEET_KEY_MEMBER)
    worksheet = sh_member.worksheet('貼付：Liny')
    temp_data = worksheet.get_all_values()
    register_L = pd.DataFrame(temp_data[2:], columns=temp_data[1])
    register_L['登録(フォロー)日時'] = register_L['登録(フォロー)日時'].str.split(' ', expand=True)[0]
    register_L['登録(フォロー)日時'] = pd.to_datetime(register_L['登録(フォロー)日時'])
    register_L['Month'] = register_L['登録(フォロー)日時'].dt.strftime("%Y-%m")
    return register_L

def get_data():
    db = pd.DataFrame()
    db = get_spred()
    db = db[['登録(フォロー)日時', 'ユーザーブロック', '流入時シナリオ', '希望勤務地', '個人情報入力経路', '流入経路', '流入経路詳細', '卒業年度', '電話番号']]
    db = db.rename(columns={'登録(フォロー)日時': 'Date', 'ユーザーブロック': '友だち', '電話番号': '個人情報'})
    db['Month'] = db['Date'].dt.strftime('%Y-%m')
    db['Date'] = db['Date'].dt.strftime('%Y%m%d')
    db['Date'] = db['Date'].astype('datetime64')
    return db

try:
    st.write(""" ## 表示日数選択 """)
    db = get_data()

    mindate = db['Date'].min()
    maxdate = db['Date'].max()

    s_time, e_time = st.date_input(
        '表示したい期間を入力してください',
        [mindate, maxdate],
        min_value = mindate,
        max_value = maxdate
    )

    s_time = np.datetime64(s_time)
    e_time = np.datetime64(e_time)
    temp_db = db[(db['Date'] >= s_time) & (db['Date'] <= e_time)]

    data = pd.pivot_table(data = temp_db, values = ["友だち", "卒業年度", "個人情報"], aggfunc ="count", index = "Date", margins=False, fill_value=0)
    # data['卒業年度%'] = (data['卒業年度'] / data['友だち']).map('{:.2%}'.format)
    # data['個人情報%'] = (data['個人情報'] / data['卒業年度']).map('{:.2%}'.format)
    data['卒業年度%'] = (data['卒業年度'] / data['友だち'])
    data['個人情報%'] = (data['個人情報'] / data['卒業年度'])
    data.index = data.index.strftime('%Y/%m/%d')
    data = data.T

    items = st.multiselect(
        '表示項目を選択してください',
        list(data.index),
        (['友だち', '卒業年度', '個人情報'])
    )

    st.write(f"""
    ###  日別の数字
    """)
    st.write(data)
    temp_data = data.loc[items]
    temp_data = temp_data.T.reset_index()
    temp_data = pd.melt(temp_data, id_vars=['Date'])
    chart = (
        alt.Chart(temp_data)
        .mark_line(opacity=0.8, clip=True)
        .encode(
            x="Date:T",
            y=alt.Y("value:Q", stack=None),
            color='variable:N'
        )
    )
    st.altair_chart(chart, use_container_width=True)

    st.write(f"""
    ###  月別の数字
    """)
    mdata = pd.pivot_table(data = temp_db, values = ["友だち", "卒業年度", "個人情報"], aggfunc ="count", index = "Month", margins=False, fill_value=0)
    # data['卒業年度%'] = (data['卒業年度'] / data['友だち']).map('{:.2%}'.format)
    # data['個人情報%'] = (data['個人情報'] / data['卒業年度']).map('{:.2%}'.format)
    mdata['卒業年度%'] = (mdata['卒業年度'] / mdata['友だち'])
    mdata['個人情報%'] = (mdata['個人情報'] / mdata['卒業年度'])
    mdata = mdata.T
    st.write(mdata)
    temp_mdata = mdata.loc[items]
    temp_mdata = temp_mdata.T.reset_index()
    temp_mdata = pd.melt(temp_mdata, id_vars=['Month'])
    chart = (
        alt.Chart(temp_mdata)
        .mark_line(opacity=0.8, clip=True)
        .encode(
            x="Month:T",
            y=alt.Y("value:Q", stack=None),
            color='variable:N'
        )
    )
    st.altair_chart(chart, use_container_width=True)
except:
    st.error("おっと！何かエラーが起きているようです。")