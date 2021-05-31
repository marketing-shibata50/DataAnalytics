import streamlit as st
import pandas as pd
import numpy as np
import datetime as dt
import altair as alt
from gspread_dataframe import set_with_dataframe
import gspread
from google.oauth2.service_account import Credentials

st.title('Liny数字管理')

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

# @st.cache
def get_data():
    db = pd.DataFrame()
    db = get_spred()
    db = db[['登録(フォロー)日時', 'ユーザーブロック', '流入時シナリオ', '希望勤務地', '個人情報入力経路', '流入経路', '流入経路詳細', '卒業年度', '電話番号']]
    db = db.rename(columns={'登録(フォロー)日時': 'Date', 'ユーザーブロック': '友だち', '電話番号': '個人情報'})
    db['Month'] = db['Date'].dt.strftime('%Y-%m')
    db['Date'] = db['Date'].astype('datetime64')
    db = db.replace(r'^\s*$', np.nan, regex=True)
    return db

def get_chart(data, period):
    data = data.reset_index()
    ymin1 = 0
    ymax1 = data['友だち'].max()
    ymin2 = 0
    ymax2 = 1
    base = alt.Chart(data).encode(
        alt.X(period+':T', axis=alt.Axis(title=None))
    )
    line1 = base.mark_line(color='#ff0000').encode(
        alt.Y('友だち',
            axis=alt.Axis(title='UU', titleColor='#ff0000'),
            scale=alt.Scale(domain=[ymin1, ymax1])
            )
    )
    line2 = base.mark_line(color='#0000ff').encode(
        alt.Y('個人情報',
            axis = None,
            scale=alt.Scale(domain=[ymin1, ymax1]))
    )
    line3 = base.mark_line(stroke='#ffff00').encode(
        alt.Y('全体%',
            axis=alt.Axis(title='全体%', titleColor='#ffff00'),
            scale=alt.Scale(domain=[ymin2, ymax2])
            )
    )
    chart = alt.layer(line1, line2, line3).resolve_scale(
        y = 'independent'
    )
    return chart

def calc_db(temp_db, period):
    data = pd.pivot_table(data = temp_db, values = ["友だち", "卒業年度", "個人情報"], aggfunc ="count", index = period, margins=False, fill_value=0)
    data['卒業年度%'] = (data['卒業年度'] / data['友だち'])
    data['個人情報%'] = (data['個人情報'] / data['卒業年度'])
    data['全体%'] = (data['個人情報'] / data['友だち'])
    # data.index = data.index.strftime('%Y/%m/%d')
    st.write(data.T)
    return get_chart(data, period)

st.write(""" # 表示日数選択 """)
db = get_data()

mindate = dt.date(2020,8,1)
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
temp_db['Date'] = temp_db['Date'].dt.strftime('%Y/%m/%d')

st.write("# 全体")
# st.write("##  日別の数字")
# chart = calc_db(temp_db, 'Date')
# st.altair_chart(chart, use_container_width=True)

st.write("##  月別の数字")
chart = calc_db(temp_db, 'Month')
st.altair_chart(chart, use_container_width=True)

st.write("# 就活市場経由")
temp_db_s = temp_db[temp_db.流入経路.eq('就活市場')]

# st.write("##  日別の数字")
# chart = calc_db(temp_db_s, 'Date')
# st.altair_chart(chart, use_container_width=True)

st.write("##  月別の数字")
chart = calc_db(temp_db_s, 'Month')
st.altair_chart(chart, use_container_width=True)

st.write("# 広告経由")
temp_db_a = temp_db[temp_db.流入経路.eq('LP経由')]

# st.write("##  日別の数字")
# chart = calc_db(temp_db_a, 'Date')
# st.altair_chart(chart, use_container_width=True)

st.write("##  月別の数字")
chart = calc_db(temp_db_a, 'Month')
st.altair_chart(chart, use_container_width=True)