import re
from altair.vegalite.v4.api import concat
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
import re

def read_spred_keys():
    # スプレッドシートの読み込み
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    credentials = Credentials.from_service_account_file('service_account.json', scopes=scopes)
    gc = gspread.authorize(credentials)

    SP_SHEET_KEY_SPARK = '1Tg7M8InNGxVmtQD01ujK6tFWTrmEAyU1pxqo9XCFtIg'
    SP_SHEET_KEY_MEMBER = '13QemJFcysc703EpF7PsnJWDQyO6KiXFPWiGBkQ-cw6U'
    SP_SHEET_KEY_MASTER = '1oeORQTjudO9-B4JjyFqilHtrOQtlvOmdUNEp8NnwSzI'

    sh_spark = gc.open_by_key(SP_SHEET_KEY_SPARK)
    sh_member = gc.open_by_key(SP_SHEET_KEY_MEMBER)
    sh_master = gc.open_by_key(SP_SHEET_KEY_MASTER)

    return sh_spark, sh_member, sh_master

def read_spred_tab():
    SP_SHEET = {
        'SP_SHEET_IMP': '【計算用】表示回数',
        'SP_SHEET_CT': '【更新】クリック'
    }
    SP_SHEET_REGISTER = {
        'SP_SHEET_R': '貼付：会員登録',
        'SP_SHEET_L': '貼付：Liny'
    }
    return SP_SHEET, SP_SHEET_REGISTER

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

# スプレッドの情報抜き出し
sh_spark, sh_member, sh_master = read_spred_keys()
SP_SHEET, SP_SHEET_REGISTER = read_spred_tab()

# 初期設定
val = pd.DataFrame()

# マスターデータ
worksheet = sh_master.worksheet('マスターデータ')
temp_master = worksheet.get_all_values()
master = pd.DataFrame(temp_master[4:], columns=temp_master[3])
master = master[['ID', '進捗', '担当', 'ニーズ']]
st.write(master)

# 表示回数の計算
imp = pd.DataFrame()
worksheet = sh_spark.worksheet(SP_SHEET['SP_SHEET_IMP'])
temp_imp = worksheet.get_all_values()
imp = pd.DataFrame(temp_imp[6:], columns=temp_imp[4])
imp = imp.rename(columns={'ID': 'Date'})
imp['Date'] = pd.to_datetime(imp['Date'])
imp['Date'] = imp['Date'].dt.strftime('%Y/%m/%d')
imp = imp.replace(r'^\s*$', 0, regex=True)
imp = pd.melt(imp, id_vars=['Date']).rename(columns={'value': 'Count', 'variable': 'ID'})
imp['type'] = 'imp'
val = imp

# CT数の計算
ct = pd.DataFrame()
worksheet = sh_spark.worksheet(SP_SHEET['SP_SHEET_CT'])
temp_ct = worksheet.get_all_values()
ct = pd.DataFrame(temp_ct[15:], columns=temp_ct[14]).rename(columns={'Total Events': 'Count', 'Event Label': 'ID'})
ct['Date'] = pd.to_datetime(ct['Date'])
ct['Date'] = ct['Date'].dt.strftime('%Y/%m/%d')
ct['type'] = 'ct'
val = pd.concat([val,ct])

# 会員登録の計算
worksheet = sh_member.worksheet(SP_SHEET_REGISTER['SP_SHEET_R'])
temp_data = worksheet.get_all_values()
register_R = pd.DataFrame(temp_data[1:], columns=temp_data[0]).rename(columns={'ID': '学生ID', '登録日時': 'Date', '経由点(バナー)': 'ID'})
register_R = register_R[['Date', 'ID']]
register_R['pattern'] = register_R['ID'].str[-1]
temp = register_R.loc[register_R['pattern']=='p']
temp['ID'] = temp['ID'].str[:-1]
register_R['ID'].loc[register_R['pattern']=='p'] = temp['ID']
register_R['Count'] = 1
register_R['Date'] = register_R['Date'].str.split(' ', expand=True)[0]
register_R['Date'] = pd.to_datetime(register_R['Date'])
register_R['Date'] = register_R['Date'].dt.strftime('%Y/%m/%d')
val_register_R = pd.pivot_table(data = register_R, values = "Count", aggfunc ="count", index = ["Date", "ID"], margins=False, fill_value=0)
val_register_R = val_register_R.reset_index()
val_register_R['type'] = 'cv'
val = pd.concat([val,val_register_R])

# LINE登録の計算
worksheet = sh_member.worksheet(SP_SHEET_REGISTER['SP_SHEET_L'])
temp_data = worksheet.get_all_values()
register_L = pd.DataFrame(temp_data[2:], columns=temp_data[1]).rename(columns={'ID': '学生ID', '登録(フォロー)日時': 'Date', '流入経路詳細': 'ID'})
register_L = register_L[['Date', 'ID', '学生ID', '卒業年度', '電話番号']].rename(columns={'学生ID': '友だち', '電話番号': 'Count'})
register_L['pattern'] = register_L['ID'].str[-1]
temp = register_R.loc[register_L['pattern']=='p']
temp['ID'] = temp['ID'].str[:-1]
register_L['ID'].loc[register_L['pattern']=='p'] = temp['ID']
register_L['Date'] = register_L['Date'].str.split(' ', expand=True)[0]
register_L['Date'] = pd.to_datetime(register_L['Date'])
register_L['Date'] = register_L['Date'].dt.strftime('%Y/%m/%d')
val_register_L = pd.pivot_table(data = register_L, values = "Count", aggfunc ="count", index = ["Date", "ID"], margins=False, fill_value=0)
val_register_L = val_register_L.reset_index()
val_register_L['type'] = 'cv'
val = pd.concat([val,val_register_L])


val['Date'] = pd.to_datetime(val['Date'])
val['Month'] = val['Date'].dt.strftime("%Y-%m")
val['Date'] = val['Date'].dt.strftime('%Y/%m/%d')
val_daily = pd.merge(val, master, left_on='ID', right_on='ID', how='left')
st.write(val_daily)

# val_month = val.

# values_month = pd.DataFrame(whole_data.groupby('Month').sum()['Unique Pageviews']).rename(columns={'Unique Pageviews': 'UU'})
# temp_values_R = pd.DataFrame(register_R.groupby('Month').count()['ID']).rename(columns={'ID': '登録'})
# temp_values_L = pd.DataFrame(register_L.groupby('Month').count()['ID']).rename(columns={'ID': 'LINE'})
# values_month = pd.merge(values_month, temp_values_R, left_index=True, right_index=True, how="left")
# values_month = pd.merge(values_month, temp_values_L, left_index=True, right_index=True, how="left")
# values_month = values_month.fillna(0)

# st.write(values_month.T)
# chart = get_chart(values_month)
# st.altair_chart(chart, use_container_width=True)