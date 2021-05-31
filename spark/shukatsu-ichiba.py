import re
from altair.vegalite.v4.api import concat
from altair.vegalite.v4.schema.channels import Column
from pandas.core import groupby
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
    credentials = Credentials.from_service_account_file('spark/service_account.json', scopes=scopes)
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

def make_register(db):
    db = db[['Date', 'ID']]

    db['pattern'] = db['ID'].str[-1]
    temp = db.loc[db['pattern']=='p']
    temp['ID'] = temp['ID'].str[:-1]
    db['ID'].loc[db['pattern']=='p'] = temp['ID']
    db['CV'] = 1
    db['Date'] = db['Date'].str.split(' ', expand=True)[0]

    db = pd.pivot_table(data = db, values = "CV", aggfunc ="count", index = ["Date", "ID"], margins=False, fill_value=0)
    db = db.reset_index()
    db['CV'] = db['CV'].astype(int)
    return db

def make_datetime(db):
    db['Date'] = pd.to_datetime(db['Date'])
    db['Date'] = db['Date'].dt.strftime('%Y/%m/%d')
    return db

def calc_per(db):
    db['CTR'] = db['CT'] / db['Imp']
    db['CVR'] = db['CV'] / db['CT']
    db = pd.merge(db, master, on='ID', how='left')
    db = db.fillna(0)
    st.write(db)
    return db

# スプレッドの情報抜き出し
sh_spark, sh_member, sh_master = read_spred_keys()
SP_SHEET, SP_SHEET_REGISTER = read_spred_tab()

# マスターデータ
worksheet = sh_master.worksheet('マスターデータ')
temp_master = worksheet.get_all_values()
master = pd.DataFrame(temp_master[4:], columns=temp_master[3])
master = master[['ID', '進捗', '担当', 'ニーズ']]
master = master.replace(r'^\s*$', 0, regex=True)
master = master.loc[master['ID']!=0]
st.write(master)

# 表示回数の計算
imp = pd.DataFrame()
worksheet = sh_spark.worksheet(SP_SHEET['SP_SHEET_IMP'])
temp_imp = worksheet.get_all_values()
imp = pd.DataFrame(temp_imp[6:], columns=temp_imp[4]).rename(columns={'ID': 'Date'})
imp = pd.melt(imp, id_vars=['Date']).rename(columns={'value': 'Imp', 'variable': 'ID'})
imp = imp.replace(r'^\s*$', 0, regex=True)
imp['Imp'] = imp['Imp'].astype(int)
imp = make_datetime(imp)

# CT数の計算
ct = pd.DataFrame()
worksheet = sh_spark.worksheet(SP_SHEET['SP_SHEET_CT'])
temp_ct = worksheet.get_all_values()
ct = pd.DataFrame(temp_ct[15:], columns=temp_ct[14]).rename(columns={'Total Events': 'CT', 'Event Label': 'ID'})
ct['pattern'] = ct['ID'].str[-1]
temp = ct.loc[ct['pattern']=='p']
temp['ID'] = temp['ID'].str[:-1]
ct['ID'].loc[ct['pattern']=='p'] = temp['ID']
ct = ct.groupby(['Date', 'ID']).sum()['CT']
ct = ct.reset_index()
ct['CT'] = ct['CT'].astype(int)
ct = make_datetime(ct)

# 会員登録の計算
worksheet = sh_member.worksheet(SP_SHEET_REGISTER['SP_SHEET_R'])
temp_data = worksheet.get_all_values()
register_R = pd.DataFrame(temp_data[1:], columns=temp_data[0]).rename(columns={'ID': '学生ID', '登録日時': 'Date', '経由点(バナー)': 'ID'})
val_register_R = make_register(register_R)

# LINE登録の計算
worksheet = sh_member.worksheet(SP_SHEET_REGISTER['SP_SHEET_L'])
temp_data = worksheet.get_all_values()
register_L = pd.DataFrame(temp_data[2:], columns=temp_data[1]).rename(columns={'ID': '学生ID', '登録(フォロー)日時': 'Date', '流入経路詳細': 'ID'})
val_register_L = make_register(register_L)

val_register = pd.concat([val_register_R, val_register_L])
val_register = make_datetime(val_register)

val_daily = pd.merge(imp, ct, on=['Date', 'ID'], how='left')
val_daily = pd.merge(val_daily, val_register, on=['Date', 'ID'], how='left')
val_daily = calc_per(val_daily)

val_month = val_daily
val_month['Date'] = pd.to_datetime(val_month['Date'])
val_month['Month'] = val_month['Date'].dt.strftime('%Y%m')
del val_month['Date']
val_month = val_month.groupby(['Month', 'ID']).sum()[['Imp', 'CT', 'CV']]
val_month = val_month.reset_index()
val_month = calc_per(val_month)