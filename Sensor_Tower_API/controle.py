# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 12:00:39 2024

@author: barbosad
"""

import sensor_tower_api as sta
from tabulacao_dados import bring_last_months, total_time_spent_and_users
from google.Export_df_to_gsheets_Auth import login_token, Delete_Data_on_Sheets, Export_Data_To_Sheets
from YouTube import Youtube_functions

# Token from Google API
token = r''
service = login_token(token)

# Token from Sensor Tower API
token_api = ''

# Address to Send
gsheetId = ''
sheetId_download_one = 'Competitors_Data' + '!A:E'
sheetId_download_six = 'Competitors_Data' + '!G:K'
sheetId_dau_one = 'Competitors_Data' + '!M:O'
sheetId_dau_six = 'Competitors_Data' + '!Q:T'
sheetId_shooter_download_one = 'Competitors_Data' + '!V:Z'
sheetId_shooter_download_six = 'Competitors_Data' + '!AB:AF'
sheetId_shooter_dau_one = 'Competitors_Data' + '!AH:AJ'
sheetId_shooter_dau_six = 'Competitors_Data' + '!AL:AO'
sheetId_spent_time = 'Competitors_Data' + '!AQ:AY'
sheetId_ios_retention = 'Competitors_Data' + '!BA:BE'
sheetId_android_retention = 'Competitors_Data' + '!BG:BK'
sheetId_delete = 'Competitors_Data' + '!A:BM'

# Bring the current dates
first_day_one_month_ago, last_day_last_month, six_months_ago_str, each_six_month_dates = bring_last_months()

# Extract data from APIs JSON to DF
df_top_download_one = sta.top_apps_download_one_month(token_api, first_day_one_month_ago, last_day_last_month)
df_top_download_six = sta.top_apps_download_six_months(token_api, last_day_last_month, six_months_ago_str)
df_top_dau_one = sta.top_apps_dau_one_month(token_api, first_day_one_month_ago)
df_top_dau_six = sta.top_apps_dau_six_months(token_api, each_six_month_dates)
df_shooter_download_one, df_total_time_spent_list, df_ios_ids, df_android_ids = sta.top_apps_shooter_download_one_month(token_api, first_day_one_month_ago, last_day_last_month)
df_shooter_download_six = sta.top_apps_shooter_download_six_months(token_api, last_day_last_month, six_months_ago_str)
df_shooter_dau_one = sta.top_apps_shooter_dau_month(token_api, first_day_one_month_ago)
df_shooter_dau_six = sta.top_apps_shooter_dau_six_months(token_api, each_six_month_dates)

# Get users of the last two months and merge with the spent list
df_time_spent_users = total_time_spent_and_users(df_total_time_spent_list, df_shooter_dau_six)

# Get the retention data from IOS and ANDROID apps
df_retention_ios, df_retention_android = sta.revivel_time_shooter_apps(token_api, df_ios_ids, df_android_ids)

# Delete abd Send data to Sheets
Delete_Data_on_Sheets(gsheetId,sheetId_delete,service)
Export_Data_To_Sheets(gsheetId,sheetId_download_one,df_top_download_one,service)
Export_Data_To_Sheets(gsheetId,sheetId_download_six,df_top_download_six,service)
Export_Data_To_Sheets(gsheetId,sheetId_dau_one,df_top_dau_one,service)
Export_Data_To_Sheets(gsheetId,sheetId_dau_six,df_top_dau_six,service)
Export_Data_To_Sheets(gsheetId,sheetId_shooter_download_one,df_shooter_download_one,service)
Export_Data_To_Sheets(gsheetId,sheetId_shooter_download_six,df_shooter_download_six,service)
Export_Data_To_Sheets(gsheetId,sheetId_shooter_dau_one,df_shooter_dau_one,service)
Export_Data_To_Sheets(gsheetId,sheetId_shooter_dau_six,df_shooter_dau_six,service)
Export_Data_To_Sheets(gsheetId,sheetId_spent_time,df_time_spent_users,service)
Export_Data_To_Sheets(gsheetId,sheetId_ios_retention,df_retention_ios,service)
Export_Data_To_Sheets(gsheetId,sheetId_android_retention,df_retention_android,service)