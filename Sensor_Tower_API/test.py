# -*- coding: utf-8 -*-
"""
Created on Thu Aug 15 15:47:48 2024

@author: barbosad
"""

token_api = ''

import pandas as pd
import requests
import tabulacao_dados as td

# Calculate the date one month ago and today
first_day_one_month_ago = '2024-06-01'
last_day_last_month = '2024-07-31'

# URL to get Time Spent of Unified Apps using a list of IDs
url = "https://api.sensortower.com/v1/apps/timeseries/unified_apps"

# List of information to get
timeseries_list = ','.join(['time_spent','total_time_spent','session_duration', 'session_count', 'total_session_count'])

# Define the parameters
params = {
    'start_date': first_day_one_month_ago,
    'end_date': last_day_last_month,
    'app_ids': apps_ids_shooter_list,
    'timeseries': timeseries_list,
    'regions': 'BR',
    'time_period': 'month',
    'auth_token': token_api
}

# Getting the response
response = requests.get(url, params=params)
# Changing the response to JSON
total_online_spent_time_json = response.json()
# Extractinc the most relevant information from JSON
ext_total_time_spent_list = td.extract_total_time_spent(total_online_spent_time_json)
# Changing the response from JSON to DF
df_total_time_spent_list = pd.DataFrame(ext_total_time_spent_list)
# Merge to replace app_id with app_name
df_total_time_spent_list = td.merge_df(df_total_time_spent_list, df_apps_names)


# Get shooter list
_, df_total_time_spent_list = sta.top_apps_shooter_download_one_month(token_api, first_day_one_month_ago, last_day_last_month)

# Get users of the last two months and merge with the spent list
df_time_apent_users = total_time_spent_and_users(df_total_time_spent_list, df_shooter_dau_six)




'''
def extract_shooter_ids_list(df_shooter_download_one):
    # Getting the list of the Shooter Games
    shooter_list = df_shooter_download_one['app_id'].tolist()
    # Separating them per comma
    apps_ids_shooter_list = ','.join(shooter_list)
'''