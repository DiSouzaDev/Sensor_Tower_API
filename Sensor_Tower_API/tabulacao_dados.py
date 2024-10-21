# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 12:00:39 2024

@author: barbosad
"""

from pandas.tseries.offsets import MonthEnd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
import requests

# Token from Sensor Tower API
token_api = ''

def bring_last_months():
    
    # List of the last six months
    each_six_month_dates = []
    # Date today
    today = datetime.today()
    # Converting to a string
    today_str = today.strftime('%Y-%m-%d')
    # First day of last Month
    first_day_one_month_ago = today.replace(day=1) - relativedelta(months=1)
    # Last day of last month
    last_day_last_month = today.replace(day=1) - relativedelta(days=1)
    # Last Six Months
    six_months_ago = first_day_one_month_ago - relativedelta(months=5)
    # 'For' to get the last six months
    for i in range(6):
        # First day less the relative month
        date_n_months_ago = first_day_one_month_ago - relativedelta(months=i)
        # Changing to a STRING
        date_n_months_ago_str = date_n_months_ago.strftime('%Y-%m-%d')
        # Adding this month inside the list
        each_six_month_dates.append(date_n_months_ago_str)
    
    # Changing to a STRING 
    first_day_one_month_ago = first_day_one_month_ago.strftime('%Y-%m-%d')
    # Changing to a STRING
    last_day_last_month = last_day_last_month.strftime('%Y-%m-%d')
    # Changing to a STRING
    six_months_ago_str = six_months_ago.strftime('%Y-%m-%d')
    
    return first_day_one_month_ago, last_day_last_month, six_months_ago_str, each_six_month_dates

def extract_ids_list(df_ids_list):
    # Getting the list of the Shooter Games
    ids_list = df_ids_list['app_id'].tolist()
    # Separating them per comma
    apps_ids_list = ','.join(ids_list)
    
    # Getting the API response
    def apps_names(token_api, apps_ids_list):
        # URL to get the names based on the unified apps IDs
        url = 'https://api.sensortower.com/v1/unified/apps'
        
        params = {
            'app_id_type': 'unified',
            'app_ids': apps_ids_list,
            'auth_token': token_api
        }
        # Getting the response
        response = requests.get(url, params=params)
        # Changing the response to JSON
        apps_info_json = response.json()
        # Function to extract the names of the apps
        df_apps_names = extract_names(apps_info_json)
        
        return df_apps_names
    
    # Call the function to get the names of the apps
    df_apps_names = apps_names(token_api, apps_ids_list)
    
    return apps_ids_list, df_apps_names

def extract_names(apps_info_json):
    # List with the apps names
    app_name_list = []
    
    # 'For' in the JSON with the apps information
    for app_info in apps_info_json['apps']:
        # Get the unified id
        unified_app_id = app_info.get('unified_app_id', {})
        # Get the app_name
        app_name = app_info.get('name', {})
        
        # If unified id is not null
        if unified_app_id:
            # Add app names inside the app_name_list
            app_name_list.append({
                'app_id': unified_app_id, 
                'app_name': app_name
            })
    
    # Changing the list into a DataFrame
    df_apps_names = pd.DataFrame(app_name_list)
    
    return df_apps_names

def merge_df(df_shooter_download_one, df_apps_names):
    # Merging Dataframes based on the 'app_id' column
    merged_df = pd.merge(df_shooter_download_one, df_apps_names, on='app_id')
    
    # Remove the 'app_id' column
    merged_df.drop('app_id', axis=1, inplace=True)
    
    # Moving the 'app_name' column to the first position
    cols = ['app_name'] + [col for col in merged_df.columns if col != 'app_name']
    final_df = merged_df[cols]
    
    return final_df

def extract_total_time_spent(total_online_spent_time_json):
    # List of the total time spent
    total_time_spent_list = []
    
    # For' in the JSON with the total time spent
    for unified_app in total_online_spent_time_json['unified_apps']:
        # Get the app id
        app_id = unified_app['unified_app_id']
        # Get the timeseries
        timeseries_data = unified_app['timeseries']
        
        # 'For' in the JSON with timeseries
        for entry in timeseries_data:
            # Get the month
            month = entry.get('date')
            # Get the time spent
            time_spent_sum = entry.get('time_spent', 0)
            # Get the total time spent
            total_time_spent_sum = entry.get('total_time_spent', 0)
            # Get the session duration
            session_duration_sum = entry.get('session_duration', 0)
            # Get the session count
            session_count_sum = entry.get('session_count', 0)
            # Get the total session count
            total_session_count_sum = entry.get('total_session_count', 0)
            # Adding the data inside the total time spent list
            total_time_spent_list.append({
                'app_id': str(app_id),
                'month': month,
                'time_spent': float(time_spent_sum),
                'total_time_spent': float(total_time_spent_sum),
                'session_duration': float(session_duration_sum),
                'session_count': float(session_count_sum),
                'total_session_count': float(total_session_count_sum)
            })
    
    # Changing the list into a DataFrame
    df_total_time_spent_list = pd.DataFrame(total_time_spent_list)
    # Moving the month column to the last position
    move_colum = df_total_time_spent_list.pop('month')
    df_total_time_spent_list['month'] = move_colum
    
    return df_total_time_spent_list

def total_time_spent_and_users(df_total_time_spent_list, df_shooter_dau_six):
    # Being sure that the data into the month column are STRINGS
    df_shooter_dau_six['month'] = df_shooter_dau_six['month'].astype(str)
    # Being sure that the data into the month column are STRINGS
    df_total_time_spent_list['month'] = df_total_time_spent_list['month'].astype(str)
    
    # Filter the last two months (excluding the current month)
    mask = (df_shooter_dau_six['month'] >= (pd.to_datetime('today') - MonthEnd(3)).strftime('%Y-%m-%d')) & \
           (df_shooter_dau_six['month'] < (pd.to_datetime('today') - MonthEnd(1)).strftime('%Y-%m-%d'))
    
    # Applying the filter inside the DataFrame with the last six months to get the last two months (excluding the current month)
    df_filtered = df_shooter_dau_six[mask]
    
    # Merging the DataFrames
    df_merged = pd.merge(df_filtered, df_total_time_spent_list, on=['app_name', 'month'])
    
    # Moving the column to the last position
    move_colum = df_merged.pop('month')
    df_merged['month'] = move_colum
    
    return df_merged

def extract_numbers_top_downloads(top_app_download):
    # List with the top downloads apps
    top_apps_list = []
    
    # 'For' in the JSON with the top downloads apps
    for app_id in top_app_download:
        # Get the app id
        app_info = app_id.get('app_id', {})
        # Get the revenue data from this month
        revenue_absolute = app_id.get('revenue_absolute', {})
        # Get the revenue data from the last month
        revenue_delta = app_id.get('revenue_delta', {})
        # Get the users from this month
        units_absolute = app_id.get('units_absolute', {})
        # Get the users from the last month
        units_delta = app_id.get('units_delta', {})
        
        # Adding the top apps inside the top downloads apps list
        if app_info:
            top_apps_list.append({
                'app_id': str(app_info), 
                'revenue_absolute': revenue_absolute, 
                'revenue_delta': revenue_delta, 
                'units_absolute': units_absolute, 
                'units_delta': units_delta
            })
          
    # Changing the apps list into a DataFrame
    df_top_apps_list = pd.DataFrame(top_apps_list)
          
    return df_top_apps_list

def extract_numbers_top_dau(top_app_dau):
    # top DAU apps list
    top_apps_list = []
    
    # 'For' in the top DAU apps list
    for app_id in top_app_dau:
        # Get the app id
        app_info = app_id.get('app_id', {})
        # Get the users fromm this month
        users_absolute = app_id.get('users_absolute', {})
        # Get the users from the last month
        users_delta = app_id.get('users_delta', {})
        # Adding the apps inside the top DAU apps list
        if app_info:
            top_apps_list.append({
                'app_id': str(app_info), 
                'users_absolute': users_absolute, 
                'users_delta': users_delta
            })
            
    # Changing the top DAU apps list into a DataFrame
    df_top_apps_list = pd.DataFrame(top_apps_list)
    
    return df_top_apps_list

def extract_app_id_retention(top_shooter_download_one_json):
    # List of the retention
    rows = []
    
    # 'For' in the JSON with the retention data
    for item in top_shooter_download_one_json:
        
        # Get the unified app id
        main_id = item['app_id']
        
        # 'For' in the itens to get the apps ids
        for entity in item.get('entities', []):
            # Get the IOS or ANDROID IDs
            app_id = entity['app_id']
            
            # To know if its a IOS or ANDROID id
            if isinstance(app_id, str):
                # Adding the id inside the list
                rows.append({'app_id': main_id, 'android_id': app_id, 'ios_id': None})
            elif isinstance(app_id, int):
                # Adding the id inside the list
                rows.append({'app_id': main_id, 'android_id': None, 'ios_id': app_id})
    
    # Changing the list with retention into a DataFrame
    df = pd.DataFrame(rows)
    # Droppin the null ios IDs    
    df_ios_ids = df[['app_id', 'ios_id']].dropna(subset=['ios_id'])
    # Droppin the null android IDs
    df_android_ids = df[['app_id', 'android_id']].dropna(subset=['android_id'])
    
    return df_ios_ids, df_android_ids

def ext_retention_data(retention_ids_ios_json):
    # List of the retention data apps
    rows = []
    
    # 'For' in the JSON with the retention data
    for app in retention_ids_ios_json['app_data']:
        # Get the app id
        app_id = app['app_id']
        # Get the retention values
        retention_values = app['corrected_retention']
        # Get the date
        date = app['date']
        
        # 'For' in the retention data
        for day, retention in enumerate(retention_values, start=1):
            # Adding the data inside the list of the retention data apps
            rows.append({'app_id': app_id, 'date': date, 'day': day, 'retention': retention})
    
    # Changing the list with extracted retention into a DataFrame
    df_retention = pd.DataFrame(rows)
    
    return df_retention

'''
# Code to get the Spent Time using JSON
def extract_shooter_ids_list_json(top_shooter_download_one_json):
    shooter_list = []
    
    for app_id in top_shooter_download_one_json:
        
        primary_app_id = app_id.get('app_id')
        
        if primary_app_id:
            shooter_list.append(primary_app_id)
    
    apps_ids_shooter_list = ','.join(shooter_list)
    
    return apps_ids_shooter_list
'''