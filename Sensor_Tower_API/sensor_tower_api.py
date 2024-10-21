# -*- coding: utf-8 -*-
"""
Created on Wed Aug 21 12:00:39 2024

@author: barbosad
"""

import requests
import pandas as pd
import tabulacao_dados as td
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# Get the Top Apps by Download and Revenue per month
def top_apps_download_one_month(token_api, first_day_one_month_ago, last_day_last_month):
    # URL to get Top Apps by Download and Revenue using a range of date
    url = "https://api.sensortower.com/v1/unified/sales_report_estimates_comparison_attributes"

    # Define the parameters
    params = {
        'comparison_attribute': 'absolute',
        'time_range': 'month',
        'measure': 'units',
        'device_type': 'total',
        'category': '6014',
        'date': first_day_one_month_ago,
        'end_date': last_day_last_month,
        'regions': 'BR',
        'limit': '25',
        'custom_tags_mode': 'include_unified_apps',
        'auth_token': token_api
    }
    
    # Getting the response
    response = requests.get(url, params=params)
    # Changing the response to JSON
    top_download_one_json = response.json()
    # Extractinc the most relevant information from JSON
    ext_top_download_one = td.extract_numbers_top_downloads(top_download_one_json)
    # Changing the response from JSON to DF
    df_top_download_one = pd.DataFrame(ext_top_download_one)
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_top_download_one)
    # Merge to replace app_id with app_name
    df_top_download_one = td.merge_df(df_top_download_one, df_apps_names)
    
    return df_top_download_one
    
def top_apps_download_six_months(token_api, last_day_last_month, six_months_ago_str):
    # URL to get Top Apps by Download and Revenue using a range of date
    url = "https://api.sensortower.com/v1/unified/sales_report_estimates_comparison_attributes"

    # Define the parameters
    params = {
        'comparison_attribute': 'absolute',
        'time_range': 'month',
        'measure': 'units',
        'device_type': 'total',
        'category': '6014',
        'date': six_months_ago_str,
        'end_date': last_day_last_month,
        'regions': 'BR',
        'limit': '25',
        'custom_tags_mode': 'include_unified_apps',
        'auth_token': token_api
    }
    
    # Getting the response
    response = requests.get(url, params=params)
    # Changing the response to JSON
    top_download_six_json = response.json()
    # Extractinc the most relevant information from JSON
    ext_top_download_six = td.extract_numbers_top_downloads(top_download_six_json)
    # Changing the response from JSON to DF
    df_top_download_six = pd.DataFrame(ext_top_download_six)
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_top_download_six)
    # Merge to replace app_id with app_name
    df_top_download_six = td.merge_df(df_top_download_six, df_apps_names)
    
    return df_top_download_six

def top_apps_dau_one_month(token_api, first_day_one_month_ago):
    # URL to get Top Apps by Active Users using one date
    url = "https://api.sensortower.com/v1/unified/top_and_trending/active_users"

    # Define the parameters
    params = {
        'comparison_attribute': 'absolute',
        'time_range': 'month',
        'measure': 'DAU',
        'category': '6014',
        'date': first_day_one_month_ago,
        'regions': 'BR',
        'limit': '25',
        'device_type': 'total',
        'auth_token': token_api
    }
    
    # Getting the response
    response = requests.get(url, params=params)
    # Changing the response to JSON
    top_dau_one_json = response.json()
    # Extractinc the most relevant information from JSON
    ext_top_dau_one = td.extract_numbers_top_dau(top_dau_one_json)
    # Changing the response from JSON to DF
    df_top_dau_one = pd.DataFrame(ext_top_dau_one)
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_top_dau_one)
    # Merge to replace app_id with app_name
    df_top_dau_one = td.merge_df(df_top_dau_one, df_apps_names)
    
    return df_top_dau_one

def top_apps_dau_six_months(token_api, each_six_month_dates):
    # DF with all the results
    df_top_dau_six = pd.DataFrame()
    
    # 'For' in the list of dates
    for month in each_six_month_dates:
        
        # URL to get Top Apps by Active Users using a list of dates
        url = "https://api.sensortower.com/v1/unified/top_and_trending/active_users"
        
        # Define the parameters
        params = {
            'comparison_attribute': 'absolute',
            'time_range': 'month',
            'measure': 'DAU',
            'category': '6014',
            'date': month,
            'regions': 'BR',
            'limit': '25',
            'device_type': 'total',
            'auth_token': token_api
        }
        
        # Getting the response
        response = requests.get(url, params=params)
        # Changing the response to JSON
        top_dau_one_json = response.json()
        # Extractinc the most relevant information from JSON
        month_info = td.extract_numbers_top_dau(top_dau_one_json)
        # Changing the response from JSON to DF
        df_month_info = pd.DataFrame(month_info)
        # Adding the month in the last collum of the DF
        df_month_info['month'] = month
        # Concat the information to the main DF
        df_top_dau_six = pd.concat([df_top_dau_six, df_month_info], axis=0)
    
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_top_dau_six)
    # Merge to replace app_id with app_name
    df_top_dau_six = td.merge_df(df_top_dau_six, df_apps_names)
    
    return df_top_dau_six
    

def top_apps_shooter_download_one_month(token_api, first_day_one_month_ago, last_day_last_month):
    # URL to get Top Shooter Apps by Download and Revenue using a range of date
    # Also use the response to get the time_spent and some unified and separeted ids for others requests
    url = "https://api.sensortower.com/v1/unified/sales_report_estimates_comparison_attributes"

    # Define the parameters
    params = {
        'comparison_attribute': 'absolute',
        'time_range': 'month',
        'measure': 'units',
        'device_type': 'total',
        'category': '6014',
        'date': first_day_one_month_ago,
        'end_date': last_day_last_month,
        'regions': 'BR',
        'limit': '25',
        'custom_fields_filter_id': '600abc40241bc16eb8510760',
        'custom_tags_mode': 'include_unified_apps',
        'auth_token': token_api
    }
    
    # Getting the response
    response = requests.get(url, params=params)
    # Changing the response to JSON
    top_shooter_download_one_json = response.json()
    
    # Get the IOS and ANDROID ids
    df_ios_ids, df_android_ids = td.extract_app_id_retention(top_shooter_download_one_json)
    # Get the IOS apps' names
    _, df_apps_names = td.extract_ids_list(df_ios_ids)
    # Merge to replace app_id with app_name
    df_ios_ids = td.merge_df(df_ios_ids, df_apps_names)
    # Get the ANDROID apps' names
    _, df_apps_names = td.extract_ids_list(df_android_ids)
    # Merge to replace app_id with app_name
    df_android_ids = td.merge_df(df_android_ids, df_apps_names)
    
    # Extractinc the most relevant information from JSON
    ext_shooter_download_one = td.extract_numbers_top_downloads(top_shooter_download_one_json)
    # Changing the response from JSON to DF
    df_shooter_download_one = pd.DataFrame(ext_shooter_download_one)
    
    # Get the shooter list ID to have the spent time
    apps_ids_shooter_list, df_apps_names = td.extract_ids_list(df_shooter_download_one)
    # Call the spent_time function
    df_total_time_spent_list = total_time_spent(token_api, first_day_one_month_ago, last_day_last_month, apps_ids_shooter_list, df_apps_names)
    
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_shooter_download_one)
    # Merge to replace app_id with app_name
    df_shooter_download_one = td.merge_df(df_shooter_download_one, df_apps_names)
    
    return df_shooter_download_one, df_total_time_spent_list, df_ios_ids, df_android_ids
    
# Use the IDs list of this request to get the total_time_spent
def top_apps_shooter_download_six_months(token_api, last_day_last_month, six_months_ago_str):
    # URL to get Top Shooter Apps by Download and Revenue using a range of date
    url = "https://api.sensortower.com/v1/unified/sales_report_estimates_comparison_attributes"

    # Define the parameters
    params = {
        'comparison_attribute': 'absolute',
        'time_range': 'month',
        'measure': 'units',
        'device_type': 'total',
        'category': '6014',
        'date': six_months_ago_str,
        'end_date': last_day_last_month,
        'regions': 'BR',
        'limit': '25',
        'custom_fields_filter_id': '600abc40241bc16eb8510760',
        'custom_tags_mode': 'include_unified_apps',
        'auth_token': token_api
    }
    
    # Getting the response
    response = requests.get(url, params=params)
    # Changing the response to JSON
    top_shooter_download_six_json = response.json()
    # Extractinc the most relevant information from JSON
    ext_shooter_download_six = td.extract_numbers_top_downloads(top_shooter_download_six_json)
    # Changing the response from JSON to DF
    df_shooter_download_six = pd.DataFrame(ext_shooter_download_six)
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_shooter_download_six)
    # Merge to replace app_id with app_name
    df_shooter_download_six = td.merge_df(df_shooter_download_six, df_apps_names)
    
    return df_shooter_download_six


def top_apps_shooter_dau_month(token_api, first_day_one_month_ago):
    # URL to get Top Shooter Apps by Active Users using one date
    url = "https://api.sensortower.com/v1/unified/top_and_trending/active_users"

    # Define the parameters
    params = {
        'comparison_attribute': 'absolute',
        'time_range': 'month',
        'measure': 'DAU',
        'category': '6014',
        'date': first_day_one_month_ago,
        'regions': 'BR',
        'limit': '25',
        'device_type': 'total',
        'custom_fields_filter_id': '600abc40241bc16eb8510760',
        'auth_token': token_api
    }
    
    # Getting the response
    response = requests.get(url, params=params)
    # Changing the response to JSON
    top_shooter_dau_one_json = response.json()
    # Extractinc the most relevant information from JSON
    ext_shooter_dau_one = td.extract_numbers_top_dau(top_shooter_dau_one_json)
    # Changing the response from JSON to DF
    df_shooter_dau_one = pd.DataFrame(ext_shooter_dau_one)
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_shooter_dau_one)
    # Merge to replace app_id with app_name
    df_shooter_dau_one = td.merge_df(df_shooter_dau_one, df_apps_names)
    
    return df_shooter_dau_one

def top_apps_shooter_dau_six_months(token_api, each_six_month_dates):
    # DF with all the results
    df_shooter_dau_six = pd.DataFrame()
    
    # 'For' in the list of dates
    for month in each_six_month_dates:
        
        # URL to get Top Shooter Apps by Active Users using a list of dates
        url = "https://api.sensortower.com/v1/unified/top_and_trending/active_users"

        # Define the parameters
        params = {
            'comparison_attribute': 'absolute',
            'time_range': 'month',
            'measure': 'DAU',
            'category': '6014',
            'date': month,
            'regions': 'BR',
            'limit': '25',
            'device_type': 'total',
            'custom_fields_filter_id': '600abc40241bc16eb8510760',
            'auth_token': token_api
        }
        
        # Getting the response
        response = requests.get(url, params=params)
        # Changing the response to JSON
        top_dau_one_json = response.json()
        # Extractinc the most relevant information from JSON
        month_info = td.extract_numbers_top_dau(top_dau_one_json)
        # Changing the response from JSON to DF
        df_month_info = pd.DataFrame(month_info)
        # Adding the month in the last collum of the DF
        df_month_info['month'] = month
        # Concat the information to the main DF
        df_shooter_dau_six = pd.concat([df_shooter_dau_six, df_month_info], axis=0)
    
    
    # Get the apps' names
    _, df_apps_names = td.extract_ids_list(df_shooter_dau_six)
    # Merge to replace app_id with app_name
    df_shooter_dau_six = td.merge_df(df_shooter_dau_six, df_apps_names)
        
    return df_shooter_dau_six

def total_time_spent(token_api, first_day_one_month_ago, last_day_last_month, apps_ids_shooter_list, df_apps_names):
    
    # Datetime object
    if isinstance(first_day_one_month_ago, str):
        first_day_one_month_ago = datetime.strptime(first_day_one_month_ago, '%Y-%m-%d')
    
    # Less one month
    first_day_one_month_ago = first_day_one_month_ago - relativedelta(months=1)
    first_day_one_month_ago = first_day_one_month_ago.strftime('%Y-%m-%d')

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
    
    return df_total_time_spent_list

def revivel_time_shooter_apps(token_api, df_ios_ids, df_android_ids):
    # List of devices to get the retention
    devices = ['ios', 'android']
    # Day today
    today = date.today().strftime('%Y-%m-%d')
    
    # 'For' in devices list
    for device in devices:
        # To know if is IOS or ANDROID
        if device == 'ios':
            # Separating the ids and changing them to INT
            df_ids = df_ios_ids['ios_id'].apply(lambda x: str(int(x)))
        else:
            # Separating the ids and changing them to STRING
            df_ids = df_android_ids['android_id'].astype(str)
        
        # Separating the ids using comma
        app_ids = ','.join(df_ids)
        
        # URL to get the IOS or ANDROID ids
        url = f'https://api.sensortower.com/v1/{device}/usage/retention'
        
        # Define the parameters
        params = {
            'app_ids': app_ids,
            'date_granularity': 'all_time',
            'start_date': today,
            'country': 'BR',
            'auth_token': token_api
        }

        # Getting the response
        response = requests.get(url, params=params)
        # Changing the response to JSON
        df_retention_json = response.json()
        
        # To know if is IOS or ANDROID
        if device == 'ios':
            # Extracting the retention based on the IDS
            df_ext = td.ext_retention_data(df_retention_json)
            # Filter to keep the first 10 days and then skip every 10 days
            df_filt = df_ext[(df_ext['day'] <= 6) | ((df_ext['day'] >= 7) & (df_ext['day'] % 7 == 0))]
            # Changing the name 'app_id' to 'ios_id'
            df_filt = df_filt.rename(columns={'app_id': 'ios_id'})
            # Merge to get the apps' names based on the ids
            df_retention_ios = pd.merge(df_ios_ids, df_filt, on='ios_id')
        else:
            # Extracting the retention based on the IDS
            df_ext = td.ext_retention_data(df_retention_json)
            # Filter to keep the first 10 days and then skip every 10 days
            df_filt = df_ext[(df_ext['day'] <= 6) | ((df_ext['day'] >= 7) & (df_ext['day'] % 7 == 0))]
            # Changing the name 'app_id' to 'android_id'
            df_filt = df_filt.rename(columns={'app_id': 'android_id'})
            # Merge to get the apps' names based on the ids
            df_retention_android = pd.merge(df_android_ids, df_filt, on='android_id')
        
    return df_retention_ios, df_retention_android