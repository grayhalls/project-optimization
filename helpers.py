import pandas as pd
from dotenv import load_dotenv
import os 
import boto3 
import numpy as np
import json  
from io import StringIO    
from monday_functions import Monday
from sql_queries import run_sql_query, units_sql, tasks_to_units

capex_threshold = 2500
monday_data = Monday()
load_dotenv()
MASTER_ACCESS_KEY = os.getenv("MASTER_ACCESS_KEY")
MASTER_SECRET = os.getenv("MASTER_SECRET")
board_id = os.getenv('board_id')

def s3_init():  
    
    # --- s3 client --- 
    s3 = boto3.client('s3', region_name = 'us-west-1', 
          aws_access_key_id=MASTER_ACCESS_KEY, 
          aws_secret_access_key=MASTER_SECRET) 
    return s3 
    
def grab_s3_file(f, bucket, idx_col=None, is_json=False):
    s3 = s3_init()
    data = s3.get_object(Bucket=bucket, Key=f)['Body'].read().decode('utf-8') 
    
    # Check if the file is a JSON
    if is_json:
        return json.loads(data)  # Return the parsed JSON data as a dictionary
    
    # If the file is a CSV
    if idx_col is None:
        data = pd.read_csv(StringIO(data)) 
    else:
        data = pd.read_csv(StringIO(data), index_col=idx_col)

    return data 

def grab_unit_values():
    json_data=grab_s3_file('unit-value/last_update.json','rev-mgt',is_json=True)
    last_upload_date = json_data["last_upload"]
    csv_file_name = f"unit-value/{last_upload_date}.csv"
    data = grab_s3_file(csv_file_name, 'rev-mgt')
    data = data.rename(columns={'site_code':'RD'})
    data= data[['RD','width', 'length', 'unit_type','replace_value']]
    
    return data

def add_values_to_projects():
    values= grab_unit_values()
    assert not values.empty, "values DataFrame is empty."
    
    unit_projects = monday_data.generate_subitem_df(board_id, groups=['South', 'North', 'Central'], columns=["dropdown3", "status", "text4"])
    assert not unit_projects.empty, "unit_projects DataFrame is empty."
    unit_projects['task_id'] = pd.to_numeric(unit_projects['link'].str.split('/').str[-1], errors='coerce').fillna(0).astype('int64')
    
    units_to_tasks = run_sql_query(tasks_to_units)
    units_to_tasks = units_to_tasks.rename(columns={'site_code':'RD'})
    unit_projects = unit_projects.merge(units_to_tasks, how='left', on=['task_id'])

    units = run_sql_query(units_sql)
    assert not units.empty, "units DataFrame is empty."
    
    units = units.rename(columns={'rd':'RD'})
    unit_projects = unit_projects.merge(units, how="left", on=['RD', 'unit_number'])
    unit_value_data = unit_projects.merge(values, how="left", on=['RD','width','length','unit_type'])

    return unit_value_data


def grab_budgets(facilities):
    budget = pd.read_csv('budgets_2023.csv')
    # budget = grab_s3_file('budgets_2023.csv', bucket ='capex-rm-optimization') #switch later

    facility_data = facilities[['rd', 'fund', 'fs']]

    merge = pd.merge(facility_data, budget, how="left", left_on = ['rd'], right_on = 'RD')
    merge = merge.drop(columns=['RD'])
    merge = merge.rename(columns={'rd': 'RD'})
    # merge[['R&M budget', 'R&M monthly']] = merge[['R&M budget', 'R&M monthly']].apply(pd.to_numeric)
    
    return merge 

def remaining_facility(completed, facilities):
    budget = grab_budgets(facilities)
    budget = budget.loc[budget.index.repeat(2)].reset_index(drop=True)
    budget['Capex'] = [True, False] * (len(budget) // 2)

    budget['budget'] = np.where(budget['Capex'], budget['recast_capex'], budget['R&M budget'])
    budget = budget[['RD', 'fund', 'Capex', 'budget']]

    completed['Final Cost'] = pd.to_numeric(completed['Final Cost'], errors='coerce').fillna(0)
    # determining if a project is capex based on $2500 threshold
    completed['Capex'] = completed['Final Cost'] >= capex_threshold

    spent = completed[['RD', 'Final Cost', 'Capex']]
    spent = spent.groupby(['RD', 'Capex'], as_index=False).sum()

    merge = budget.merge(spent, how='left', on=['RD', 'Capex']) 

    merge['Final Cost'] = merge['Final Cost'].fillna(0)
    
    merge['remaining_budget'] = merge['budget'] - merge['Final Cost']

    merge = merge.rename(columns={"Final Cost": "spent_facility"})

    return merge 

def remaining_fund(remaining_by_facility):
    remaining_by_facility = remaining_by_facility[['fund', 'Capex', 'budget', 'spent_facility']]
    fund_leftovers = remaining_by_facility.groupby(['fund', 'Capex'], as_index=False).sum()
    fund_leftovers['remaining_fund_budget'] = fund_leftovers['budget'] - fund_leftovers['spent_facility']
    fund_leftovers = fund_leftovers.rename(columns={"spent_facility": "spent_fund"})

    return fund_leftovers

def categorize_projects(df, pending_statuses):
    df['project_category'] = df['Status'].apply(lambda x: 'pending' if x in pending_statuses else 'in_process')
    return df

def 

