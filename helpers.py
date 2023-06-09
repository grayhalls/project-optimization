import pandas as pd
from sql_queries import * 
from dotenv import load_dotenv
import os 
import boto3 
import json  
from io import StringIO    
import pickle  


load_dotenv()
MASTER_ACCESS_KEY = os.getenv("MASTER_ACCESS_KEY")
MASTER_SECRET = os.getenv("MASTER_SECRET")

def s3_init():  
    
    # --- s3 client --- 
    s3 = boto3.client('s3', region_name = 'us-west-1', 
          aws_access_key_id=MASTER_ACCESS_KEY, 
          aws_secret_access_key=MASTER_SECRET) 
    return s3 
    
def grab_s3_file(f, bucket='capex-rm-optimization', idx_col=None):
    s3 = s3_init()
    data = s3.get_object(Bucket=bucket, Key=f)['Body'].read().decode('utf-8') 
    if idx_col is None:
        data = pd.read_csv(StringIO(data)) 
    else:
        data = pd.read_csv(StringIO(data), index_col=idx_col)

    return data 

    
def grab_budgets():
    budget = pd.read_csv('budgets_2023.csv')
    # budget = grab_s3_file('budgets_2023.csv') #switch later

    facilities = run_sql_query(facilities_sql)
    facility_data = facilities[['rd', 'fund', 'fs']]

    merge = pd.merge(facility_data, budget, how="left", left_on = ['rd'], right_on = 'RD')
    merge = merge.drop(columns=['RD'])
    merge = merge.rename(columns={'rd': 'RD'})
    # merge[['R&M budget', 'R&M monthly']] = merge[['R&M budget', 'R&M monthly']].apply(pd.to_numeric)
    
    return merge 

def grab_pl():
    pl = pd.read_csv('p&l_4_2023.csv')
    # pl = grab_s3_file('p&l_4_2023.csv') #switch later
    cols_to_keep = ['RD']
    cols_to_melt = pl.columns.difference(cols_to_keep)

    new_pl = pd.melt(pl, id_vars=cols_to_keep, value_vars=cols_to_melt, var_name="Date", value_name="R&M")

    return new_pl

def remaining_facility(completed, capex=False):
    budget = grab_budgets()

    completed['Final Cost'] = pd.to_numeric(completed['Final Cost'], errors='coerce').fillna(0)
    if capex==False:
        completed = completed[completed['Final Cost'] < 2500]
    else:
        completed = completed[completed['Final Cost'] >= 2500]

    spent = completed[['RD', 'Final Cost']]
    spent = spent.groupby('RD', as_index=False).sum()

    merge = pd.merge(budget, spent, how='left', on='RD') 
    
    merge['Final Cost'] = merge['Final Cost'].fillna(0)
    merge['remaining_budget'] = merge['R&M budget'] - merge['Final Cost']

    return merge 

def remaining_fund(completed, capex=False):
    remaining = remaining_facility(completed, capex)
    remaining = remaining[['fund', 'R&M budget', 'Final Cost', 'remaining_budget']]
    remaining = remaining.rename(columns = {'remaining_budget':'remaining_fund_budget'})
    fund_leftovers = remaining.groupby('fund', as_index=False).sum()

    return fund_leftovers

def categorize_projects(df, pending_statuses):
    df['project_category'] = df['Status'].apply(lambda x: 'pending' if x in pending_statuses else 'in_process')
    return df
