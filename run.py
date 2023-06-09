import pandas as pd
from monday_functions import * 
from sql_queries import *
from helpers import *
from algo import *

#update variables as needed
low_base, medium_base, high_base, emergency_base = 1, 2, 3, 200
rate = 0.0177
unit_value_factor = 0.5
buffer = 1.1 #adding a 10% buffer to costs of uncompleted projects
pending_statuses = ['Waiting for Pictures','Reserve','Waiting for Estimate', 'Vendor Needed','2nd Vendor Needed', 'Quote Requested','New Project', 'On Hold','Pending Schedule','Gathering Scope', 'FS Verifying','Estimate Walk Scheduled']
monday_data = Monday()


def execute_model(completed_df, open_df):

    completed_df = monday_data.fetch_completed()
    open_df = monday_data.fetch_open_items()

    open_df = categorize_projects(open_df, pending_statuses)
    # Split the df_open into in_process and pending DataFrames
    df_in_process = open_df[open_df['project_category'] == 'in_process']
    df_pending = open_df[open_df['project_category'] == 'pending']

    # Calculate costs for in process projects
    df_in_process = calculate_costs(df_in_process)
    df_in_process['cost'] = df_in_process['cost'] * 1.1  # Adding a 10% buffer
    completed_df = calculate_costs(completed_df)

    df_in_process['completed'] = False 
    completed_df['completed'] = True
    completed_combined = pd.concat([completed_df, df_in_process], ignore_index=True)

    # Calculate the remaining budget for each facility and each fund for R&M
    remaining_facility_df = remaining_facility(completed_combined)
    remaining_fund_df = remaining_fund(completed_combined)

    # rename columns before the merge
    remaining_facility_df = remaining_facility_df.rename(columns={"Final Cost": "spent_facility"})
    remaining_fund_df = remaining_fund_df.rename(columns={"Final Cost": "spent_fund"})
    remaining_fund_df = remaining_fund_df.drop(columns=['R&M budget'])

    facilities_df = remaining_facility_df.merge(remaining_fund_df, on='fund', how='left')

    open_df = calc_cost_effectiveness(df_pending, low_base, medium_base, high_base, emergency_base, rate, unit_value_factor)
    # Merge the facilities dataframe into the open projects dataframe to get the budget information for each project
    open_df = open_df.merge(facilities_df[['RD', 'fund', 'remaining_budget', 'remaining_fund_budget']], on='RD', how='left')

    # Rank the projects based on cost-effectiveness
    open_df = open_df.sort_values(by='cost_effectiveness', ascending=False)
 
    # Add columns for cumulative cost, and flags for surpassing facility and fund budgets
    open_df['cumulative_cost'] = open_df.groupby('RD')['cost'].cumsum()
    open_df['exceeds_facility_budget'] = open_df['cumulative_cost'] > open_df['remaining_budget']
    open_df['cumulative_fund_cost'] = open_df.groupby('fund')['cost'].cumsum()
    open_df['exceeds_fund_budget'] = open_df['cumulative_fund_cost'] > open_df['remaining_fund_budget']

    return open_df

