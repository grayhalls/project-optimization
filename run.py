import pandas as pd
from monday_functions import * 
from sql_queries import *
from helpers import *
from algo import *

#update variables as needed
low_base, medium_base, high_base, emergency_base = 1, 2, 3, 200
rate = 0.002529
unit_value_factor = 0.5
buffer = 1.1 #adding a 10% buffer to costs of uncompleted projects
pending_statuses = ['Waiting for Pictures','Reserve','Waiting for Estimate', 'Vendor Needed','2nd Vendor Needed', 'Quote Requested','New Project', 'On Hold','Pending Schedule','Gathering Scope', 'FS Verifying','Estimate Walk Scheduled']
monday_data = Monday()

# group ids
in_process_group = 'topics'
error_group = 'new_group'
in_queue_group = 'group_title'

facilities = run_sql_query(facilities_sql)

def execute_model():

    completed_df = monday_data.fetch_completed()
    open_df = monday_data.fetch_open_items()
    # print('fetched monday items')
    # Categorize the projects
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
    
    open_df = open_df.reset_index(drop=True)
    return open_df, df_in_process

#still in the works
def process_and_send_items(df_in_process, open_df, new_board_id):
    df_in_process = df_in_process.drop(columns=['region'])
    df_in_process = df_in_process.merge(facilities, how="left", left_on = 'RD', right_on = 'rd')
    existing_items = monday_data.fetch_items_by_board_id(new_board_id)
    existing_items = existing_items['data']['boards'][0]['items']

    # Iterate over the rows in df_in_process and open_df
    for df in [df_in_process, open_df]:
        for _, row in df.iterrows():
            # Iterate over the existing_items
            for item in existing_items:
                existing_item_dict = {column['id']: column['text'] for column in item['column_values']}
                
                # Extract the relevant keys from the existing_item_dict
                existing_dict_relevant_keys = {key: existing_item_dict[key] for key in row.keys() if key in existing_item_dict}
                # Now you can compare the dictionaries
                if row.to_dict() != existing_dict_relevant_keys:
                    # If the column values have changed, delete the item from the board
                    monday_data.delete_item(item['id'])


            if df is df_in_process:
                # Send the item to the 'topics' group
                monday_data.create_items_from_df(row.to_frame().T, in_process_group)
            else:
                # Check if the cost or cost_effectiveness is missing or not a number
                if pd.isna(row['cost']) or row['cost'] == 0 or pd.isna(row['cost_effectiveness']):
                    # Send the item to the 'new_group'
                    monday_data.create_items_from_df(row.to_frame().T, error_group)
                else:
                    # Otherwise, send the item to the 'group_title'
                    monday_data.create_items_from_df(row.to_frame().T, in_queue_group)

