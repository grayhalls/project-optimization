import pandas as pd
from monday_functions import * 
from sql_queries import *
from helpers import *
from algo import *
import os
from dotenv import load_dotenv

load_dotenv()
# update variables as needed - scaling variables in algo.py
buffer = 1.1 # adding a 10% buffer to costs of uncompleted projects
pending_statuses = ['Waiting for Estimate', 'Vendor Needed','Quote Requested','New Project', 'On Hold','Gathering Scope', 'Locating Vendors']
monday_data = Monday()
new_board_id = os.getenv('new_board_id')

# group ids
in_process_group = 'topics'
error_group = 'new_group'
in_queue_group = 'group_title'
completed_group = 'new_group51572'

facilities = run_sql_query(facilities_sql)

def calc_and_sort():

    completed_df = monday_data.fetch_completed()
    open_df = monday_data.fetch_open_items()

    # Categorize the projects
    open_df = categorize_projects(open_df, pending_statuses)
    # Split the df_open into in_process and pending DataFrames
    df_in_process = open_df[open_df['project_category'] == 'in_process']
    df_pending = open_df[open_df['project_category'] == 'pending']

    # Calculate costs for in process projects
    df_in_process = calculate_costs(df_in_process)
    df_in_process['cost'] = df_in_process['cost'] * buffer  
    completed_df = calculate_costs(completed_df)

    df_in_process['completed'] = False 
    completed_df['completed'] = True
    completed_combined = pd.concat([completed_df, df_in_process], ignore_index=True)

    # Calculate the remaining budget for each facility and each fund for R&M
    remaining_facility_df = remaining_facility(completed_combined,facilities)
    remaining_fund_df = remaining_fund(completed_combined, facilities)

    # rename columns before the merge
    remaining_facility_df = remaining_facility_df.rename(columns={"Final Cost": "spent_facility"})
    remaining_fund_df = remaining_fund_df.rename(columns={"Final Cost": "spent_fund"})
    remaining_fund_df = remaining_fund_df.drop(columns=['R&M budget'])

    facilities_df = remaining_facility_df.merge(remaining_fund_df, on='fund', how='left')

    df_in_process = calc_cost_effectiveness(df_in_process)
    open_df = calc_cost_effectiveness(df_pending)
    # Merge the facilities dataframe into the open projects dataframe to get the budget information for each project
    open_df = open_df.merge(facilities_df[['RD', 'fund', 'remaining_budget', 'remaining_fund_budget']], on='RD', how='left')

    # Rank the projects based on cost-effectiveness
    open_df = open_df.sort_values(by='cost_effectiveness', ascending=False)
 
    # Add columns for cumulative cost, and flags for surpassing facility and fund budgets
    open_df['cumulative_cost'] = open_df.groupby('RD')['cost'].cumsum().astype(int)
    open_df['cumulative_fund_cost'] = open_df.groupby('fund')['cost'].cumsum()
    open_df['cumulative_fund_cost'] =open_df['cumulative_fund_cost'].fillna(0).astype(int)
    open_df['remaining_budget'] =open_df['remaining_budget'].fillna(0).astype(int)
    open_df['remaining_fund_budget'] =open_df['remaining_fund_budget'].fillna(0).astype(int)

    open_df['remaining_budget_by_rd'] = (open_df['remaining_budget'] - open_df['cumulative_cost']).fillna(0).astype(int)
    open_df['remaining_budget_by_fund'] = (open_df['remaining_fund_budget'] - open_df['cumulative_fund_cost']).fillna(0).astype(int)
    open_df = open_df.rename(columns = {'remaining_budget': 'current_rd_budget_status', 'remaining_fund_budget':'current_fund_budget_status'})
    
    open_df['exceeds_facility_budget'] = open_df['remaining_budget_by_rd'] < 0
    open_df['exceeds_fund_budget'] = open_df['remaining_budget_by_fund'] < 0
    
    open_df = open_df.reset_index(drop=True)
    return open_df, df_in_process, completed_df

def preprocess_df(df):
    # Define column mappings and their default values
    column_mappings = {
        'RD': ('rd', ''),
        'id': ('text2', ''),
        'cost': ('numbers', 0),
        'cost_effectiveness': ('numbers6', 0),
        'current_rd_budget_status': ('numbers0', 0),
        'current_fund_budget_status': ('numbers_1', 0),
        'exceeds_facility_budget': ('exceeds_rd_budget5', 'N/A'),
        'exceeds_fund_budget': ('exceeds_fund_budget2', 'N/A'),
        'Status': ('status19', ''),
        'Priority': ('status9', ''),
        'priority_value': ('numbers1',0),
        'region': ('region5',''),
        'fund': ('text',''),
        'remaining_budget_by_rd': ('numbers05',0),
        'remaining_budget_by_fund': ('numbers_15',0),
        'PC': ('text8', ''),
        'item_name': ('item_name', '')
    }

    for original_column, (new_column, default_value) in column_mappings.items():
        # If the column exists, rename it
        if original_column in df.columns:
            df = df.rename(columns={original_column: new_column})
        # If the column doesn't exist, create it with default values
        else:
            df[new_column] = default_value

    # Ensure both 'rd' and 'item_name' exist and are strings before concatenation
    if 'rd' in df.columns and 'item_name' in df.columns:
        df['rd'] = df['rd'].fillna('')
        df['item_name'].fillna('', inplace=True)
        df['rd'] = df['rd'].astype(str)
        df['item_name'] = df['item_name'].astype(str)
        df['name'] = df['rd'] + " - " + df['item_name']
    else:
        df['name'] = ''

    df['text2'] = df['text2'].astype(str)
    df['link'] = df.apply(lambda row: {"url": f"https://reddotstorage2.monday.com/boards/{board_id}/pulses/{row['text2']}", "text": row['name']}, axis=1)

    existing_column_names = monday_data.fetch_column_names(new_board_id)

    # Find the columns to drop
    columns_to_drop = set(df.columns) - set(existing_column_names)

    # Drop the columns
    df = df.drop(columns=columns_to_drop)
    return df

def preprocessing(df_in_process, open_df, completed):
    df_in_process = preprocess_df(df_in_process)
    open_df = preprocess_df(open_df)
    completed = preprocess_df(completed)
    return df_in_process, open_df, completed 

def find_existing_rows():
    existing_items = monday_data.fetch_items_by_board_id(new_board_id)
    existing_items = existing_items['data']['boards'][0]['items']
    output = [
    {
        "id": next((col["text"] for col in item["column_values"] if col["id"] == "text2"), None),
        "group": item['group']['title'],
        "status": next((col["text"] for col in item["column_values"] if col["id"] == "status19"), None),
        "item_id": item['id']
    } 
    for item in existing_items

    ]

    return output

def move_between_groups(completed_df, in_process_df, open_df):
    # output should be a list of dictionaries with 'id', 'group', and 'status'
    output = find_existing_rows()
    for item in output:
        project_id = str(item['id'])
        item_group = item['group']
        item_id = item['item_id']
        
        # Check if item_id is not None
        if project_id is not None:
        
        # Check if the item is in 'completed_df' but not marked as 'Complete' on the board
            if item_group != 'Complete' and project_id in completed_df['text2'].values:
                # Delete the item from its current group
                monday_data.delete_item(item_id)
                
                # Recreate the item in the 'Complete' group
                # You can extract the row from 'completed_df' corresponding to 'item_id'
                row = completed_df[completed_df['text2'] == project_id].iloc[0]
                monday_data.create_items_from_df(row, completed_group, error_group)  # replace group IDs with actual IDs

            # Do similar checks for 'In Process' and 'In Queue' groups
            elif item_group != 'In Process' and project_id in in_process_df['text2'].values:
                monday_data.delete_item(item_id)
                row = in_process_df[in_process_df['text2'] == project_id].iloc[0]
                monday_data.create_items_from_df(row, in_process_group, error_group)  # replace group IDs with actual IDs
                
            elif item_group != 'In Queue' and project_id in open_df['text2'].values:
                monday_data.delete_item(item_id)
                row = open_df[open_df['text2'] == project_id].iloc[0]
                monday_data.create_items_from_df(row, in_queue_group, error_group)  # replace group IDs with actual IDs

def create_missing_items(completed_df, in_process_df, open_df):
    # Get a list of existing item ids
    existing_items = find_existing_rows()
    existing_ids = [str(item['id']) for item in existing_items]

    for df, group in [(completed_df, completed_group), 
                      (in_process_df, in_process_group), 
                      (open_df, in_queue_group)]:
        
        # Go through each item in the dataframe
        for index, row in df.iterrows():
            item_id = str(row['text2'])

            # If the item id is not in the list of existing ids, create a new item
            if item_id not in existing_ids:
                monday_data.create_items_from_df(row, group, error_group)

#still in the works
# def process_and_send_items(df_in_process, open_df, new_board_id):
#     df_in_process = df_in_process.drop(columns=['region'])
#     df_in_process = df_in_process.merge(facilities, how="left", left_on = 'RD', right_on = 'rd')
#     existing_items = monday_data.fetch_items_by_board_id(new_board_id)
#     existing_items = existing_items['data']['boards'][0]['items']

#     # Iterate over the rows in df_in_process and open_df
#     for df in [df_in_process, open_df]:
#         for _, row in df.iterrows():
#             # Iterate over the existing_items
#             for item in existing_items:
#                 existing_item_dict = {column['id']: column['text'] for column in item['column_values']}
                
#                 # Extract the relevant keys from the existing_item_dict
#                 existing_dict_relevant_keys = {key: existing_item_dict[key] for key in row.keys() if key in existing_item_dict}
#                 # Now you can compare the dictionaries
#                 if row.to_dict() != existing_dict_relevant_keys:
#                     # If the column values have changed, delete the item from the board
#                     monday_data.delete_item(item['id'])


#             if df is df_in_process:
#                 # Send the item to the 'topics' group
#                 monday_data.create_items_from_df(row.to_frame().T, in_process_group)
#             else:
#                 # Check if the cost or cost_effectiveness is missing or not a number
#                 if pd.isna(row['cost']) or row['cost'] == 0 or pd.isna(row['cost_effectiveness']):
#                     # Send the item to the 'new_group'
#                     monday_data.create_items_from_df(row.to_frame().T, error_group)
#                 else:
#                     # Otherwise, send the item to the 'group_title'
#                     monday_data.create_items_from_df(row.to_frame().T, in_queue_group)
