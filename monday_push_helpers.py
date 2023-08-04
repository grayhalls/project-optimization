import pandas as pd
from monday_functions import Monday
from sql_queries import run_sql_query, facilities_sql 
from helpers import remaining_facility, remaining_fund, categorize_projects
from algo import calculate_costs, calc_cost_effectiveness
import os
from dotenv import load_dotenv

load_dotenv()
# update variables as needed - scaling variables in algo.py
buffer = 1.1 # adding a 10% buffer to costs of uncompleted projects
pending_statuses = ['Waiting for Estimate', 'Vendor Needed','Quote Requested','New Project', 'On Hold','Gathering Scope', 'Locating Vendors']
# checks the values and updates changes for these columns
columns_to_check = ['numbers', 'numbers6', 'numbers0', 'numbers_1', 'status19', 'status9', 'numbers05', 'numbers_15']
monday_data = Monday()
new_board_id = os.getenv('new_board_id')
board_id = os.getenv('board_id')

# group ids
in_process_group = 'topics'
error_group = 'new_group'
eligible_group = 'group_title'
completed_group = 'new_group51572'

facilities = run_sql_query(facilities_sql)

def calc_and_sort():
    print("Fetching project board...takes up to 3 mins.")
    completed_df = monday_data.fetch_items(['Complete'])
    open_df = monday_data.fetch_items(['North', 'South', 'Central'], all_groups=['North', 'South', 'Central'])

    open_df = categorize_projects(open_df, pending_statuses)

    # Split the df_open into in_process and pending DataFrames
    df_in_process = open_df[open_df['project_category'] == 'in_process']
    df_pending = open_df[open_df['project_category'] == 'pending']

    # Calculate costs for in process projects including a 10% buffer for projects not completed
    df_in_process = calculate_costs(df_in_process, buffer) 
    completed_df = calculate_costs(completed_df, 1)

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
    df['numbers1'] = df['numbers1'].astype(float)
    df['text2'] = df['text2'].astype(str)
    df['link'] = df.apply(lambda row: {"url": f"https://reddotstorage2.monday.com/boards/{board_id}/pulses/{row['text2']}", "text": row['name']}, axis=1)

    existing_column_names = monday_data.fetch_column_names(new_board_id)

    # Find the columns to drop
    columns_to_drop = set(df.columns) - set(existing_column_names)

    # Drop the columns
    df = df.drop(columns=columns_to_drop)
    return df

def preprocessing(df_in_process, open_df, completed):
    print('preprocessing...')
    df_in_process = preprocess_df(df_in_process)
    open_df = preprocess_df(open_df)
    completed = preprocess_df(completed)
    return df_in_process, open_df, completed 

def find_existing_rows():
    print('fetching data from Ranking board...')
    existing_items = monday_data.fetch_items_by_board_id(new_board_id)
    existing_items = existing_items['data']['boards'][0]['items']
    output = [
        {
            "id": next((col["text"] for col in item["column_values"] if col["id"] == "text2"), None),
            "group": item['group']['title'],
            "status19": next((col["text"] for col in item["column_values"] if col["id"] == "status19"), None),
            "item_id": item['id'],
            "numbers": next((col["text"] for col in item["column_values"] if col["id"] == "numbers"), None),
            "numbers6": next((col["text"] for col in item["column_values"] if col["id"] == "numbers6"), None),
            "numbers0": next((col["text"] for col in item["column_values"] if col["id"] == "numbers0"), None),
            "numbers_1": next((col["text"] for col in item["column_values"] if col["id"] == "numbers_1"), None),
            "status9": next((col["text"] for col in item["column_values"] if col["id"] == "status9"), None),
            "numbers1": next((col["text"] for col in item["column_values"] if col["id"] == "numbers1"), None),
            "numbers05": next((col["text"] for col in item["column_values"] if col["id"] == "numbers05"), None),
            "numbers_15": next((col["text"] for col in item["column_values"] if col["id"] == "numbers_15"), None)
        } 
        for item in existing_items
        ]

    df = pd.DataFrame(output)

    # Replace blank strings with 0
    df.replace('', 0, inplace=True)
    
    # Convert to the correct data types
    df['numbers'] = df['numbers'].astype(float)
    df['numbers6'] = df['numbers6'].astype(float)
    df['numbers0'] = df['numbers0'].astype(int)
    df['numbers_1'] = df['numbers_1'].astype(int)
    df['numbers1'] = df['numbers1'].astype(float)
    df['numbers05'] = df['numbers05'].astype(int)
    df['numbers_15'] = df['numbers_15'].astype(int)
    
    return df


def move_between_groups(completed_df, in_process_df, open_df, existing_items):
    print('moving rows to correct groups...')
    for index, row in existing_items.iterrows():
        project_id = str(row['id'])
        item_group = row['group']
        item_id = row['item_id']
        
        # Check if item_id is not None
        if project_id is not None:
        
            # Check if the item is in 'completed_df' but not marked as 'Complete' on the board
            if item_group != 'Completed' and project_id in completed_df['text2'].values:
                monday_data.move_items_between_groups(item_id, completed_group)
                print(f"{project_id} moved from {item_group} to Completed.")
            # Do similar checks for 'In Process'
            elif item_group != 'In Process' and project_id in in_process_df['text2'].values:
                monday_data.move_items_between_groups(item_id, in_process_group)
                print(f"{project_id} moved from {item_group} to In Process.")
            # Check for eligible items that have inputs that would cause an error and move to error group
            elif item_group == 'Eligible' and (row['numbers'] == "" or row['numbers'] == 0 or row['numbers6']=="" or row['status9'] == 'Escalation'\
                or pd.isna(row['numbers']) or pd.isna(row['numbers6'])):
                monday_data.move_items_between_groups(item_id, error_group)
                print(f"{project_id} moved from {item_group} to Errors.")
            # Check for items not in eligible group but are currently available and ranked
            elif item_group != 'Eligible' and project_id in open_df['text2'].values:
                monday_data.move_items_between_groups(item_id, eligible_group)
                print(f"{project_id} moved from {item_group} to Eligible.")  # replace group IDs with actual IDs

def create_missing_items(completed_df, in_process_df, open_df, existing_items):
    print('adding new projects...')
    existing_ids = set(existing_items['id'])

    for df, group in [(completed_df, completed_group), 
                      (in_process_df, in_process_group), 
                      (open_df, eligible_group)]:
        # Go through each item in the dataframe
        for index, row in df.iterrows():
            item_id = row['text2']

            # If the item id is not in the list of existing ids, create a new item
            if item_id not in existing_ids:
                print(f'adding {item_id}')
                monday_data.create_items_from_df(row, group, error_group)

def update_existing_data(preprocessed_df, existing_items):
    count =1
    # Loop through each item in preprocessed_df
    for index, row in preprocessed_df.iterrows():
        matching_items = [item for item in existing_items if item['id'] == row['text2']]
        if matching_items:
            matching_item = matching_items[0]
            for column in columns_to_check:
                # convert string values to their appropriate type before comparison
                if matching_item[column].isdigit():
                    # if it's a string representation of an integer
                    existing_value = int(matching_item[column])
                else:
                    try:
                        # try converting to a float (will fail if the string is not a number)
                        existing_value = float(matching_item[column])
                    except ValueError:
                        # if it's not a number, keep it as a string
                        existing_value = matching_item[column]

                if row[column] != existing_value:
                    print(column)
                    print(f"values changed: {count}")
                    if pd.isna(row[column]):
                        continue
                    monday_data.change_item_value(new_board_id, matching_item['item_id'], column, row[column])
                    count += 1


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
