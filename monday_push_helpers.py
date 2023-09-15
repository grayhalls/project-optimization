import pandas as pd
from monday_functions import Monday
from sql_queries import run_sql_query, facilities_sql 
from helpers import remaining_facility, remaining_fund, categorize_projects
from algo import calculate_costs, calc_cost_effectiveness
import os
import json
from dotenv import load_dotenv

load_dotenv()
# update variables as needed - scaling variables in algo.py
buffer = 1.1 # adding a 10% buffer to costs of uncompleted projects
capex_threshold = 2500
pending_statuses = ['Waiting for Estimate', 'Vendor Needed','Quote Requested','New Project', 'On Hold','Gathering Scope', 'Locating Vendors']
pending_statuses_opc = ['New Project', 'Vendor Needed', 'Waiting for Estimate/Proof', 'On Hold', 'Denied']
# checks the values and updates changes for these columns
columns_to_check = ['numbers', 'numbers6', 'status19', 'status9', 'numbers05', 'numbers_15', 'numbers1', 'numbers7']
monday_data = Monday()
new_board_id = os.getenv('new_board_id')
cipc_board_id = os.getenv('board_id')
opc_board_id = os.getenv('opc_board_id')
hauling_board_id = os.getenv('hauling_board_id')

# group ids
in_process_group = 'topics'
error_group = 'new_group'
eligible_group = 'group_title'
completed_group = 'new_group51572'
ineligible_group ='new_group40156'

column_mappings = {
        'RD': ('rd', ''),
        'id': ('text2', ''),
        'cost': ('numbers', 0),
        'rank': ('numbers6', 0),
        'current_rd_budget_status': ('numbers0', 0),
        'current_fund_budget_status': ('numbers_1', 0),
        'Status': ('status19', ''),
        'Priority': ('status9', ''),
        'priority_value': ('numbers1',0),
        'region': ('region5',''),
        'fund': ('text',''),
        'remaining_budget_by_rd': ('numbers05',0),
        'remaining_budget_by_fund': ('numbers_15',0),
        'PC': ('text8', ''),
        'item_name': ('item_name', ''),
        'avg_unit_value':('numbers7',''),
        'team':('dropdown','')
    }

status_mapping = {
    'High': {"index": 11},
    'Medium': {"index": 1},
    'Low': {"index": 0},
    'EMERGENCY': {"index": 2},
    'Escalation': {"index": 3},
    '':{"index":0}
}
opc_budget_mapping = {
    'Landscape': 'Landscaping and Snow Removal',
    'Signage': 'Supplies',
    'Plumbing': 'R&M',
    'Paint': 'R&M',
    'Pest control': 'Cleaning and Sanitation',
    'Hauling': 'Cleaning and Sanitation'
}

facilities = run_sql_query(facilities_sql)

def fetch_data():
    print("Fetching project board...takes up to 3 mins.")
    completed = monday_data.fetch_items(board = cipc_board_id, group_titles=['Complete'])
    open_data = monday_data.fetch_items(board = cipc_board_id, group_titles=['North', 'South', 'Central'], all_groups=['North', 'South', 'Central'])
    
    # Filter rows with 'status' == 'Compete' from open_data
    to_move = open_data[open_data['Status'] == 'Compete']
    
    # Append these rows to completed dataframe
    completed = pd.concat([completed, to_move], ignore_index=True)
    
    # Drop these rows from open_data dataframe
    open_data = open_data[open_data['Status'] != 'Compete']
    
    print('fetched CIPC Boards')
    opc_open = monday_data.fetch_items(board = opc_board_id, group_titles=['North', 'South', 'Central'], all_groups=['North', 'South', 'Central'])
    opc_completed = monday_data.fetch_items(board = opc_board_id, group_titles=['Complete'])
    # hauling = monday_data.fetch_items(hauling_board_id, group_titles=['Unit Haul List'],all_groups=['Vehicle Towing','Unit Haul List'])
    print('fetched OPC Boards')
    return completed, open_data, opc_open, opc_completed #, hauling


def split_data(df):
    df_in_process = df[df['project_category'] == 'in_process']
    df_pending = df[df['project_category'] == 'pending']
    print('split')
    return df_in_process, df_pending

def calculate_combined_costs(df_in_process, completed_df, buffer): #buffer determined at top
    df_in_process = calculate_costs(df_in_process, buffer)
    # completed_df = calculate_costs(completed_df, 1)
    df_in_process['completed'] = False 
    completed_df['completed'] = True
    print('calculated costs')
    return pd.concat([completed_df, df_in_process], ignore_index=True)

def gathered_budgets(completed_combined, facilities):
    remaining_facility_df = remaining_facility(completed_combined, facilities)
    remaining_fund_df = remaining_fund(remaining_facility_df)
    facilities_df = remaining_facility_df.merge(remaining_fund_df, on=['fund','Capex'], how='left')
    print('fetched budgets')
    return facilities_df.rename(columns={
        'remaining_budget_x': 'remaining_facility_budget',
        'budget_x': 'facility_budget',
        'budget_y': 'fund_budget'
    })

def process_dataframes(df, facilities_df):
    df = calc_cost_effectiveness(df)
    df['Capex'] = df['cost'] > capex_threshold
    df = df.merge(facilities_df[['RD','Capex', 'fund', 'remaining_budget', 'remaining_fund_budget']], on=['RD','Capex'], how='left')
    df = df.sort_values(by='cost_effectiveness', ascending=False)
    print('processed cost effectiveness')
    return df

def add_cumulative_budget_columns(df):
    df['cumulative_cost'] = df.groupby('RD')['cost'].cumsum().astype(int)
    df['cumulative_fund_cost'] = df.groupby('fund')['cost'].cumsum().fillna(0).astype(int)
    df['remaining_budget'] = df['remaining_budget'].fillna(0).astype(int)
    df['remaining_fund_budget'] = df['remaining_fund_budget'].fillna(0).astype(int)
    df['remaining_budget_by_rd'] = (df['remaining_budget'] - df['cumulative_cost']).fillna(0).astype(int)
    df['remaining_budget_by_fund'] = (df['remaining_fund_budget'] - df['cumulative_fund_cost']).fillna(0).astype(int)
    return df.rename(columns={
        'remaining_budget': 'current_rd_budget_status',
        'remaining_fund_budget': 'current_fund_budget_status'
    })

def calc_and_sort():
    completed_df, open_df, opc_open, opc_completed = fetch_data()
    assert not completed_df.empty, "The completed_df dataframe is empty."
    assert not open_df.empty, "The open_df dataframe is empty."
    
    open_df = categorize_projects(open_df, pending_statuses)
    opc_open = categorize_projects(opc_open, pending_statuses_opc)
    
    opc_open['Line Item'] = opc_open['Project Type'].map(opc_budget_mapping).fillna(opc_open['Project Type'])
    opc_completed['Line Item'] = opc_completed['Project Type'].map(opc_budget_mapping).fillna(opc_completed['Project Type'])
    opc_open = opc_open.rename(columns = {'People': 'PC'})
    
    df_in_process, df_pending = split_data(open_df)
    opc_in_process, opc_pending = split_data(opc_open)
    assert (df_in_process['project_category'] == 'in_process').all(), "The df_in_process contains incorrect data."
    assert (df_pending['project_category'] == 'pending').all(), "The df_pending contains incorrect data."

    completed_df = calculate_costs(completed_df, buffer=1)
    opc_comp_costs =calculate_costs(opc_completed, buffer=1)
    completed_combined = calculate_combined_costs(df_in_process, completed_df, buffer)
    assert len(completed_combined) == len(df_in_process) + len(completed_df), "Data mismatch in combined dataframe."
    
    completed_budgets = gathered_budgets(completed_df, facilities)
    expected_budgets = gathered_budgets(completed_combined, facilities) 
    expected_columns = ['RD', 'fund', 'Capex', 'facility_budget', 'spent_facility','remaining_budget', 'fund_budget', 'spent_fund','remaining_fund_budget']
    assert all(column in expected_budgets.columns for column in expected_columns), "expected_budget is missing expected columns."
    
    opc_pending = calc_cost_effectiveness(opc_pending)
    opc_in_process = calc_cost_effectiveness(opc_in_process)
    cipc_in_process = calc_cost_effectiveness(df_in_process)
    cipc_pending = calc_cost_effectiveness(df_pending)

    #concat pending and in_process dfs separately
    df_in_process = pd.concat([cipc_in_process, opc_in_process])

    df_in_process = process_dataframes(df_in_process, completed_budgets)
    open_df = process_dataframes(df_pending, expected_budgets)

    df_in_process = add_cumulative_budget_columns(df_in_process)
    open_df = add_cumulative_budget_columns(open_df)
    budget_columns = ['cumulative_cost', 'cumulative_fund_cost', 'remaining_budget_by_rd', 'remaining_budget_by_fund']
    assert all(column in open_df.columns for column in budget_columns), "open_df is missing budget columns after calculations."

    open_df['exceeds_facility_budget'] = open_df['remaining_budget_by_rd'] < 0
    open_df['exceeds_fund_budget'] = open_df['remaining_budget_by_fund'] < 0
    open_df = open_df.reset_index(drop=True)
    
    return open_df, df_in_process, completed_df

def preprocess_df(df): #column_mappings at the top
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
    df['numbers7'].replace('','NaN', inplace=True)
    df['numbers7']=df['numbers7'].astype(float)
    # df['status9'] = df['status9'].map(status_mapping)
    df['status9'] = df['status9'].map(lambda x: {"text": x, "value": status_mapping.get(x, {})})
    df['link'] = df.apply(lambda row: {"url": f"https://reddotstorage2.monday.com/boards/{board_id}/pulses/{row['text2']}", "text": row['name']}, axis=1)

    existing_column_names = monday_data.fetch_column_names(new_board_id)

    # Find the columns to drop
    columns_to_drop = set(df.columns) - set(existing_column_names)
    print(columns_to_drop)
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
            "id": next((col["text"] for col in item["column_values"] if col["id"] == "text2"), None), #id
            "group": item['group']['title'], #name
            "status19": next((col["text"] for col in item["column_values"] if col["id"] == "status19"), None), #status
            "item_id": item['id'], #new id
            "numbers": next((col["text"] for col in item["column_values"] if col["id"] == "numbers"), None), #cost
            "numbers6": next((col["text"] for col in item["column_values"] if col["id"] == "numbers6"), None), #ranking
            "numbers0": next((col["text"] for col in item["column_values"] if col["id"] == "numbers0"), None),#rd budget
            "numbers_1": next((col["text"] for col in item["column_values"] if col["id"] == "numbers_1"), None),#fund budget
            "status9": next((col["text"] for col in item["column_values"] if col["id"] == "status9"), None), #priority
            "numbers1": next((col["text"] for col in item["column_values"] if col["id"] == "numbers1"), None), #priority value
            "numbers05": next((col["text"] for col in item["column_values"] if col["id"] == "numbers05"), None),#after rd budget
            "numbers_15": next((col["text"] for col in item["column_values"] if col["id"] == "numbers_15"), None),#after fund budget
            "text": next((col["text"] for col in item["column_values"] if col["id"] == "text"), None), #fund
            "text8": next((col["text"] for col in item["column_values"] if col["id"] == "text8"), None), #pc
            "rd": next((col["text"] for col in item["column_values"] if col["id"] == "rd"), None), #pc
            "numbers7": next((col["text"] for col in item["column_values"] if col["id"] == "numbers7"), None), #unit values
            "dropdown": next((col["text"] for col in item["column_values"] if col["id"] == "dropdown"), None) # team
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
    df['numbers7'] =df['numbers7'].astype(float)
    
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
            elif item_group != 'Eligible' and project_id in open_df['text2'].values and (row['numbers'] != "" and row['numbers'] != 0 and \
                row['numbers6']!="" and row['status9'] != 'Escalation') and (row['numbers_15'] >=0 or row['status9']=='EMERGENCY' or row['status9']=='High'):
                monday_data.move_items_between_groups(item_id, eligible_group)
                print(f"{project_id} moved from {item_group} to Eligible.")  # replace group IDs with actual IDs
            elif item_group == 'Eligible' and row['numbers_15'] < 0 and row['status9']!='EMERGENCY' and row['status9']!='High':
                monday_data.move_items_between_groups(item_id, ineligible_group)
                print(f"{project_id} moved from {item_group} to Ineligible.") 

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
                monday_data.create_items_from_df(row, group, error_group, ineligible_group)

def update_existing_data(preprocessed_df, existing_items):
    existing_items = existing_items.to_dict(orient='records')

    # print(f"Type of existing_items: {type(existing_items)}")
    count = 1

    for index, row in preprocessed_df.iterrows():
        matching_items = [item for item in existing_items if int(item['id']) == int(row['text2'])]
        
        if matching_items:
            matching_item = matching_items[0]
            
            for column in columns_to_check:
                # Handle 'status9' specially
                if column == 'status9':

                    existing_status_text = matching_item[column]
                    new_status_text = row[column]['text']
                    
                    if existing_status_text != new_status_text:
                        monday_data.change_item_value(new_board_id, matching_item['item_id'], column, row[column]['value'])
                        print(f"value {row['text2']} changed in {column} from {existing_status_text} to {new_status_text}. Values changed: {count}.")
                        count += 1
                else:
                    # Convert string values to their appropriate type before comparison
                    value = matching_item[column]
                    if isinstance(value, float):
                        existing_value = value
                    elif isinstance(value, str) and value.isdigit():
                        existing_value = int(value)
                    else:
                        try:
                            # try converting to a float (will fail if the string is not a number)
                            existing_value = float(value)
                        except ValueError:
                            # if it's not a number, keep it as a string
                            existing_value = value

                    if row[column] != existing_value and column != 'status9':
                        if pd.isna(row[column]):
                            continue
                        monday_data.change_item_value(new_board_id, matching_item['item_id'], column, row[column])
                        print(f"value {row['text2']} changed in {column} from {existing_value} to {row[column]}. Values changed: {count}.")
                        count += 1

def delete_missing_items(completed_df, in_process_df, open_df, existing_items):
    # Combine the ids from completed_df, in_process_df, and open_df
    current_ids = set(completed_df['text2']).union(set(in_process_df['text2'])).union(set(open_df['text2']))

    # Find items in existing_items that are not in current_ids
    missing_items = existing_items[~existing_items['id'].isin(current_ids)]
    print(len(missing_items), ' items to be deleted.')
    # Delete the missing items from the new board
    for _, item in missing_items.iterrows():
        monday_data.delete_item(item['item_id'])
        print(f"Deleted item with id: {item['item_id']} from the new board.")