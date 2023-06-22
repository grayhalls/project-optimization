import pandas as pd
from monday_functions import * 
from sql_queries import *
from helpers import *
from algo import *
from monday_push_helpers import *
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

# runs the ranking optimization over the available projects
# also calculates remaining budgets
open_df, in_process_df, completed_df = calc_and_sort() # takes a few mins to fetch from project board

# preps data to match with monday's column ids and data types
proc_df_in_process, proc_open_df, proc_completed = preprocessing(in_process_df, open_df, completed_df)

# cleans items between groups. (ie. if an item is completed, it will remove it from the original group 
# and add it to the completed group.)
move_between_groups(proc_completed, proc_df_in_process, proc_open_df) 

# looks for any items that are on the project board that are not in the Ranking board and adds them
create_missing_items(proc_completed, proc_df_in_process, proc_open_df)


