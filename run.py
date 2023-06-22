from monday_push_helpers import *


# runs the ranking optimization over the available projects
# also calculates remaining budgets
open_df, in_process_df, completed_df = calc_and_sort() # takes a few mins to fetch from project board

# preps data to match with monday's column ids and data types
proc_df_in_process, proc_open_df, proc_completed = preprocessing(in_process_df, open_df, completed_df)

existing_items = find_existing_rows()
# cleans items between groups. (ie. if an item is completed, it will remove it from the original group 
# and add it to the completed group.)
move_between_groups(proc_completed, proc_df_in_process, proc_open_df, existing_items) 

# looks for any items that are on the project board that are not in the Ranking board and adds them
create_missing_items(proc_completed, proc_df_in_process, proc_open_df, existing_items)


