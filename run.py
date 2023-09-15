from monday_push_helpers import calc_and_sort, preprocessing, find_existing_rows, \
    move_between_groups, create_missing_items, update_existing_data, delete_missing_items
import time 
initial = time.time()

# runs the ranking optimization over the available projects
# also calculates remaining budgets
open_df, in_process_df, completed_df = calc_and_sort() # takes a few mins to fetch from project board
print('calc_and_sort() took', round((time.time()-initial)/60,2), 'minutes.')
# preps data to match with monday's column ids and data types
proc_df_in_process, proc_open_df, proc_completed = preprocessing(in_process_df, open_df, completed_df)

existing_items = find_existing_rows()
delete_missing_items(proc_completed, proc_df_in_process, proc_open_df, existing_items)

# cleans items between groups. (ie. if an item is completed, it will remove it from the original group 
#   and add it to the completed group.)
start = time.time()
move_between_groups(proc_completed, proc_df_in_process, proc_open_df, existing_items) 
print('move_between_groups() took', round((time.time()-start)/60,2), 'minutes.')

start = time.time()
# looks for any items that are on the project board that are not in the Ranking board and adds them
create_missing_items(proc_completed, proc_df_in_process, proc_open_df, existing_items)
print('create_missing_items() took', round((time.time()-start)/60,2), 'minutes.')

existing_items = find_existing_rows() #removed this after fixing status for new projects
start = time.time()
print('Updating In Process Items')
update_existing_data(proc_df_in_process, existing_items)
print('Updating Eligible Items')
update_existing_data(proc_open_df, existing_items)
print('update_existing_data() took', round((time.time()-start)/60,2), 'minutes.')

print('Algorithm took', round((time.time()-initial)/60,2), 'minutes total.')
