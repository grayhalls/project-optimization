import pandas as pd
from datetime import date
import numpy as np
from helpers import add_values_to_projects

# Define the priority thresholds for each priority level
low_min, low_max = 10, 300
medium_min, medium_max = 350, 750
high_min, high_max = 8000, 15000
emergency_min, emergency_max = 500000, 800000
# discount_cost = (1/3)

def get_cost_matrix():
    # import cost_matrix & clean
    cost_matrix = pd.read_csv('cost_matrix.csv')
    # Strip spaces from string columns in cost_matrix
    string_columns = cost_matrix.select_dtypes(include=['object']).columns
    for col in string_columns:
        cost_matrix[col] = cost_matrix[col].str.strip()

    cost_matrix['Avg Cost'] = cost_matrix['Avg Cost'].astype(float)
    return cost_matrix

# Define the piecewise function for priority calculation
def priority_function(days, min_priority, max_priority):
    return np.piecewise(days, 
                        [days <= 730, days > 730],
                        [lambda days: min_priority + days * ((max_priority - min_priority) / 730),
                         max_priority])

def priority_value(row):
    # Get the base priority
    if row['Priority'] == 'Low':
        priority_range = (low_min, low_max)
    elif row['Priority'] == 'Medium':
        priority_range = (medium_min, medium_max)
    elif row['Priority'] == 'High':
        priority_range = (high_min, high_max)
    elif row['Priority'] == 'EMERGENCY':
        priority_range = (emergency_min, emergency_max)
    else:
        return np.nan  # return NaN for any other status
    
    scaling = priority_function(row['days'], *priority_range)

    # If the project is unit-specific, multiply the base priority by a factor that reflects the unit value
    # if row['Task Type'] == 'Unit':
    #     scaling *= (1 + row['unit_value'] * unit_value_factor)
    
    return scaling 

def find_cost(row):
    if pd.notnull(row['Final Cost']) and row['Final Cost'] != "":
        return row['Final Cost']
    elif pd.notnull(row['Quoted Cost']) and row['Quoted Cost'] != "":
        return row['Quoted Cost']
    elif pd.notnull(row['Estimated Cost']) and row['Estimated Cost'] != "":
        if row['Estimated Cost'] == 0:
            return 1
        else:
            return row['Estimated Cost']
    else:
        return 0
    
def calculate_costs(df, buffer=1):
    df = df.copy()
    df['cost'] = df.apply(find_cost, axis=1)

    df['Quantity'].fillna(1, inplace=True)
    df['Quantity'].replace('', 1, inplace=True)
    df['Quantity'] = df['Quantity'].astype(float)
    df['cost'] = df['cost'].astype(float)
    
    cost_matrix = get_cost_matrix()
     # Merge df with cost_matrix to get 'Avg Cost' for each row
    df = df.merge(cost_matrix, on=['Project Type', 'Sub Project Type', 'Task Type'], how='left')

    # Calculate estimated cost for rows where 'cost' is 0
    df.loc[df['cost'] == 0, 'cost'] = df['Quantity'] * df['Avg Cost']
    df['cost'].fillna(0, inplace=True)
    
    # Drop the 'Avg Cost' column as it's no longer needed
    df.drop(columns=['Avg Cost'], inplace=True)
    
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce').round(2)
    df['cost'] = df['cost']*buffer

    return df


def calc_cost_effectiveness(df):
    now = date.today()
    # Calculate the number of weeks since 'Open' date
    df = df.copy()
    df.loc[:,'days'] = (now - df['Open']).apply(lambda x: x.days)
    df['days'] = df['days'].astype(int)
    
    # Calculate the cost for each project
    df = calculate_costs(df)
    
    values = add_values_to_projects()
    avg_value = values['replace_value'].mean()
    max_value = values['replace_value'].max()
    values.loc[values['occupied'] == True, 'replace_value'] = max_value

    values = values[['item_id', 'replace_value']]

    alpha = values.groupby(['item_id']).mean()/avg_value
    alpha = alpha.reset_index()
    alpha = alpha.rename(columns = {'item_id':'id', 'replace_value':'alpha'})

    # Apply the function to each row of the DataFrame
    mask = df['Task Type'] == 'Unit'
    df_unit = df[mask]
    df_unit = df_unit.merge(alpha, on='id', how='left')
    df_unit['alpha'] = df_unit['alpha'].fillna(1)
    df_unit['priority_value'] = df_unit.apply(lambda row: priority_value(row), axis=1) * df_unit['alpha']

    df_not_unit = df[~mask]
    df_not_unit['priority_value'] = df_not_unit.apply(lambda row: priority_value(row), axis=1)

    # Concatenate the dataframes back together
    df = pd.concat([df_unit, df_not_unit])

    # Calculate cost-effectiveness
    df['cost_effectiveness'] = np.where(df['cost']=="", 0, np.divide(df['priority_value'], df['cost'], where=df['cost'] !=0))

    # Normalize cost_effectiveness to be out of 100
    df['cost_effectiveness'] = df['cost_effectiveness'].astype(float)

    # Rank based on cost_effectiveness from highest to lowest
    df['rank'] = df['cost_effectiveness'].rank(method='first', ascending=False)

    # Replace 'inf' and '-inf' values with NaN
    df['rank'].replace([np.inf, -np.inf], np.nan, inplace=True)

    # Replace NaN values with 1 + max rank
    max_rank = df['rank'].max()
    df['rank'] = df['rank'].fillna(1 + max_rank).astype(int)

    return df

