import pandas as pd
from datetime import date
import numpy as np

# Define the priority thresholds for each priority level
low_min, low_max = 10, 300
medium_min, medium_max = 350, 750
high_min, high_max = 800, 1500
emergency_min, emergency_max = 5000, 10000
unit_value_factor = 0.5

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
        return row['Estimated Cost']
    else:
        return 0
    
def calculate_costs(df, buffer=1):
    df = df.copy()
    df['cost'] = df.apply(find_cost, axis=1)
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce').round(2)
    df['cost'] = df['cost']*buffer
    return df

def calc_cost_effectiveness(df):
    now = date.today()
    # Calculate the number of weeks since 'Open' date
    df.loc[:,'days'] = (now - df['Open']).apply(lambda x: x.days)
    df['days'] = df['days'].astype(int)
    # Calculate the cost for each project
    df = calculate_costs(df)
    
    # Apply the function to each row of the DataFrame
    df['priority_value'] = df.apply(lambda row: priority_value(row), axis=1)

    # Calculate cost-effectiveness
    df['cost_effectiveness'] = np.where(df['cost'] == 0, df['priority_value'], np.divide(df['priority_value'], df['cost'], where=df['cost'] !=0))
    # Normalize cost_effectiveness to be out of 100
    df['cost_effectiveness'] = df['cost_effectiveness'].astype(float)

    max_cost_effectiveness = df[df['cost'] > 0]['cost_effectiveness'].max()
    df['cost_effectiveness'] = (df['cost_effectiveness'] / max_cost_effectiveness) * 100
    df['cost_effectiveness'] = df['cost_effectiveness'].round(2)
    return df


# def priority_value(row, low_base, medium_base, high_base, emergency_base, rate, unit_value_factor):
#     # Get the base priority
#     if row['Priority'] == 'Low':
#         base_priority = low_base
#     elif row['Priority'] == 'Medium':
#         base_priority = medium_base
#     elif row['Priority'] == 'High':
#         base_priority = high_base
#     elif row['Priority'] == 'EMERGENCY':
#         base_priority = emergency_base
#     else:
#         return np.nan  # return NaN for any other status

#     # If the project is unit-specific, multiply the base priority by a factor that reflects the unit value
#     # if row['Item Type'] == 'Unit':
#     #     base_priority *= (1 + row['unit_value'] * unit_value_factor)

#     # Return the priority value
#     return base_priority * np.exp(rate * row['days'])