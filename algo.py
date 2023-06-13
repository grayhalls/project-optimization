import pandas as pd
from datetime import date
import numpy as np

def priority_value(row, low_base, medium_base, high_base, emergency_base, rate, unit_value_factor):
    # Get the base priority
    if row['Priority'] == 'Low':
        base_priority = low_base
    elif row['Priority'] == 'Medium':
        base_priority = medium_base
    elif row['Priority'] == 'High':
        base_priority = high_base
    elif row['Priority'] == 'EMERGENCY':
        base_priority = emergency_base
    else:
        return np.nan  # return NaN for any other status

    # If the project is unit-specific, multiply the base priority by a factor that reflects the unit value
    # if row['Item Type'] == 'Unit':
    #     base_priority *= (1 + row['unit_value'] * unit_value_factor)

    # Return the priority value
    return base_priority * np.exp(rate * row['days'])


def find_cost(row):
    if pd.notnull(row['Final Cost']) and row['Final Cost'] != "":
        return row['Final Cost']
    elif pd.notnull(row['Quoted Cost']) and row['Quoted Cost'] != "":
        return row['Quoted Cost']
    elif pd.notnull(row['Estimated Cost']) and row['Estimated Cost'] != "":
        return row['Estimated Cost']
    else:
        return 0
    
def calculate_costs(df):
    df = df.copy()
    df['cost'] = df.apply(find_cost, axis=1)
    df['cost'] = pd.to_numeric(df['cost'], errors='coerce')
    return df

def calc_cost_effectiveness(df, low_base, medium_base, high_base, emergency_base, rate, unit_value_factor):
    now = date.today()
    # Calculate the number of weeks since 'Open' date
    df.loc[:,'days'] = (now - df['Open']).apply(lambda x: x.days)
    df['days'] = df['days'].astype(int)
    # Calculate the cost for each project
    df = calculate_costs(df)
    
    # Apply the function to each row of the DataFrame
    df['priority_value'] = df.apply(lambda row: priority_value(row, low_base, medium_base, high_base, emergency_base, rate, unit_value_factor), axis=1)

    # Calculate cost-effectiveness
    df['cost_effectiveness'] = np.where(df['cost'] == 0, df['priority_value'], df['priority_value'] / df['cost'])
    
    return df

    