from monday import MondayClient
import pandas as pd
from dotenv import load_dotenv
import os
import requests 
import numpy as np
from typing import List, Dict

class Monday:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('api_key')
        self.board_id = os.getenv('board_id') 
        self.new_board_id = os.getenv('new_board_id')
        self.client = MondayClient(self.api_key)
        self.headers = {"Authorization" : self.api_key}
        self.url = "https://api.monday.com/v2"

    def fetch_open_items(self):
        # Calculate the total number of items
        group_titles = ['North', 'South', 'Central']
        total_items = self.count_items_in_groups(group_titles)
        if total_items <=1000:
            limit = total_items
        else:
            limit = 1000
        # Determine the number of pages needed
        pages = (total_items // limit) + 1

        # Fetch column names
        column_names = self.fetch_column_names(self.board_id)

        rows = []

        # Fetch and process items page by page
        for page in range(1, pages + 1):
            try:
                results = self.client.boards.fetch_items_by_board_id(board_ids=self.board_id, limit=limit, page=page)
                data = results['data']['boards'][0]['items']
            except Exception as e:
                # the API client can raise
                print(f"An error occurred while fetching items: {e}")
                continue

            for item in data:
                if item['group']['title'] in set(group_titles):
                    row = self.parse_item(item, column_names)
                    rows.append(row)
            
        df = pd.DataFrame(rows)

        df = self.transform_dataframe(df)  # Moved dataframe manipulation to a separate function

        return df
    
    def fetch_completed(self):
        group_titles = ['North', 'South', 'Central', 'Complete']
        total_items = self.count_items_in_groups(group_titles)
        if total_items <=1000:
            limit = total_items
        else:
            limit = 1000
       
        pages = (total_items // limit) + 1
        column_names = self.fetch_column_names(self.board_id)
        rows = []

        # Fetch and process items page by page
        for page in range(1, pages + 1):
            try:
                results = self.client.boards.fetch_items_by_board_id(board_ids=self.board_id, limit=limit, page=page)
                data = results['data']['boards'][0]['items']
            except Exception as e:
                # the API client can raise
                print(f"An error occurred while fetching items: {e}")
                continue

            for item in data:
                if item['group']['title'] == 'Complete':
                    row = self.parse_item(item, column_names)
                    rows.append(row)
            
        df = pd.DataFrame(rows)
        
        df = self.transform_dataframe(df)  # Moved dataframe manipulation to a separate function

        return df
    
    def fetch_column_names(self, board_id):
        results = self.client.boards.fetch_columns_by_board_id(board_ids=board_id)
        data = results['data']['boards'][0]['columns']
        column_names = {column['id']: column['title'] for column in data}
        return column_names

    def count_items_in_groups(self, group_titles: List[str]) -> int:
        # Fetch all groups on the board
        groups = self.client.groups.get_groups_by_board(self.board_id)
        
        # Filter out the groups with matching titles
        relevant_groups = [group for group in groups['data']['boards'][0]['groups'] 
                        if group['title'] in group_titles]

        # For each relevant group, fetch items and count them
        total_items = 0
        for group in relevant_groups:
            group_id = group['id']
            group_items = self.client.groups.get_items_by_group(self.board_id, group_id)
            total_items += len(group_items['data']['boards'][0]['groups'][0]['items'])

        return total_items

    def parse_item(self, item: Dict, column_names: Dict[str, str]) -> Dict:
        """
        Helper function to parse an item into a dictionary.
        """
        if '-' in item['name']:
            facility, item_name = item['name'].split('-', 1)
        elif ' ' in item['name']:
            facility, item_name = item['name'].split(' ', 1)
        else:
            facility, item_name = item['name'], ''

        item_name = item_name.strip()
        row = {'region': item['group']['title'], 'id': item['id'] , 'item_name': item_name, 'facility':facility}

        for column in item['column_values']:
            column_name = column_names.get(column['id'], column['id'])
            row[column_name] = column['text']

        return row
    
    def transform_dataframe(self, df):
        df.loc[:,'RD'] = df['RD'].replace("", np.nan)
        df['RD'] = df['RD'].fillna(df['facility'])
        df = df[['region','id', 'RD', 'Task Type', 'Project Type', 'item_name', 'Priority', 'Status', 'PC', 'RL Link', 'Open', 'Scheduled', 'Estimated Cost', 'Quoted Cost', 'Deposit Date','Deposit Amount','Final Cost']]
        df.loc[:, 'Open'] = pd.to_datetime(df['Open']).dt.date
        df['RD'] = df['RD'].str.strip()

        return df

    def delete_item(self, item_id):
        self.client.items.delete_item_by_id(item_id)  

    def create_items_from_df(self, row, group_id, error_group):
  
        if row['status19'] == 'Complete':
            group = group_id
        elif row['number'] == "" or row['number'] == 0 or row['numbers6']=="":
            group = error_group
        else:
            group = group_id
        # Construct the column_values (you can customize this based on your needs)
        column_values = row.to_dict()

        # Create the item
        try:
            print("trying to create item...")
            self.client.items.create_item(board_id=self.new_board_id, group_id=group, 
                                                        item_name=row['name'], 
                                                        column_values=column_values,
                                                        create_labels_if_missing=True)
        except Exception as e:
            print(f"Error creating item: {e}")  
 
    # def create_items_from_df(self, df, group_id):
    #     for _, row in df.iterrows():

    #         # Construct the item_name (you can customize this based on your needs)
    #         item_name = f"{row['RD']} - {row['item_name']}" if 'item_name' in df.columns and 'RD' in df.columns else ""

    #         # Construct the column_values (you can customize this based on your needs)
    #         column_values = {
    #             "name": (row['RD'] + " - " + row['item_name']) if 'RD' in df.columns and 'item_name' in df.columns else "",
    #             "region": row['region'] if 'region' in df.columns else "",
    #             "rd": row['RD'] if 'RD' in df.columns else "",
    #             "project_id0": row['id'] if 'id' in df.columns else "",
    #             "number": row['cost'] if 'cost' in df.columns and pd.notnull(row['cost']) else 0, # replace NaN with 0
    #             "numbers6": row['cost_effectiveness'] if 'cost_effectiveness' in df.columns and pd.notnull(row['cost_effectiveness']) else 0, # replace NaN with 0
    #             "text": row['fund'] if 'fund' in df.columns and pd.notnull(row['fund']) else "", # replace NaN with empty string
    #             "numbers0": row['remaining_budget'] if 'remaining_budget' in df.columns and pd.notnull(row['remaining_budget']) else 0, # replace NaN with 0
    #             "numbers_1": row['remaining_fund_budget'] if 'remaining_fund_budget' in df.columns and pd.notnull(row['remaining_fund_budget']) else 0, # replace NaN with 0
    #             "exceeds_rd_budget3": str(row['exceeds_facility_budget']) if 'exceeds_facility_budget' in df.columns else "", # Exceeds RD budget
    #             "exceeds_fund_budget": str(row['exceeds_fund_budget']) if 'exceeds_fund_budget' in df.columns else "" # Exceeds Fund budget
    #         }

    #         # Create the item
    #         try:
    #             response = self.client.items.create_item(board_id=self.new_board_id, group_id=group_id, item_name=item_name, column_values=column_values)
    #         except Exception as e:
    #             print(f"Error creating item: {e}")

 


                  

    
    
    
    # doesn't work
    def fetch_subitems(self):
        query = '''
        query {
          boards(ids: %s) {
            items {
              id
              name
              subitems {
                id
                name
                column_values {
                  id
                  text
                }
              }
            }
          }
        }
        ''' % self.board_id
        data = {"query": query}
        response = requests.post(self.url, headers=self.headers, json=data)
        response_json = response.json()

        rows = []
        for item in response_json['data']['boards'][0]['items']:
            for subitem in item.get('subitems', []):
                row = {'parent_item_id': item['id'], 'subitem_id': subitem['id'], 'subitem_name': subitem['name']}
                for column in subitem['column_values']:
                    row[self.columns[column['id']]] = column['text']
                rows.append(row)
                
        return pd.DataFrame(rows)
    

