from monday import MondayClient
import pandas as pd
from dotenv import load_dotenv
import os
import requests 
import numpy as np
from typing import List, Dict
from collections import Counter
import json 

class Monday:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv('api_key')
        self.board_id = os.getenv('board_id') 
        self.new_board_id = os.getenv('new_board_id')
        self.client = MondayClient(self.api_key)
        self.headers = {"Authorization" : self.api_key}
        self.url = "https://api.monday.com/v2"

    def fetch_items(self, group_titles, all_groups=['North', 'South', 'Central', 'Complete']):
        # Calculate the total number of items
        total_items = self.count_items_in_groups(all_groups)
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
    
    def fetch_items_by_board_id(self, board):
        items = self.client.boards.fetch_items_by_board_id(board)
        return items

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

    def create_items_from_df(self, row, group_id, error_group, ineligible_group):

        if group_id =='new_group51572' or group_id=='topics': #if completed or in process, no errors
            group = group_id
        # checks if cost is blank or 0 or if cost_effectiveness is blank or if status is Escalation
        elif row['numbers'] == "" or row['numbers'] == 0 or row['numbers6']=="" or row['status9'] == 'Escalation'\
        or pd.isna(row['numbers']) or pd.isna(row['numbers6']):
            group = error_group
        elif row['numbers_15'] < 0:
            group = ineligible_group
        else: #else goes to eligible
            group = group_id
        # Construct the column_values (you can customize this based on your needs)
        column_values = row.to_dict()
        
        for key, value in column_values.items():
            if pd.isna(value):
                column_values[key] = None

        dropdown_columns = ['exceeds_rd_budget5', 'exceeds_fund_budget2']

        # Update dropdown column values
        for dropdown in dropdown_columns:
            if dropdown in column_values:
                if column_values[dropdown] == False:
                    column_values[dropdown] = "1"
                elif column_values[dropdown] == True:
                    column_values[dropdown] = "2"
                else:
                    column_values[dropdown] = "3"

        # print(column_values)
        # Create the item
        try:
            print("trying to create item...")

            response = self.client.items.create_item(board_id=self.new_board_id, group_id=group, 
                                                        item_name=row['name'], 
                                                        column_values=column_values,
                                                        create_labels_if_missing=True)
            print(f'API Response: {response}')
        except Exception as e:
            print(f"Error creating item: {e}") 

    def move_items_between_groups(self, item_id, group):
        self.client.items.move_item_to_group(item_id, group)

    def change_item_value(self, new_board_id, item_id, column_id, value):
        self.client.items.change_item_value(new_board_id, item_id, column_id, value)
 
    def find_existing_ids(self, new_board_id):
        existing_items = self.client.boards.fetch_items_by_board_id(new_board_id)
        existing_items = existing_items['data']['boards'][0]['items']
        return [next((col["text"] for col in item["column_values"] if col["id"] == "text2"), None) for item in existing_items]

    def find_duplicate_ids(self, id_list):
        counts = Counter(id_list)
        return [id for id, count in counts.items() if count > 1]

    def delete_duplicates(self, duplicate_ids, new_board_id):
        existing_items = self.client.boards.fetch_items_by_board_id(new_board_id)
        existing_items = existing_items['data']['boards'][0]['items']

        for item in existing_items:
            id = next((col["text"] for col in item["column_values"] if col["id"] == "text2"), None)
            item_id = item['id']

            if id in duplicate_ids:
                # remove this id from duplicate_ids list so we keep one of them
                duplicate_ids.remove(id)

                # delete this item
                try:
                    self.delete_item(item_id)
                    print(f"Deleted item: {item_id}")
                except Exception as e:
                    print(f"Error deleting item: {e}")

    def query_items(self, board_id):
        headers = {
            'Authorization': self.api_key,
            'Content-Type': 'application/json',
        }

        query = """
            query ($boardId: [Int]) {
                boards (ids: $boardId) {
                    items {
                        id
                        name
                        group {
                            title
                        }
                        column_values(ids: ["dropdown3", "status", "text4"]) {
                            id
                            text
                            value
                        }
                    }
                }
            }
        """

        variables = {'boardId': [int(board_id)]}

        data = {'query': query, 'variables': variables}

        response = requests.post('https://api.monday.com/v2', headers=headers, json=data)
        response_json = response.json()
        return response_json

    def query_subitems(self, item_id):
        headers = {
            'Authorization': self.api_key,
            'Content-Type': 'application/json',
        }

        query = """
            query ($itemId: [Int]) {
                items (ids: $itemId) {
                    subitems {
                        id
                        name
                        column_values(ids: ["link", "status_1"]) {
                            id
                            text
                        }
                    }
                }
            }
        """

        variables = {'itemId': [int(item_id)]}  

        data = {'query': query, 'variables': variables}

        response = requests.post('https://api.monday.com/v2', headers=headers, json=data)
        response_json = response.json()
        return response_json
    
    def generate_subitem_df(self, board_id, groups=['South', 'North', 'Central']):
        data_for_df = []

        items_results = self.query_items(board_id)
        assert items_results is not None, "items_results is None"

        items_data = items_results.get('data', {}).get('boards', [])[0].get('items', [])
        assert items_data is not None, "items_data is None"

        items_data = [item for item in items_data if item['group']['title'] in groups and any(col.get('value') == '{"ids":[1]}' for col in item['column_values'] if col.get('id') == 'dropdown3')]
        assert items_data, "No items match the given conditions"

        for item in items_data:
            item_id = item['id']
            item_name = item['name']
            rd = next((col.get('text') for col in item['column_values'] if col.get('id') == 'text4'), None)
            item_status = next((col.get('text') for col in item['column_values'] if col.get('id') == 'status'), None)

            subitems_results = self.query_subitems(item_id)
            assert subitems_results is not None, "subitems_results is None"

            items = subitems_results.get('data', {}).get('items', [])
            if items:
                subitems = items[0].get('subitems', [])
                if subitems:
                    for subitem in subitems:
                        subitem_id = subitem['id']
                        subitem_name = subitem['name']
                        subitem_link = next((col.get('text') for col in subitem['column_values'] if col.get('id') == 'link'), None)
                        subitem_status_1 = next((col.get('text') for col in subitem['column_values'] if col.get('id') == 'status_1'), None) or item_status

                        if not (subitem_name == 'Subitem' and not subitem_link):
                            data_for_df.append({
                                "item_id": item_id,
                                "subitem_id": subitem_id,
                                "site_code": rd,
                                "item_name": item_name,
                                "subitem_name": subitem_name,
                                "link": subitem_link,
                                "status_1": subitem_status_1
                            })

        df = pd.DataFrame(data_for_df)
        assert not df.empty, "DataFrame is empty"

        return df



                  



