# Instructions for running
---

To run: 

Command prompt and navigate to folder and activate virtual env.
```
python run.py
```

Variables that can be changed:
- scaling factors: top of algo.py
  - this is a piecewise function with mins and maxes on each priority. None overlap.
  - These scale over 2 years before reaching the max. This can be changed in the priority_function().
  - Unit values will be incorporated later.
- pending_statuses: top of monday_push_helpers.py
  - this was updated with Bre's list
- cost buffer: top of monday_push_helpers.py
  - this adds a buffer for the cost of projects that haven't been completed yet. Current buffer is 10%.   

Monday Board: https://reddotstorage2.monday.com/boards/4606795381/views/104007445

Monday python library documentation: https://github.com/ProdPerfect/monday/tree/master/docs


----- 

## TODO 

- [ ] sub-item api manipulation 
  - https://community.monday.com/t/updating-subitems-via-api/16626/4 
  - turn GraphQL requests into python requests.post 
    - Monday client may not have subitem functionality, but the monday api seems to 
- [ ] unit values in scoring 


new scoring flow will be 
  - parent item: something like "RD036 - Doors" 
    - with subitems like: "A036", "B54", etc 


--> parent item (task level) – score by priority bucket
 
--> subitems (unit level) – score/modify by unit value 