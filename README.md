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
