# Fastest Methods to Bulk Insert a Pandas Dataframe into PostgreSQL

## We are going to compare methods to load pandas dataframe into database. 

### There is 253,186 rows in csv file (16.4 MB). 

method 1: to_sql    --- 16.59 seconds 

method 2: copy_expert    --- 2.00 seconds

method 3: copy_expert_csv    --- 2.11 seconds

method 4: to_sql_method_copy   --- 1.86 seconds
