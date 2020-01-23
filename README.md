
Mike Herrera's snowflake analysis repo for the data team.
==========
This repo was created in an effort to clean up the set of unused tables in SnowFlake's PROD.MSTR_DATAMART schema.

Loading Data
-----
My analysis relied on SnowFlake audit log data that was injected into bigquery. I had collected query history from Dec 1, 2018 thru to Jan 14, 2020 right before this project was put on hold. Below describes the process which is codified in the script below:

```
bin/loadHistory.py
```

1. ping snowflake's account.query_history table on a given day, only filtering for successful prod.mstr_datamart queries
2. intellligently parse the query history for table names, taking into account that some table names are contained in other table names
3. save both the query history as well as the parsed table history into gcs:

```
PROJECT_ID = 'ox-data-devint'
BUCKET_ID = 'snowflake2bigquery'
```
4. load gcs files into big query using native python libraries (no command line)
```
DATASET_ID = 'snowflake_test'
QUERY_HISTORY_TABLE = 'query_history'
TABLE_HISTORY_TABLE = 'table_history'
```

Analyzing the Data
-----
My analysis work was codified in the following script:

```
analysis/snowFlakeAnalysis.py
```

There were two BQ tables used for the analysis:

* query_history -- the raw audit history from snowflake.
* table_history -- a condensed version of the above table with line item entries for each table found in a given query.

I used table_history to tell me aggregated instances of unique hits to tables over a given period. I also was able to break down those table entries based on the type of query commands used in the system:

```
queryTypes = {'insert': ['INSERT', 'UPDATE', 'UNKNOWN',  'DELETE', 'COPY', 'MERGE', 'SET',
                         'UNLOAD', 'BEGIN_TRANSACTION', 'TRUNCATE_TABLE',],
              'select': ['SELECT', 'CREATE_TABLE_AS_SELECT'],
              'admin': ['CREATE', 'CREATE_TABLE', 'ALTER', 'GRANT', 'REVOKE', 'DROP',
                        'CREATE_TASK', 'ALTER_TABLE_MODIFY_COLUMN', 'ALTER_TABLE_ADD_COLUMN',
                        'RENAME_TABLE', 'ALTER_TABLE', 'ALTER_MATERIALIZED_VIEW_LIFECYCLE',
                        'USE', 'RECLUSTER', 'ALTER_TABLE_DROP_CLUSTERING_KEY',
                        'ALTER_SESSION', 'RESTORE'],
              'describe': ['DESCRIBE_QUERY', 'DESCRIBE', 'SHOW',]}
```

My first part of the analysis involved tables that had no "insert" activity and no "select" activity. The other query categories, although were valid and successful commands to the system, did not reflect true activity.

My analysis was visualized using bokeh library and saved in the following notebook file:

```
notebooks\exceptions.ipynb
```

This notebook sorted all tables according to their windorized monthly hit average along with labels indicting if the table was part of any prod job, any roll up job or used by any view.

When I had collected enough tables I notified users via the slack #dw channel of the intended changes. I used the notebook below to generate an html attachment to the slack channel that contained a series of interactive line charts showing each deprecated table's query history.

```
notebooks\slack_report_template.ipynb
```

For certain tables, I used an interactive bokeh visualization of a networkx directed graph which referenced table names as its nodes, along with prod job names and views. The edges were links to any table that was part of a prod job, a roll up or part of a view definition. You can view the graph in the notebook below:

```
notebooks\networkx_tests.ipynb
```
