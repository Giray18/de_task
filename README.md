# Data Engineering Task Solution
## Motivation
This repo created to be used as a codespace for a Data Engineering task.
There are ipynb notebooks and xlsx files on main branch. Those files are creating and ELT process to create Views on a data mart created from CSV source files located on S3 bucket.
Databricks used as data platform with PYSPARK code holding notebooks does the related activities.

## Contents of Repo
**dat** : A file that holds dat python module files (Data Profiling/Analysis Module) that is a module compiled by myself and has many methods to apply profiling/analysis activities for structured/semi-structured data sources located on different platforms. Can be imported to ipynb notebooks with import dat command.

**analysis_dataset_event.csv_2024-05-06.xlsx** : Output of data_profiling notebook which can be found on (https://github.com/Giray18/de_task/blob/main/data_profiling_notebook.ipynb) this notebook connects to S3 bucket sources and automatically creates an output xlsx file that holds profile of data source file.

**analysis_dataset_item.csv_2024-05-06.xlsx** : Same with above explanation but this time profiling made for item.csv file located on source location.

**bronze_layer_pipeline.ipynb** : Holds notebook that is being used on raw data ingestion

**data_profiling_notebook.ipynb** : Holds python code that is being used on data profiling activity of source files on S3 bucket.



 
