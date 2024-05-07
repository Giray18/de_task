# Databricks notebook source
# MAGIC %md
# MAGIC ##Starter_Notebook, 
# MAGIC ### This notebook will initiate notebook runs as needed to populate 3 layer one after another. Consider a scenario where this notebook being triggered on a Azure Data Factory pipeline. By this action, we are able to apply delta load to our tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Variables

# COMMAND ----------

notebook_names = ["bronze_layer_pipeline","silver_layer_pipeline","gold_layer_pipeline"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Running Notebooks

# COMMAND ----------

for notebook_name in notebook_names:
    print(f'running {notebook_name}')
    dbutils.notebook.run(f'{notebook_name}',1000)
    print(f'running {notebook_name} completed')
