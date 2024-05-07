# Databricks notebook source
# MAGIC %md
# MAGIC ## Read Me
# MAGIC ### This notebook created to create "top_item" data mart

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Required Packages

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, dense_rank

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting Helper Functions from Helper Notebook

# COMMAND ----------

# MAGIC %run ./helper_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining Variables to be Used on Further Loops
# MAGIC ### Variables could be read from helper notebook as well but for ease of readability defining as hard coded

# COMMAND ----------

## Schema name (Tables to be saved into)
schema_name = "gold_layer"

## Metastore_name
catalog_name = "hive_metastore"

## External file location name (Azure Storage Account)
external_storage = "merkletaskstorage"

## External file location name (Azure Blob Container in storage account) for read operation
dataframe_list = ["items","events"]

## External file location name (Azure Blob Container in storage account) for write operation
naming_conversion_dict = {"items": "dimitems", "events": "factevents", "event_view_item_by_year" : "eventviewitembyyear", "item_view_numbers_by_year" : "itemviewnumbersbyyear", "most_used_platform_by_year_rank" : "mostusedplatformbyyearrank"}

# Secret scope
sas_key_scope = "BlobStorage4"

# Secret key name
sas_key_name = "BLB_Strg_Access_KEY"

# Partition col names
partition_cols = 'event_time_year'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Ingested Raw Data from Silver Layer External Location

# COMMAND ----------

for container in dataframe_list:
    # Create or use existing mount point
    mount_point = f"/mnt/{container}_raw_2"
    already_mounted = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())
    if not already_mounted:
        spark.conf.set(f"fs.azure.sas.{container}.{external_storage}.blob.core.windows.net",
                       dbutils.secrets.get(scope = sas_key_scope, key = sas_key_name))
        
        dbutils.fs.mount(
            source = f"wasbs://{container}@{external_storage}.blob.core.windows.net/",
            mount_point = mount_point,
            extra_configs = {
                f"fs.azure.sas.{container}.{external_storage}.blob.core.windows.net": dbutils.secrets.get(scope = sas_key_scope, key = sas_key_name)
            }
        )
    # Reading delta files from actual mount point per dataframe and assigning to dataframe name
    vars()[container] = read_delta_azure_file(mount_point, container)
    # print for debug on read dataframe sizes
    shape_df = (vars()[container].count(),len(vars()[container].columns))
    message = 'Dataframe read from silver layer pipeline schema read completed. Total {} rows & columns loaded into dataframe from {} table'.format(shape_df,container)
    print(message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating Requested Views on Top Item Datamart

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Total number of item views in a particular year

# COMMAND ----------

# Filtering events dataframe by event_name col. for view_item events
events_view_item = events.filter(events.event_name == "view_item")

# Inner join between events_view_item and items table on item id column to filter item related view items
event_item_join_df = events_view_item.join(items,events_view_item.sub_event_name_value == items.id)

# Selecting only needed columns
event_item_join_df_filter = event_item_join_df.select("event_time_year","id")

# Group by on year column and counting rows to get total views on a year
event_view_item_by_year = event_item_join_df_filter.groupBy("event_time_year").count()

# Column aliasing
event_view_item_by_year = event_view_item_by_year.select(col("event_time_year").alias("Year"),col("count").alias("Total Item View"))

# Ordering by Year as descending
event_view_item_by_year = event_view_item_by_year.orderBy(col("Year").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Rank of an item based on number of views in a particular year

# COMMAND ----------

# Gathered only needed cols for calculation (item id taken since it is unique per item)
event_item_join_df_filter = event_item_join_df.select("event_time_year","id")

# Grouped by year and id cols and counted rows
year_item_group_by = event_item_join_df.groupBy("event_time_year","id").count()

# Creating window item which will partition count in a year level (start again when year changes)
window_item = Window.partitionBy(year_item_group_by["event_time_year"]).orderBy(desc("count"))

# Rank column added and window function applied
item_view_numbers_by_year = year_item_group_by.withColumn("item_views",dense_rank().over(window_item))

# Column aliasing
item_view_numbers_by_year = item_view_numbers_by_year.select(col("event_time_year").alias("Year"),col("count").alias("Total Item View"),col("id").alias("Item Id"),col("item_views").alias("View Ranking"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### The most used platform in particular year

# COMMAND ----------


event_item_join_df_filter = event_item_join_df.select("event_time_year","platform")

most_used_platform_by_year = event_item_join_df_filter.groupBy("event_time_year","platform").count()

window_item = Window.partitionBy(most_used_platform_by_year["event_time_year"]).orderBy(desc("count"))

most_used_platform_by_year_rank = most_used_platform_by_year.withColumn("platform_usage",dense_rank().over(window_item))

most_used_platform_by_year_rank = most_used_platform_by_year_rank.filter(most_used_platform_by_year_rank.platform_usage == 1)

# Column aliasing
most_used_platform_by_year_rank = most_used_platform_by_year_rank.select(col("event_time_year").alias("Year"),col("platform").alias("Most Used Platform"),col("count").alias("Total Platform Usage"))

# Ordering by Year as descending
most_used_platform_by_year_rank = most_used_platform_by_year_rank.orderBy(col("Year").desc())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving dataframes to gold layer storage
# MAGIC ### For this layer dataframes will also be saved as managed table addition to external table (For proving capability on this option)

# COMMAND ----------

# MAGIC %md
# MAGIC ### External table on external location save

# COMMAND ----------

for ex_df_name,new_df_name in naming_conversion_dict.items():
    print(ex_df_name,new_df_name)
    # Saving transformed data frames into silver layer storage as external table by changing their names
    new_df_name_str = new_df_name
    vars()[new_df_name] = globals()[ex_df_name]
    # printing read file size for debug
    shape_df = (vars()[new_df_name].count(),len(vars()[new_df_name].columns))
    print('Dataframes read on gold_layer_pipeline. Total {} rows & columns loaded into dataframe for {} dataframe`s gold layer schema write'.format(shape_df,new_df_name_str))
    # Create or use existing mount point
    mount_point = f"/mnt/{new_df_name_str}_silver"
    already_mounted = any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts())
    if not already_mounted:
        spark.conf.set(f"fs.azure.sas.{new_df_name_str}.{external_storage}.blob.core.windows.net",
                       dbutils.secrets.get(scope = sas_key_scope, key = sas_key_name))
        
        dbutils.fs.mount(
            source = f"wasbs://{new_df_name_str}@{external_storage}.blob.core.windows.net/",
            mount_point = mount_point,
            extra_configs = {
                f"fs.azure.sas.{new_df_name_str}.{external_storage}.blob.core.windows.net": dbutils.secrets.get(scope = sas_key_scope, key = sas_key_name)
            }
        )
    # Writing dataframes to external Azure storage as delta formatted
    if ex_df_name == "events":
        vars()[new_df_name].write.format("delta").partitionBy(partition_cols).option("delta.columnMapping.mode", "name").mode("overwrite")\
        .option("path", f"{mount_point}").saveAsTable(new_df_name_str)
    else:
        vars()[new_df_name].write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite")\
        .option("path", f"{mount_point}").saveAsTable(new_df_name_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving dataframes as managed table on hive metastore`s gold layer schema

# COMMAND ----------

for ex_df_name,new_df_name in naming_conversion_dict.items():
    print(ex_df_name,new_df_name)
    # Saving created views and data frames into gold layer storage as managed table by changing their names
    new_df_name_str = new_df_name
    vars()[new_df_name] = globals()[ex_df_name]
    # printing read file size for debug
    shape_df = (vars()[new_df_name].count(),len(vars()[new_df_name].columns))
    print('Dataframes read on gold_layer_pipeline. Total {} rows & columns loaded into dataframe for {} dataframe`s gold layer schema write'.format(shape_df,new_df_name_str))
    # writing dataframes by using function gathered from helper notebook
    write_to_managed_table(vars()[new_df_name],new_df_name_str,schema_name,catalog_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 
