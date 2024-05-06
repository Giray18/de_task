# Databricks notebook source
# MAGIC %md
# MAGIC ## Read Me
# MAGIC ### This notebook created to apply data transformations for dataframes read from bronze layer`s external tables and save into silver layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importing Required Packages

# COMMAND ----------

from pyspark.sql.functions import from_json, col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Getting helper functions from helper notebook

# COMMAND ----------

# MAGIC %run ./helper_notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defining variables to be used on further loops
# MAGIC ### Variables could be read from helper notebook as well but for ease of readability defining as hard coded

# COMMAND ----------

## Schema name (Tables to be saved into)
schema_name = "silver_layer"

## Metastore_name
location_name = "hive_metastore"

## External file location name (Azure Storage Account)
external_storage = "merkletaskstorage"

## External file location name (Azure Blob Container in storage account) for read operation
dataframe_list = ["itemdfraw","eventdfraw"]

## External file location name (Azure Blob Container in storage account) for write operation
naming_conversion_dict = {"itemdfraw": "items", "eventdfraw": "events"}

# Secret scope
sas_key_scope = "BlobStorage4"

# Secret key name
sas_key_name = "BLB_Strg_Access_KEY"  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading Ingested Raw data from bronze layer external location

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
    # Reading CSV files from actual mount point per dataframe and assigning to dataframe name
    vars()[container] = read_csv_azure_file(mount_point, container)
    # print for debug on read dataframe sizes
    shape_df = (vars()[container].count(),len(vars()[container].columns))
    message = 'Dataframe read from bronze layer pipeline schema read completed. Total {} rows & columns loaded into dataframe from {} table'.format(shape_df,container)
    print(message)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten structured columns

# COMMAND ----------

# Definining required schema for unstructure column - There is only one column exists on eventdfraw dataframe as detected on data profiling activity
schema = StructType(
    [
        StructField('event_name', StringType(), True),
        StructField('platform', StringType(), True),
        StructField('parameter_name', StringType(), True),
        StructField('parameter_value', StringType(), True),
    ]
)

# Flattenning structured column and selecting needed columns
eventdfraw = eventdfraw.withColumn("event_payload", from_json(col("`event.payload`"), schema))\
    .select(
        col('event_id'),
        col('event_time'),
        col('user_id'),
        col('event_payload.*')
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming some attributes on dataframes
# MAGIC ### Based on data profiling activities made on sample data just 2 attiribute name will be changed to make it more understandable, parameters are sub class of event name due to that paramater_name = sub_event_name, paramater_value = sub_event_name_value changes will be done

# COMMAND ----------

# Looped through all dataframes by calling values from a dict structure on helper notebook in case there will be a lot of transformation this structure can be changed easily and provide a dynamic structure
for df_nm in dataframe_list:
    # functioned run only if dataframe is in mapping dict structure (called from helper notebook)
    if df_nm in mapping_dict.keys():
        # rename columns function called from helper notebook
        globals()[df_nm] = rename_columns(globals()[df_nm],df_nm,mapping_dict)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Casting dataframe attiributes to proper data type
# MAGIC ### Casting some attiributes aligned with business requests and lessen size volume, applying same structure with previous operation. Calling needed structures from helpers notebook

# COMMAND ----------

# Looped through all dataframes by calling values from a dict structure on helper notebook in case there will be a lot of transformation this structure can be changed easily and provide a dynamic structure
for df_nm in dataframe_list:
    # functioned run only if dataframe is in casting dict structure (called from helper notebook)
    if df_nm in casting_dict.keys():
        for col_name in casting_dict[df_nm].keys():
            # Debug print
            print(f"casted columns on dataframe, {df_nm},{col_name} as type {casting_dict[df_nm][col_name]}")
            # rename columns function called from helper notebook
            dataType = eval(casting_dict[df_nm][col_name])
            globals()[df_nm] = globals()[df_nm].withColumn(
                col_name,
                col(col_name).cast(dataType())
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating partitioning column on fact (events dataframe) by event_time column. I applied partition level as YEAR since all requested views on top_item datamart contains that level of granularity

# COMMAND ----------

# Looped through all dataframes by calling values from a dict structure on helper notebook in case there will be a lot of transformation this structure can be changed easily and provide a dynamic structure
for df_nm in dataframe_list:
    # functioned run only if dataframe is in partition dict structure (called from helper notebook)
    if df_nm in partition_dict.keys():
        for col_name,new_col_func in partition_dict[df_nm].items():
            # Debug print
            print(f"created partitioned columns on dataframe, {df_nm},column{col_name}")
            # Saving partition col name into a variable
            partition_cols = list(partition_dict[df_nm][col_name].keys())[0]
            for new_col, time_func in partition_dict[df_nm][col_name].items():
                # By below loop I am calling needed function from F (functions objects)
                if hasattr(F, time_func):  # assigning year function to func_time variable
                    func_time = getattr(F, time_func)  
                    globals()[df_nm] = globals()[df_nm].withColumn(new_col, func_time(col_name))
                else:
                    print(f"Function {time_func} not found in pyspark.sql.functions")
        

# COMMAND ----------

# MAGIC %md
# MAGIC ## Saving dataframes to silver layer storage

# COMMAND ----------

for ex_df_name,new_df_name in naming_conversion_dict.items():
    print(ex_df_name,new_df_name)
    # Saving transformed data frames into silver layer storage as external table by changing their names
    new_df_name_str = new_df_name
    vars()[new_df_name] = globals()[ex_df_name]
    # printing read file size for debug
    shape_df = (vars()[new_df_name].count(),len(vars()[new_df_name].columns))
    print('Dataframes transformed on silver_layer_pipeline. Total {} rows & columns loaded into dataframe for {} dataframe`s Silver layer schema write'.format(shape_df,new_df_name_str))
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
    if ex_df_name == "eventdfraw":
        vars()[new_df_name].write.format("delta").partitionBy(partition_cols).option("delta.columnMapping.mode", "name").mode("overwrite")\
        .option("path", f"{mount_point}").saveAsTable(new_df_name_str)
    else:
        vars()[new_df_name].write.format("delta").option("delta.columnMapping.mode", "name").mode("overwrite")\
        .option("path", f"{mount_point}").saveAsTable(new_df_name_str)

# COMMAND ----------

# MAGIC %md
# MAGIC
