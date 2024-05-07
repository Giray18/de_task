# Databricks notebook source
# MAGIC %md
# MAGIC ## Reads csv files from given s3 location

# COMMAND ----------

def read_csv_file(storage_location_name,dataframe_name,file_list = []):
    """
    Creates dataframe based on defined elements data_frame_names list.
    Reads files from cloud storage location conditionally by the file type 

    Args:
        dataframe_name (str): The name of the dataframe.
        storage_location_name (str): The name of the storage location name.
        file_list (list): Name of the files to be read from storage location saved in a list.

    Returns:
        DataFrame: A spark dataframe.
    """
    for file_name in file_list:
        if "csv" in file_name:
            dataframe_name = (spark.read.format("csv")\
            .option("mode", "PERMISSIVE")\
            .option("quote",'"')\
            .option("escape",'"')\
            .option("header", "true")\
            .option("inferSchema", "true")\
            .option("delimiter", ",")\
            .load(f"s3a://{storage_location_name}/de/{file_name}"))
        else:
            print(f"No CSV files exists in {storage_location_name} cloud storage location")
    return dataframe_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read csv from Azure Storage

# COMMAND ----------

def read_csv_azure_file(mount_point,dataframe_name):
    """
    Creates dataframe by reading csv files from azure blob storage.

    Args:
        mount_point (str): mount point specific to container.
        dataframe_name (str): The name of the designated dataframe name.
        file_list (list): Name of the files to be read from storage location saved in a list.

    Returns:
        DataFrame: A spark dataframe.
    """
    dataframe_name = (spark.read.format("csv")\
    .option("mode", "PERMISSIVE")\
    .option("quote",' ')\
    .option("escape",'"')\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .option("delimiter", "|")\
    .load(f"{mount_point}"))
    return dataframe_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta file from Azure Storage

# COMMAND ----------

def read_delta_azure_file(mount_point,dataframe_name):
    """
    Creates dataframe by reading delta files from azure blob storage.

    Args:
        mount_point (str): mount point specific to container.
        dataframe_name (str): The name of the designated dataframe name.
        file_list (list): Name of the files to be read from storage location saved in a list.

    Returns:
        DataFrame: A spark dataframe.
    """
    dataframe_name = (spark.read.format("delta")\
    .load(f"{mount_point}"))
    return dataframe_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Casts all columns into string with col method

# COMMAND ----------

def cast_to_string(df):
    from pyspark.sql.functions import col
    """
    Casts columns of tabular dataframe to string format 

    Args:
        dataframe_name (df): dataframe in dataframe format.

    Returns:
        DataFrame: With all columns casted to string
    """
    df = df.select([col(f"`{c}`").cast("string") for c in df.columns])
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Casting attiributes mapping

# COMMAND ----------

casting_dict = {"eventdfraw" : {"user_id":"IntegerType", "event_time":"DateType"}, "itemdfraw":{"id":"IntegerType", "price":"FloatType"}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Renaming attiributes mapping

# COMMAND ----------

mapping_dict = {"eventdfraw" : {"parameter_name":"sub_event_name", "parameter_value":"sub_event_name_value"}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rename dataframe attributes function

# COMMAND ----------

def rename_columns(dataframe_name,df_name,mapping_dict = {}):
    """
	    Renames the columns of a PySpark DataFrame based on the provided configuration. All Notebook names are defined in namespace of related config file section
	    e.g. F_MES_ACTIVITIES_COLUMN_RENAME:
                PLANT:PLANT>
                DESCRIPTION:OPERATION>
	    Args:
	        DataFrame (pyspark.sql.DataFrame): The DataFrame to rename columns.
            df_name (str): String name of dataframe to call correct nested dict item
	    Returns:
	        pyspark.sql.DataFrame: The DataFrame with renamed columns.
	    Example:
	        >>> dataframe = spark.createDataFrame([(1, "John"), (2, "Jane")], ["id", "name"])
	        >>> rename_columns_with_config_file(dataframe, config)
	        DataFrame[user_id: int, user_name: string]
    """
    for key,value in mapping_dict[f"{df_name}"].items():
        dataframe_name = dataframe_name.withColumnRenamed((key),(value))
    return dataframe_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Partition column mapping

# COMMAND ----------

partition_dict = {"eventdfraw" : {"event_time": {"event_time_year" :"year"}}}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save dataframe into hive metastore schema

# COMMAND ----------

def create_schema(schema_name,location_name):
    """
    Checks if a schema exists in the spark catalog.

    Args:
        location_name (str): catalog name to save schema on
        schema_name (str): The name of the schema to check.

    Returns:
        DataFrame: Empty DF creates schema on defined catalog if not exits
    """
    return spark.sql(f"CREATE SCHEMA IF NOT EXISTS  {location_name}.{schema_name}")


def check_if_table_exists(schema_name, table_name):
    """
    Checks if a table exists in the spark catalog.

    Args:
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    return spark.catalog.tableExists(f"hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")

def write_to_managed_table(df, table_name, schema_name, location_name, partition_cols = [] ,mode = "overwrite"):
    """
    Writes a DataFrame to a managed table in Delta Lake.

    If the table exists and mode is overwrite, it performs an overwrite operation.
    Otherwise, it either creates a new table or appends transactions to table based on the `mode` parameter.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to write to the table.
        table_name (str): The name of the target table.
        schema_name (str): The schema name of target table.
        location_name (str): Catalog name to save schema on
        mode (str, optional): The write mode.
    """
    #create schema if not exists
    create_schema(schema_name,location_name)
    # check if the table exists
    if check_if_table_exists(schema_name, table_name):
        print(f"Table exists on hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
        if mode == "overwrite":
            print(f"Overwriting all transactions on managed table hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
            df.write.format("delta").partitionBy(partition_cols).option("delta.columnMapping.mode", "name").mode(mode).saveAsTable(f"hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
        else:
            print(f"Appending all transactions on managed table hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
            df.write.format("delta").partitionBy(partition_cols).option("delta.columnMapping.mode", "name").mode(mode).saveAsTable(f"hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
    else:
        print(f"Writing to managed table hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
        df.write.format("delta").partitionBy(partition_cols).option("delta.columnMapping.mode", "name").saveAsTable(f"hive_metastore.{schema_name}.{table_name}_gold_layer_managed_table")
