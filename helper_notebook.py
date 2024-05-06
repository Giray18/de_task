# Databricks notebook source
# MAGIC %md
# MAGIC ## Reads csv files from given s3 location, If no csv detected returns exception

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
# MAGIC ## Casts all columns into string with col method

# COMMAND ----------

# DBTITLE 1,Casts all columns into string with col method
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
