# Databricks notebook source
# DBTITLE 1,Reads csv files from given s3 location, If no csv detected returns exception
def read_csv_file(storage_location_name,dataframe_name,file_list = []):
    """
    Creates dataframe based on defined elements data_frame_names list.
    Reads files from cloud storage location conditionally by the file type 

    Args:
        dataframe_name (str): The name of the dataframe.
        storage_location_name (str): The name of the storage location name.
        file_list (list): Name of the files to be read from storage location saved in a list.

    Returns:
        DataFrame: True if the parameters exists, False otherwise.
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

def write_to_external_table():
    spark.sql("CREATE TABLE <catalog>.<schema>.<table-name> "
    "( "
    "  <column-specification> "
    ") "
    "USING delta "
    "LOCATION 's3://<table-location>' "
    "SELECT * from <format>.`s3://<path-to-files>`")

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
