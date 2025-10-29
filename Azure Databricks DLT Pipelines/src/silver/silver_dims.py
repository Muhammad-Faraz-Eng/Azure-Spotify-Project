# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import sys, os

# ----------------------------------------
# 📌 Add repo root for utils imports
# ----------------------------------------
repo_root = os.path.join(os.getcwd(), "../../") 
sys.path.append(repo_root)
from utils.transformations import DataFrameTransformer

# COMMAND ----------

# ----------------------------------------
# 📌 Base paths and catalog
# ----------------------------------------
bronze_base = "abfss://bronze@spotifyazurefarazs.dfs.core.windows.net/"
silver_base = "abfss://silver@spotifyazurefarazs.dfs.core.windows.net/"
catalog_name = "spotify_faraz"
schema_name = "silver"

# ----------------------------------------
# 📌 Get all folders from Bronze (skip _cdc)
# ----------------------------------------
all_folders = dbutils.fs.ls(bronze_base)
folders = [f.path for f in all_folders if not f.name.endswith("_cdc/")]
print("✅ Folders detected:", folders)

# COMMAND ----------

# ----------------------------------------
# 📌 Dictionaries to store DataFrames and streaming queries
# ----------------------------------------
dfs = {}
streams = {}

# ----------------------------------------
# 📌 Generic streaming loader function
# ----------------------------------------
def load_streaming_df(folder_path):
    folder_name = folder_path.strip("/").split("/")[-1]
    checkpoint_path = f"{silver_base}{folder_name}/checkpoint"

    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("schemaEvolutionMode", "AddNewColumns") \
        .option("cloudFiles.schemaLocation", checkpoint_path) \
        .load(folder_path)
    
    return folder_name, df

# Load all Bronze folders into dictionary
for folder in folders:
    name, df = load_streaming_df(folder)
    dfs[name] = df

# COMMAND ----------

# ----------------------------------------
# 📌 Transformation functions
# ----------------------------------------
def transform_dim_user(df):
    df = df.repartition("user_id")  # reduce shuffle for dedup
    return DataFrameTransformer(df, is_stream=True) \
        .deduplicate(["user_id"], "updated_at") \
        .change_case(["user_name"]) \
        .drop_columns(["_rescued_data"]) \
        .get_df()

def transform_dim_date(df):
    return DataFrameTransformer(df, is_stream=True) \
        .deduplicate(["date_key"], "date") \
        .change_case(["day"]) \
        .drop_columns(["_rescued_data"]) \
        .get_df()

def transform_dim_artist(df):
    df = df.repartition("artist_id")
    return DataFrameTransformer(df, is_stream=True) \
        .deduplicate(["artist_id"], "updated_at") \
        .change_case(["country"]) \
        .drop_columns(["_rescued_data"]) \
        .get_df()

def transform_dim_track(df):
    df = df.withColumn(
        'durationFlag',
        when(col('duration_sec') < 150, "low")
        .when(col('duration_sec') < 300, "medium")
        .otherwise("high")
    ).withColumn("track_name", regexp_replace(col('track_name'), "-", " "))
    
    df = df.repartition("track_id")  # reduce shuffle
    return DataFrameTransformer(df, is_stream=True) \
        .deduplicate(["track_id"], "updated_at") \
        .change_case(["album_name"]) \
        .drop_columns(["_rescued_data"]) \
        .get_df()

def transform_fact_stream(df):
    df = df.repartition("stream_id")  # reduce shuffle
    return DataFrameTransformer(df, is_stream=True) \
        .deduplicate(["stream_id"], "stream_timestamp") \
        .change_case(["device_type"]) \
        .drop_columns(["_rescued_data"]) \
        .get_df()

# Transformation mapping
transformations = {
    "DimUser": transform_dim_user,
    "DimDate": transform_dim_date,
    "DimArtist": transform_dim_artist,
    "DimTrack": transform_dim_track,
    "FactStream": transform_fact_stream
}

# COMMAND ----------

# ----------------------------------------
# 📌 Apply transformations dynamically
# ----------------------------------------
for table_name, df in dfs.items():
    print(f"🔹 Processing table: {table_name}")
    if table_name in transformations:
        dfs[table_name] = transformations[table_name](df)
        print(f"   ✅ Transformation applied for {table_name}")
    else:
        print(f"   ⚠️ No transformation defined for {table_name}, passing through")


# COMMAND ----------

# ----------------------------------------
# 📌 Generic streaming writer function
# ----------------------------------------
def write_stream_to_silver(table_name, df):
    output_path = f"{silver_base}{table_name}/data"
    checkpoint_path = f"{silver_base}{table_name}/checkpoint"
    table_full_name = f"{catalog_name}.{schema_name}.{table_name}"

    query = df.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .option("mergeSchema", "true") \
        .option("path", output_path) \
        .trigger(once=True) \
        .toTable(table_full_name)
    
    return query

# Write all transformed DataFrames to Silver
for table_name, df in dfs.items():
    print(f"💾 Writing {table_name} to Silver Delta...")
    streams[table_name] = write_stream_to_silver(table_name, df)

print("\n🎯 All streams are now writing to Silver Delta and registered in Unity Catalog!")