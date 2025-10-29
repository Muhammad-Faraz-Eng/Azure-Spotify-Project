import dlt


@dlt.table
def FactStreamStg():
    df = df.readstream.table("spotify_faraz.silver.factstream")
    return df

dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow( 
  target = "factstream",
  source = "FactStreamStg",
  keys = ["stream_id"],
  sequence_by="stream_timestamp"
  stored_as_scd_type = 1,
  track_history_except_column_list = None,
  name =None,
  once = False
)