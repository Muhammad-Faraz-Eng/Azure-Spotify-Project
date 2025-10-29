import dlt

expectations = {
  "rule1" : "user_id is not NULL"
}

@dlt.table
@dlt.expect_all_or_drop(expectations)
def DimUserStg():
    df = df.readstream.table("spotify_faraz.silver.dimuser")
    return df

dlt.create_streaming_table(
  name = "dimuser",
  expect_all_or_drop = expectations
  
)

dlt.create_auto_cdc_flow( 
  target = "dimuser",
  source = "DimUserStg",
  keys = ["user_id"],
  sequence_by="updated_at"
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name =None,
  once = False
)