from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

class DataFrameTransformer:
    def __init__(self, df: DataFrame, is_stream: bool = False):
        self.df = df
        self.is_stream = is_stream
        self._original_types = {}

    def deduplicate(self, subset_cols: list, order_by_col: str = None, timestamp_format: str = "yyyy-MM-dd HH:mm:ss" ,watermark="1 hour"):
        """Deduplicate safely for both batch and streaming DataFrames"""
        if self.is_stream:
            if order_by_col is None:
                raise ValueError("Streaming DF deduplicate requires a timestamp column for watermark")

            # 1️⃣ Detect and store original column type
            field = next((f for f in self.df.schema.fields if f.name == order_by_col), None)
            if not field:
                raise ValueError(f"Column '{order_by_col}' not found in DataFrame.")
            original_type = field.dataType
            self._original_types[order_by_col] = original_type

            # 2️⃣ Convert to proper timestamp before watermark
            if isinstance(original_type, DateType):
                self.df = self.df.withColumn(order_by_col, col(order_by_col).cast("timestamp"))
            elif isinstance(original_type, StringType):
                self.df = self.df.withColumn(order_by_col, to_timestamp(col(order_by_col), timestamp_format))
            elif isinstance(original_type, IntegerType) or isinstance(original_type, LongType):
                # Treat int/long as epoch seconds
                self.df = self.df.withColumn(order_by_col, (col(order_by_col) / 1000).cast("timestamp"))

            # 3️⃣ Apply watermark and deduplicate
            self.df = (
                self.df
                .withWatermark(order_by_col, watermark)
                .dropDuplicates(subset=subset_cols)
            )

            # 4️⃣ Revert column back to original type
            if isinstance(original_type, DateType):
                self.df = self.df.withColumn(order_by_col, col(order_by_col).cast("date"))
            elif isinstance(original_type, StringType):
                self.df = self.df.withColumn(order_by_col, date_format(col(order_by_col), timestamp_format))
            elif isinstance(original_type, IntegerType) or isinstance(original_type, LongType):
                self.df = self.df.withColumn(order_by_col, (col(order_by_col).cast("double") * 1000).cast("long"))

        else:
            # Batch deduplication logic
            if order_by_col:
                window_spec = Window.partitionBy(*subset_cols).orderBy(col(order_by_col).desc())
            else:
                window_spec = Window.partitionBy(*subset_cols).orderBy(col(subset_cols[0]))

            self.df = (
                self.df.withColumn("row_num", row_number().over(window_spec))
                       .filter(col("row_num") == 1)
                       .drop("row_num")
            )

        return self

    def change_case(self, columns: list = None, to_upper: bool = True, column_cases: dict = None):
        if column_cases:
            for c, case in column_cases.items():
                if c in self.df.columns:
                    self.df = self.df.withColumn(c, upper(col(c)) if case.lower() == "upper" else lower(col(c)))
        else:
            if columns is None:
                columns = [f.name for f in self.df.schema.fields if f.dataType.simpleString() == 'string']
            for c in columns:
                self.df = self.df.withColumn(c, upper(col(c)) if to_upper else lower(col(c)))
        return self

    def drop_columns(self, columns: list):
        columns_to_drop = [c for c in columns if c in self.df.columns]
        if self.is_stream:
            remaining_cols = [c for c in self.df.columns if c not in columns_to_drop]
            self.df = self.df.select(*remaining_cols)
        else:
            self.df = self.df.drop(*columns_to_drop)
        return self

    def get_df(self) -> DataFrame:
        return self.df


     
###############################################  Use Case   ######################################

# transformer = DataFrameTransformer(df_DimUser, is_stream=True)

# df_DimUser = (
#     transformer
#     .deduplicate(subset_cols=["user_id"], order_by_col="date" ,watermark="3 days")  # date can be DATE, STRING, or INT
#     .change_case(columns=["user_name"])
#     .drop_columns(["_rescued_data"])
#     .get_df()
# )

# df_DimUser.display()