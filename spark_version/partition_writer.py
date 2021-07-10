from pathlib import Path

from pyspark.sql import SparkSession, functions
import schema


_DATA_FOLDER_PATH = Path(__file__).parent.parent / "data"


if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Partition Reader").getOrCreate()
    visits_json_path = _DATA_FOLDER_PATH / "visits.json"

    visits_df = spark.read.json(path=str(visits_json_path), schema=schema.EventLog)

    current_datetime = functions.current_date()
    visits_df_with_date_columns = (
        visits_df.withColumn("year", functions.year(current_datetime))
            .withColumn("month", functions.month(current_datetime))
            .withColumn("day", functions.dayofmonth(current_datetime))
    )

    (
        visits_df_with_date_columns.write
            .partitionBy(["year", "month", "day"])
            .json(path="/tmp/workspace/spark-partitioned", mode="overwrite")
    )
