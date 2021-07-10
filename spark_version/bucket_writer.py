from pathlib import Path

from pyspark.sql import SparkSession

from spark_version import schema


_DATA_FOLDER_PATH = Path(__file__).parent.parent / "data"


if __name__ == '__main__':
    temp_warehouse = "/tmp/partitioning-applied/buckets-warehouse"
    spark = (
        SparkSession.builder.master("local[*]")
            .appName("Bucket Writer")
            .config("spark.sql.warehouse.dir", temp_warehouse).enableHiveSupport()
            .getOrCreate()
    )

    visits_json_path = str(_DATA_FOLDER_PATH / "visits.json")
    visits_df = spark.read.json(path=visits_json_path, schema=schema.EventLog)

    customers = [(529227, "user123"), (529234, "user246"), (529248, "user369")]
    customers_df = spark.createDataFrame(data=customers, schema=["user_id", "login"])

    customers_df.write.option("path", temp_warehouse).mode("overwrite").bucketBy(numBuckets=5, col="user_id").saveAsTable(name="users_bucketed")
    customers_df.write.option("path", temp_warehouse).mode("overwrite").saveAsTable(name="users_not_bucketed")

    visits_df.write.option("path", temp_warehouse).mode("overwrite").bucketBy(numBuckets=5, col="user_id").saveAsTable(name="visits_bucketed")
    visits_df.write.option("path", temp_warehouse).mode("overwrite").saveAsTable(name="visits_not_bucketed")

    tables = spark.catalog.listTables()
    print(tables)
