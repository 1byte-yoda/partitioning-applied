from pyspark.sql import SparkSession


if __name__ == '__main__':
    temp_warehouse = "/tmp/partitioning-applied/buckets-warehouse"
    spark = (
        SparkSession.builder.appName("Bucket Reader")
            .master(master="local[*]")
            .config("spark.sql.warehouse.dir", temp_warehouse).enableHiveSupport()
            .config("spark.sql.adaptive.enabled", False)
            .config("spark.sql.autoBroadcastJoinThreshold", -1)
            .getOrCreate()
    )

    tables = spark.catalog.listTables()
    print(tables)

    not_bucketed_query = spark.sql(
        "SELECT * FROM visits_not_bucketed vnb JOIN users_not_bucketed unb ON unb.user_id = vnb.user_id"
    )
    bucketed_query = spark.sql(
        "SELECT * FROM visits_bucketed vb JOIN users_bucketed ub ON ub.user_id = vb.user_id"
    )
    print("NOT BUCKETED EXECUTION PLAN")
    not_bucketed_query.explain(extended=True)

    print("BUCKETED EXECUTION PLAN")
    bucketed_query.explain(extended=True)
