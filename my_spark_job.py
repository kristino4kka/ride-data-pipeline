"""Aggregate taxi trips by payment method and write the result to ClickHouse.

Required environment variables:
    CLICKHOUSE_HOST
    CLICKHOUSE_USER
    CLICKHOUSE_PASSWORD

Optional environment variables:
    CLICKHOUSE_PORT (default: 8443)
    CLICKHOUSE_DATABASE (default: playground_<CLICKHOUSE_USER>)
"""

import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("TaxiAggregation")
        .config("fs.s3a.endpoint", "storage.yandexcloud.net")
        .getOrCreate()
    )

    clickhouse_host = os.environ["CLICKHOUSE_HOST"]
    clickhouse_user = os.environ["CLICKHOUSE_USER"]
    clickhouse_password = os.environ["CLICKHOUSE_PASSWORD"]
    clickhouse_port = os.getenv("CLICKHOUSE_PORT", "8443")
    clickhouse_database = os.getenv(
        "CLICKHOUSE_DATABASE",
        f"playground_{clickhouse_user}",
    )

    jdbc_url = (
        f"jdbc:clickhouse://{clickhouse_host}:{clickhouse_port}/"
        f"{clickhouse_database}?ssl=true"
    )

    input_path = "s3a://da-plus-dags/project_04/taxi_data.parquet"

    trips = spark.read.parquet(input_path)

    payment_summary = (
        trips.groupBy("payment_type")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare").alias("avg_fare"),
            F.avg("tips").alias("avg_tips"),
            F.sum("trip_total").alias("sum_revenue"),
        )
    )

    (
        payment_summary.write.format("jdbc")
        .option("url", jdbc_url)
        .option("user", clickhouse_user)
        .option("password", clickhouse_password)
        .option("dbtable", "taxi_payment_summary")
        .mode("overwrite")
        .save()
    )

    spark.stop()


if __name__ == "__main__":
    main()
