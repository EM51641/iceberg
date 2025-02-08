#!/usr/bin/env python3
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
# import trino

def create_spark_session():
    spark = (
        SparkSession.builder
        .appName("IcebergIngestion")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # REST catalog
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
        .config("spark.sql.catalog.rest.uri", "http://rest:8181")
        .config("spark.sql.catalog.rest.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.rest.s3.access-key-id", "admin")
        .config("spark.sql.catalog.rest.s3.secret-access-key", "password")
        # S3/MinIO settings
        #.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        #.config("spark.hadoop.fs.s3a.access.key", "admin")
        #.config("spark.hadoop.fs.s3a.secret.key", "password")
        .getOrCreate()
    )
    return spark

def ingest_data(spark, num_rows=1000000):
    print("Creating namespace in REST catalog if not exists...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS rest.default")

    print("Creating Iceberg table in REST catalog...")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS rest.default.big_data (
            id BIGINT,
            value DOUBLE
        ) USING iceberg
    """)
# 
    print(f"Ingesting {num_rows} rows into the table using Spark...")
    df = spark.range(0, num_rows).withColumn("value", (rand() * 100))
    
    df.write.format("iceberg").mode("append").save("rest.default.big_data")
    print("Data ingestion completed.")

def query_with_spark(spark):
    print("Retrieving data using Spark:")
    df_spark = spark.read.format("iceberg").load("rest.default.big_data")
    df_spark.show(5)
    count = df_spark.count()
    print(f"Spark: Total row count in table: {count}")

# def query_with_presto():
#     # Connect to Presto (Trino). Ensure that port 8080 is correctly mapped.
#     print("Retrieving aggregation using Presto:")
#     conn = trino.dbapi.connect(
#         host="localhost",
#         port=8080,
#         user="test",  # Presto/Trino user (can be arbitrary)
#         catalog="iceberg",  # Using the catalog defined in presto-catalog/iceberg.properties
#         schema="default",
#     )
#     cur = conn.cursor()
#     # Query the table created in the Hive catalog. In Presto, the table name is "big_data".
#     cur.execute("SELECT count(*) FROM big_data")
#     result = cur.fetchone()
#     print(f"Presto: Total rows in table: {result[0]}")


def main():
    spark = create_spark_session()
    time.sleep(5)  # Wait a few seconds to ensure all catalog connections are ready

    # Ingest data using Spark.
    ingest_data(spark, num_rows=1000000)  # Adjust num_rows to simulate a high-volume ingestion
    # query_with_spark(spark)
# 
    # # Pause to ensure the data is committed and visible for external queries.
    # time.sleep(5)
    # query_with_presto()

    spark.stop()

if __name__ == "__main__":
    main()
    time.sleep(1000)