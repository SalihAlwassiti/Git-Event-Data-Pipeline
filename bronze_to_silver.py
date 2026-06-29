from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType

target_schema = StructType([
    StructField("event_id", LongType(), True),
    StructField("event_type", StringType(), True),
    StructField("created_at", TimestampType(), True),
    StructField("actor_id", LongType(), True),
    StructField("actor_login", StringType(), True),
    StructField("repo_id", LongType(), True),
    StructField("repo_name", StringType(), True),
    StructField("org_id", LongType(), True),
    StructField("org_login", StringType(), True)
])

target_table = "github.default.github_silver"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table} (
        event_id BIGINT,
        event_type STRING,
        created_at TIMESTAMP,
        actor_id BIGINT,
        actor_login STRING,
        repo_id BIGINT,
        repo_name STRING,
        org_id BIGINT,
        org_login STRING
    )
""")

bronze_df = spark.table("github.default.github_bronze").filter(F.col("_airbyte_data").isNotNull())

parsed_df = bronze_df.select(
    F.get_json_object("_airbyte_data", "$.id").cast(LongType()).alias("event_id"),
    F.get_json_object("_airbyte_data", "$.type").alias("event_type"),
    F.get_json_object("_airbyte_data", "$.created_at").cast(TimestampType()).alias("created_at"),
    F.get_json_object("_airbyte_data", "$.actor.id").cast(LongType()).alias("actor_id"),
    F.get_json_object("_airbyte_data", "$.actor.login").alias("actor_login"),
    F.get_json_object("_airbyte_data", "$.repo.id").cast(LongType()).alias("repo_id"),
    F.get_json_object("_airbyte_data", "$.repo.name").alias("repo_name"),
    F.get_json_object("_airbyte_data", "$.org.id").cast(LongType()).alias("org_id"),
    F.get_json_object("_airbyte_data", "$.org.login").alias("org_login")
)

try:
    existing_silver_df = spark.table(target_table).select("event_id")
    new_records_df = parsed_df.join(existing_silver_df, on="event_id", how="left_anti")
    new_count = new_records_df.count()
except Exception as e:
    new_records_df = parsed_df
    new_count = new_records_df.count()

if new_count > 0:
    new_records_df.write.mode("append").format("delta").saveAsTable(target_table)
    display(new_records_df.limit(5))
