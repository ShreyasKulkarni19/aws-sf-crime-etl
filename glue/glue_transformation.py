from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_timestamp,
    year,
    month,
    dayofmonth,
    hour,
    date_format,
    when,
    lit
)

spark = SparkSession.builder.appName("CrimeTransformJob").getOrCreate()

RAW_PATH = "s3://your-raw-bucket/incremental/"
CURATED_PATH = "s3://your-curated-bucket/"

# Read JSON
df = spark.read.json(RAW_PATH)

# Parse timestamps
df = df.withColumn(
    "incident_timestamp",
    to_timestamp(col("incident_datetime"))
)

df = df.withColumn(
    "report_timestamp",
    to_timestamp(col("report_datetime"))
)

# Derive date features
df = df.withColumn("incident_year", year("incident_timestamp"))
df = df.withColumn("incident_month", month("incident_timestamp"))
df = df.withColumn("incident_day", dayofmonth("incident_timestamp"))
df = df.withColumn("incident_hour", hour("incident_timestamp"))

df = df.withColumn(
    "incident_quarter",
    ((col("incident_month") - 1) / 3 + 1).cast("int")
)

df = df.withColumn(
    "incident_day_of_week",
    date_format("incident_timestamp", "EEEE")
)

df = df.withColumn(
    "is_weekend",
    when(date_format("incident_timestamp", "u").isin("6","7"), lit(True))
    .otherwise(lit(False))
)

# Cast numeric columns
df = df.withColumn("latitude", col("latitude").cast("double"))
df = df.withColumn("longitude", col("longitude").cast("double"))

df = df.withColumn("supervisor_district",
                   col("supervisor_district").cast("int"))

df = df.withColumn("supervisor_district_2012",
                   col("supervisor_district_2012").cast("int"))

# Violent crime flag
violent_crimes = [
    "HOMICIDE",
    "ASSAULT",
    "ROBBERY",
    "KIDNAPPING",
    "SEX_OFFENSES"
]

df = df.withColumn(
    "is_violent_crime",
    when(col("incident_category").isin(violent_crimes), lit(True))
    .otherwise(lit(False))
)

# Peak hour flag
df = df.withColumn(
    "is_peak_hour",
    when(col("incident_hour").between(17,21), lit(True))
    .otherwise(lit(False))
)

# Deduplicate
df = df.dropDuplicates(["incident_id"])

# Write Parquet partitioned
df.write \
    .mode("append") \
    .partitionBy("incident_year","incident_month","incident_day") \
    .parquet(CURATED_PATH)

spark.stop()