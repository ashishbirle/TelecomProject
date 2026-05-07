from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
from pyspark.sql import SparkSession

from pyspark.sql.functions import col, to_date

spark = SparkSession.builder.appName("MyApp").getOrCreate()

# ----------------------------
# Spark Config Tuning
# ----------------------------

spark.conf.set("spark.sql.shuffle.partitions", 300)  # tune based on cluster
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)  # we control broadcast manually

# ----------------------------
# Load + Repartition Early
# ----------------------------

df = spark.read.option("header", True).option("inferSchema", True).csv(r"C:\Users\Ashish Birle\OneDrive\Desktop\Exponent IT\Data Engineering\Telecom Project Files\telecom data for pyspark.csv", header=True, inferSchema=True)

df = df.repartition("Date", "Cell ID")  # critical for large telecom datasets

# ----------------------------
# Column Standardization
# ----------------------------

df = df.toDF(*[
    c.strip()
     .replace(" ", "_")
     .replace("%", "pct")
     .replace("-", "_")
     .replace("[", "")
     .replace("]", "")
    for c in df.columns
])

# ----------------------------
# Casting
# ----------------------------

numeric_cols = [
    "Cell_Availabililty", "Session_Setup_Success_Rate", "VoLTE_Drop_Rate_pct",
    "Handover_Success_Rate_pct", "Traffic_24Hrs_GB", "DL_PRB_utilizationpct",
    "CQI", "IP_Throughput_Mbps", "RRC_Connected_Users",
    "Peak_RRC_Connected", "Average_TA", "Mute_Call_Ratepct"
]

for col in numeric_cols:
    df = df.withColumn(col, F.col(col).cast("double"))

df = df.withColumn("Date", to_date ("Date", "dd-MMM-yy"))

# ----------------------------
# Data Cleaning
# ----------------------------

df = df.dropna(subset=["Cell_ID", "Date"]).fillna(0)  

print("---------- Cleaned Data -----------")
print(df.show(5))
print()

df.cache()

# ----------------------------
# Rolling Window
# ----------------------------
window_spec = Window.partitionBy("Cell_ID").orderBy("Date").rowsBetween(-6, 0)

df = df.withColumn("rolling_cssr", F.avg("Session_Setup_Success_Rate").over(window_spec)).withColumn("rolling_drop", F.avg("VoLTE_Drop_Rate_pct").over(window_spec)).withColumn("rolling_tp", F.avg("IP_Throughput_Mbps").over(window_spec))

print("---------- Rolling Window---------- ")
print(df.show(5))
print()

# ----------------------------
# Z-score Stats (Broadcast Join)
# ----------------------------

stats = df.groupBy("Cell_ID").agg(
    F.mean("IP_Throughput_Mbps").alias("mean_tp"), 
    F.stddev("IP_Throughput_Mbps").alias("std_tp")
)

df = df.join(broadcast(stats), "Cell_ID")

df = df.withColumn(
    "zscore_tp", (F.col("IP_Throughput_Mbps") - F.col("mean_tp")) / F.col("std_tp")
).withColumn(
    "tp_anomaly",
    F.when(F.abs(F.col("zscore_tp")) > 2,1).otherwise(0)
)

print("---------- Z-score Stats -----------")
print(df.show(5))
print()

# ----------------------------
# Min-Max WITHOUT collect()
# ----------------------------
minmax_df = df.select([
    F.min(c).alias(f"{c}_min") for c in numeric_cols] + [
        F.max(c).alias(f"{c}_max") for c in numeric_cols
    ]
)

# Broadcast instead of collect
minmax_df = broadcast(minmax_df)
df = df.crossJoin(minmax_df)

print("--------- Network Attributes Min and Max ---------")
print(df.show(5))
print()

def normalize(col):
    return (
        (F.col(col) - F.col(f"{col}_min")) /
        (F.col(f"{col}_max") - F.col(f"{col}_min") + F.lit(1e-6))
    )

df = df.withColumn("norm_cssr", normalize("Session_Setup_Success_Rate")) \
        .withColumn("norm_drop", 1 - normalize("VoLTE_Drop_Rate_pct")) \
        .withColumn("norm_ho", normalize("Handover_Success_Rate_pct")) \
        .withColumn("norm_prb", 1 - normalize("DL_PRB_utilizationpct")) \
        .withColumn("norm_cqi", normalize("CQI")) \
        .withColumn("norm_tp", normalize("IP_Throughput_Mbps"))

# ----------------------------
# Health Score
# ----------------------------

df = df.withColumn(
    "health_score",
    (
        F.col("norm_cssr") * 0.2 +
        F.col("norm_drop") * 0.2 +
        F.col("norm_ho") * 0.15 +
        F.col("norm_prb") * 0.1 +
        F.col("norm_cqi") * 0.15 +
        F.col("norm_tp") * 0.2
    )
)

# ----------------------------
# Persist before heavy reuse
# ----------------------------
df.persist()

print("------- DataFrame with all modifications -------")
print(df.show())

# ----------------------------
# Ranking (Avoid full shuffle skew)
# ----------------------------

rank_window = Window.partitionBy("Date").orderBy(F.col("health_score").asc())

df_ranked = df.withColumn("rank", F.row_number().over(rank_window))

worst_cells = df_ranked.filter(F.col("rank") <= 10)

print("--------- Ranked Cells ----------")
print(df_ranked.show())
print()

print("--------- Worst Cells -----------")
print(worst_cells.show())
print()


# ----------------------------
# Write Efficiently
# ----------------------------

# worst_cells.write.mode("overwrite") \
#     .partitionBy("Date") \
#     .parquet("output/worst_cells")