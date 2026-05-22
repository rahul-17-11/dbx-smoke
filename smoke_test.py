import os
DATA_BUCKET = "dbx-data-" + spark.conf.get(
    "spark.databricks.clusterUsageTags.cloudProviderAccountId", "UNKNOWN")
print("Data bucket:", DATA_BUCKET)

# COMMAND ----------
import subprocess
print(subprocess.run(["aws","sts","get-caller-identity"], capture_output=True, text=True).stdout)

# COMMAND ----------
dbutils.fs.ls(f"s3a://{DATA_BUCKET}/raw/")

# COMMAND ----------
df = (spark.read.option("header", True).option("inferSchema", True)
      .csv(f"s3a://{DATA_BUCKET}/raw/sample.csv"))
df.show()

# COMMAND ----------
out = df.selectExpr("id","city","temperature_c","temperature_c * 9/5 + 32 AS temperature_f")
(out.write.mode("overwrite").parquet(f"s3a://{DATA_BUCKET}/processed/temperature_f/"))
print("Wrote:", f"s3a://{DATA_BUCKET}/processed/temperature_f/")
dbutils.fs.ls(f"s3a://{DATA_BUCKET}/processed/temperature_f/")
