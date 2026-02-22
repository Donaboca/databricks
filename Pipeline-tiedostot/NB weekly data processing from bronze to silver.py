# Databricks notebook source
from pyspark.sql import functions as F
from datetime import datetime

silver_log_df = spark.table("vr_hopea.ingestion_log")
last_write_time = silver_log_df.agg({'latest_data_date': 'max'}).first()[0]

expected_columns = [
    'departureDate',
    'trainNumber',
    'trainCategory',
    'trainType',
    'commuterLineID',
    'stationShortCode',
    'type',
    'scheduledTime',
    'actualTime',
    'differenceInMinutes',
    'operatorUICCode',
    'operatorShortCode',
    'cancelled'
]

bronze_df_new_rows = spark.table("vr_pronssi.vr_raw").where(F.col("departureDate") > last_write_time)
if bronze_df_new_rows.isEmpty():
  raise Exception("PRONSSITASOLLA EI OLLUT UUSIA RIVEJÄ")

actual_columns = bronze_df_new_rows.columns

if not expected_columns == actual_columns:
  dbutils.notebook.exit("SARAKKEET EIVÄT TÄSMÄNNEET")

# COMMAND ----------

last_write_time

# COMMAND ----------

display(bronze_df_new_rows.limit(5))

# COMMAND ----------

bronze_df_new_rows.printSchema()

# COMMAND ----------

# DBTITLE 1,Untitled
col_changes = [
    F.to_date(F.col("departureDate"), "yyyy-MM-dd").alias('departure_date'),
    F.col('trainNumber').cast('int').alias('train_number'),
    F.col('trainCategory').alias('train_category'),
    F.col('trainType').alias('train_type'),
    F.col('commuterLineID').alias('commuter_line_id'),
    F.col('stationShortCode').alias('station_short_code'),
    F.col('type').alias('event_type'),
    F.to_timestamp('scheduledTime', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias('scheduled_time'),
    F.to_timestamp('actualTime', "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias('actual_time'),
    F.col('differenceInMinutes').cast('int').alias('difference_in_minutes'),
    F.col('operatorUICCode').cast('int').alias('operator_uic_code'),
    F.col('operatorShortCode').alias('operator_short_code')
]

column_names_changed = ['departureDate', 'trainNumber', 'trainCategory', 'trainType', 'commuterLineID',
                         'stationShortCode', 'type', 'scheduledTime', 'actualTime', 'differenceInMinutes',
                         'operatorUICCode', 'operatorShortCode']

silver_df_new_rows = (
    bronze_df_new_rows
    .select('*', *col_changes)
    .drop(*column_names_changed)
)

# COMMAND ----------

display(silver_df_new_rows.limit(5))

# COMMAND ----------


avg_diff_time = silver_df_new_rows.select(
    (F.unix_timestamp('scheduled_time') - F.unix_timestamp('actual_time'))
    .alias('diff')
    ).where('diff IS NOT NULL') \
    .agg(F.avg('diff')) \
    .first()[0]

silver_df_new_rows = silver_df_new_rows.withColumn(
    'difference_in_seconds', 
    F.when(
        F.col('actual_time').isNotNull(),
        F.unix_timestamp('scheduled_time') - F.unix_timestamp('actual_time')
    )
    .when(
        F.col('difference_in_minutes').isNotNull(),
        - F.col('difference_in_minutes') * 60
    ).otherwise(
        avg_diff_time
        )
    ).withColumn(
        'event_nk',
        F.sha2(
            F.concat_ws(
                '||',
                F.col('train_number'),
                F.col('station_short_code'),
                F.col('event_type'),
                F.col('scheduled_time')
            ), 
            256
        )

    ).withColumn(
        'actual_hour',
        F.hour(F.coalesce('actual_time', 'scheduled_time'))
    )

# COMMAND ----------

display(silver_df_new_rows.limit(5))

# COMMAND ----------

silver_df_new_rows.write.mode("append").saveAsTable("vr_hopea.vr_processed")

# COMMAND ----------

now = datetime.now()
max_date = silver_df_new_rows.agg({'departure_date': 'max'}).first()[0]
row_count = silver_df_new_rows.count()

spark.sql(f"INSERT INTO vr_hopea.ingestion_log VALUES ('{now}', '{max_date}', '{row_count}')")

dbutils.notebook.exit("HOPEATASON DATA JA LOKI KIRJOITETTU ONNISTUNEESTI")

