# Databricks notebook source
from pyspark.sql import Row
from pyspark.sql.functions import current_timestamp, lit

files = dbutils.fs.ls('/Volumes/workspace/vr_pronssi/vr_pronssi_volume')
latest_file = sorted(files, key=lambda x: x.modificationTime, reverse=True)[0]

log_df = spark.table('vr_pronssi.ingestion_log')
row_count = log_df.count()

if row_count == 0:
    latest_file_path = None
else:
    latest_file_path = spark.sql('''
        SELECT file_path
        FROM workspace.vr_pronssi.ingestion_log
        ORDER BY processed_at DESC
        LIMIT 1                           
    ''').collect()[0]['file_path']

if latest_file_path == latest_file.path:
    dbutils.notebook.exit('EI SUORITETA NOTEBOOKIA. DELTATAULUSSA ON JO TARVITTAVA DATA')



# COMMAND ----------

df = spark.read.parquet(latest_file.path)
if df.isEmpty():
  raise Exception("VALIDOINTI EPÄONNISTUI: TUOREIMMASSA TIEDOSTOSSA EI OLLUT DATAA")

display(df.limit(5))

# COMMAND ----------

df.write.format('delta').mode('append').saveAsTable('vr_pronssi.vr_raw')

# COMMAND ----------

# DBTITLE 1,Cell 3
log_data = [Row(file_path = latest_file.path)]
log_df = spark.createDataFrame(log_data)
row_count = df.count()

log_df.withColumn('processed_at', current_timestamp()) \
    .withColumn('new_rows', lit(row_count)) \
    .write \
    .format('delta') \
    .mode('append') \
    .saveAsTable('vr_pronssi.ingestion_log')

dbutils.notebook.exit('PRONSSITASON DATA JA LOKI KIRJOITETTU ONNISTUNEESTI')