# Databricks notebook source
# MAGIC %md
# MAGIC # Dim-taulujen luonti ja fact-taulun päivitys

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime

# COMMAND ----------

parameter_table = spark.table('vr_hopea.pipeline_parameters')

is_first_run = (
    parameter_table    
    .filter(parameter_table.param_name == 'is_first_run')
    .select('param_value')
    .first()[0]
)

print(f'Parametri: {is_first_run = }')


# COMMAND ----------

silver_df = spark.table('vr_hopea.vr_processed')
display(silver_df.limit(5))

# COMMAND ----------

dim_date = spark.table('workspace.vr_hopea.dim_date')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Luodaan dim-taulut
# MAGIC ### Otetaan asemat nimien ja lyhenteiden kera csv-tiedostosta ja yhdistetään asemien nimet alkuperäiseen asemadataan -> Muodostetaan dim_stations

# COMMAND ----------

stations_df = spark.read.csv('/Volumes/workspace/vr_hopea/files/VR_traffic_points.csv', header=True)
display(stations_df.limit(5))

stations_dim_df =  (
    silver_df
    .select('station_short_code')
    .dropDuplicates()
    .withColumn('station_id', F.monotonically_increasing_id())
    .withColumn('updated_at', F.current_timestamp())
)

dim_stations = stations_dim_df.join(stations_df, stations_dim_df.station_short_code == stations_df.stationShortCode, 'left').drop('stationShortCode')
dim_stations.write.mode('overwrite').saveAsTable('vr_hopea.dim_stations')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Muodostetaan dim_operators

# COMMAND ----------

dim_operators = (
  silver_df
  .select('operator_short_code')
  .dropDuplicates()
  .withColumn('operator_id', F.monotonically_increasing_id())
  .withColumn('updated_at', F.current_timestamp())
)
dim_operators.write.mode('overwrite').saveAsTable('vr_hopea.dim_operators')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Muodostetaan dim_trains

# COMMAND ----------

dim_trains =  (
    silver_df
    .select('train_number', 'train_category', 'train_type', 'commuter_line_id')
    .dropDuplicates()
    .withColumn('train_id', F.monotonically_increasing_id())
    .withColumn('updated_at', F.current_timestamp())
)

dim_trains.write.mode('overwrite').saveAsTable('vr_hopea.dim_trains')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Luodaan fact-taulu / lisätään uudet rivit deltatauluun

# COMMAND ----------

# Jos dataputki ajetaan ensikertaa, muodostetaan fact
if is_first_run:
    fact_trains = (
        silver_df
        .join(dim_trains, ['train_number', 'train_category', 'train_type', 'commuter_line_id'])
        .join(dim_operators, ['operator_short_code'])
        .join(dim_stations, ['station_short_code'])
        .join(dim_date, silver_df.departure_date == dim_date.date)
        .select(
            'event_nk',
            'cancelled',
            'event_type',
            'scheduled_time',
            'actual_time',
            'difference_in_minutes',
            'difference_in_seconds',
            'actual_hour',
            'station_id',
            'operator_id',
            'train_id'
        )
    )
    fact_trains.write.mode('overwrite').saveAsTable('vr_hopea.fact_trains')

    # Change pipeline parameter value
    delta = DeltaTable.forName(spark, 'vr_hopea.pipeline_parameters')
    delta.update(
        condition = "param_name = 'is_first_run'", set = {'param_value': F.lit(0)}
    )

    
# Dataputken viikottaiset ajot ensiajon jälkeen
else:
    fact_event_nks = spark.table('vr_hopea.fact_trains').select('event_nk')

    # left_anti -> kaikki rivit hopean koontitaulusta (vasemman puolisesta), joilla ei ole vastinetta fact-taulussa -> eli kaikki uudet rivit koontitaulusta fact-tauluun
    new_fact_rows = (
        silver_df.alias('s')
        .join(fact_event_nks.alias('f'),
            F.col('s.event_nk') == F.col('f.event_nk'), 'left_anti')
        )
    
    fact_batch = (
        new_fact_rows
        .join(dim_trains, ['train_number', 'train_category', 'train_type', 'commuter_line_id'])
        .join(dim_operators, ['operator_short_code'])
        .join(dim_stations, ['station_short_code'])
        .join(dim_date, new_fact_rows.departure_date == dim_date.date)
        .select(
            'event_nk',
            'cancelled',
            'event_type',
            'scheduled_time',
            'actual_time',
            'difference_in_minutes',
            'difference_in_seconds',
            'actual_hour',
            'station_id',
            'operator_id',
            'train_id'
        )
    )
    
    fact_table = DeltaTable.forName(spark, 'vr_hopea.fact_trains')
    
    (
        fact_table.alias('target')
        .merge(
            fact_batch.alias('source'),
            'target.event_nk = source.event_nk'
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

print('Data kirjoitettu dim- ja fact-tauluihin!')