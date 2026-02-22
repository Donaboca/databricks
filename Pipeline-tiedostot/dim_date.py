# Databricks notebook source
# MAGIC %md
# MAGIC # dim_daten luonti
# MAGIC - Oma jobi -> päivittymään päivittäin

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from datetime import datetime, date

# COMMAND ----------

parameter_table = spark.table('vr_hopea.pipeline_parameters')

dim_date_is_first_run = (
    parameter_table    
    .filter(parameter_table.param_name == 'dim_date_is_first_run')
    .select('param_value')
    .first()[0]
)

print(f'Parametri: {dim_date_is_first_run = }')

# COMMAND ----------

if dim_date_is_first_run == 1:
    min_date = datetime.strptime('2024-01-01', '%Y-%m-%d').date()
    print(min_date)

# COMMAND ----------

max_date = date.today()

# COMMAND ----------

if dim_date_is_first_run == 1:
    days = (max_date - min_date).days


# COMMAND ----------

if dim_date_is_first_run == 1:
    dim_date = (
        spark.range(0, days + 1)
        .withColumn('date', F.expr(f'date_add("{min_date}", cast(id as int))'))
        .withColumn('year', F.year('date'))
        .withColumn('month', F.month('date'))
        .withColumn('day', F.dayofmonth('date'))
        .withColumn('weekday', F.dayofweek('date'))
        .withColumn('weekday_name', F.date_format('date', 'EEEE'))
        .withColumn('is_weekend', (F.col('weekday')>=6))
        .withColumn('week_number', F.weekofyear('date'))
        .withColumn('quarter', F.quarter('date'))
        .withColumn('updated_at', F.current_timestamp())
    )

    dim_date.write.mode('overwrite').saveAsTable('vr_hopea.dim_date')
    
    # Change pipeline parameter value
    delta = DeltaTable.forName(spark, 'vr_hopea.pipeline_parameters')
    delta.update(
        condition = "param_name = 'dim_date_is_first_run'", set = {'param_value': F.lit(0)}
    )
    
else:
    
    new_date = (
        spark.range(1).select(
            max_date.alias('date'),
            F.year(max_date).alias('year'),
            F.month(max_date).alias('month'),
            F.dayofmonth(max_date).alias('day'),
            F.dayofweek(max_date).alias('weekday'),
            F.date_format(max_date, 'EEEE').alias('weekday_name'),
            F.dayofweek(max_date).isin(1, 7).alias('is_weekend'), #
            F.weekofyear(max_date).alias('week_number'),
            F.quarter(max_date).alias('quarter'),
            F.current_timestamp().alias('updated_at')
        )
    )

    new_date.write.mode('append').saveAsTable('vr_hopea.dim_date')


# COMMAND ----------

display(dim_date.orderBy(F.desc('id')).limit(10))

# COMMAND ----------

display(parameter_table)

# COMMAND ----------

# MAGIC %md
# MAGIC