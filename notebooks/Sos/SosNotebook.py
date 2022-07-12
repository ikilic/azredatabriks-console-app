# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

ehConf = {}
connectionString = "Endpoint=sb://ihsuprodamres120dednamespace.servicebus.windows.net/;SharedAccessKeyName=iothubowner;SharedAccessKey=VW+lXk3WpcitX7kQdMeaoJaG/zV0Yjvhns46oZtLUi4=;EntityPath=iothub-ehub-kstreamhub-19838619-7c0a89f519"
ehConf['eventhubs.connectionString'] = connectionString
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# COMMAND ----------

df = (spark.readStream.format("eventhubs").options(**ehConf).load()) 

# COMMAND ----------

display(df)

# COMMAND ----------

jsonSchema = StructType(
                        [StructField("RuzgarHizi",DoubleType(), True),
                         StructField("Tarih", TimestampType(), True),
                         StructField("TribunAdi", StringType(), True)]
                        )

df = df.withColumn("body", from_json(df.body.cast("string"),jsonSchema))
df.printSchema()

# COMMAND ----------

df_select=df.select(df.body.TribunAdi.alias("TribunAdi"),
                   map_values("systemProperties")[3].alias("CihazId"),
                   df.body.RuzgarHizi.alias("RuzgarHizi"),
                   df.body.Tarih.alias("Tarih")
                   )

# COMMAND ----------

df_select_groupby=df_select.groupBy(df_select.TribunAdi,
                                   df_select.CihazId,
                                   window(df_select.Tarih,"30 second")).count()
display(df_select_groupby)

# COMMAND ----------

stream_query_memory=(df_select_groupby
                    .writeStream
                    .format("memory")
                    .queryName("event_group_table")
                    .outputMode("complete")
                    .start()
                    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   TribunAdi,
# MAGIC   CihazId,
# MAGIC   date_format(window.end,"yyyy-MM-dd HH:mm") as SonTarih,
# MAGIC   count as Adet
# MAGIC from event_group_table
# MAGIC order by 2 desc
# MAGIC LIMIT 3

# COMMAND ----------

stream_query_delta=(df_select_groupby
                    .writeStream
                    .format("delta")
                    .outputMode("complete")
                    .option("checkpointLocation", "/delta/events/_checkpoints/from-json")
                    .table("iot_events_table")
                    )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot_events_table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CihazId,
# MAGIC   date_format(window.end,"yyyy-MM-dd HH:mm") as Tarih,
# MAGIC   count as adet
# MAGIC FROM iot_events_table

# COMMAND ----------

