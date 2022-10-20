# Databricks notebook source
# token = dapif620fc53d0b8d97bed68acaf499baf5a

#Git token = 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

# COMMAND ----------


dbutils.fs.mount(
  source = "wasbs://gurpreetbr@sdairlines.blob.core.windows.net",
  mount_point = "/mnt/bro",
  extra_configs = {"fs.azure.account.key.sdairlines.blob.core.windows.net":"7MkaYKcfrU4Gi/GWmg1xEI10aiy5TaznTohE8S7GYzQd+sqVL799qjRPY+3AKuw0w48dg+is3xWN+ASt51GxPg=="})

dbutils.fs.mount(
  source = "wasbs://gurpreetsl@sdairlines.blob.core.windows.net",
  mount_point = "/mnt/sil",
  extra_configs = {"fs.azure.account.key.sdairlines.blob.core.windows.net":"7MkaYKcfrU4Gi/GWmg1xEI10aiy5TaznTohE8S7GYzQd+sqVL799qjRPY+3AKuw0w48dg+is3xWN+ASt51GxPg=="})

dbutils.fs.mount(
  source = "wasbs://gurpreetgl@sdairlines.blob.core.windows.net",
  mount_point = "/mnt/gol",
  extra_configs = {"fs.azure.account.key.sdairlines.blob.core.windows.net":"7MkaYKcfrU4Gi/GWmg1xEI10aiy5TaznTohE8S7GYzQd+sqVL799qjRPY+3AKuw0w48dg+is3xWN+ASt51GxPg=="})

# COMMAND ----------

dbutils.fs.ls('/mnt/bro')
# dbutils.fs.ls('/mnt/sil')
# dbutils.fs.ls('/mnt/gol')

# COMMAND ----------

Airline = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/airlines.csv2022-10-19T11:11:46.8918012Z')
Airport = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/airports.csv2022-10-19T11:12:06.7100272Z')
part1 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-01.txt')
part2 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-02.txt')
part3 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-03.txt')
part4 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-04.txt')
part5 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-05.txt')
part6 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-06.txt')
part7 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-07.txt')
part8 = spark.read.format("csv").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/bro/gupreetrz/Flight/partition-08.txt')
# Airport = spark.read.format("csv").option("header","true").load(TimePathcsv)
# flight = spark.read.format("csv").option("header","true").load(TimePathcsv)



# COMMAND ----------

display(part7)

# COMMAND ----------

dbutils.data.summarize(part1)

# COMMAND ----------

Airline.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/airline")
Airport.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/airport")
part1.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part1")
part2.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part2")
part3.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part3")
part4.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part4")
part5.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part5")
part6.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part6")
part7.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part7")
part8.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/part8")
# part9.coalesce(1).write.format('parquet').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/airline")



# COMMAND ----------

dbutils.fs.ls('/mnt/sil')

# COMMAND ----------

Airline = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/airline')
Airport = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/airport')
part1 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part1')
part2 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part2')
part3 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part3')
part4 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part4')
part5 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part5')
part6 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part6')
part7 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part7')
part8 = spark.read.format("parquet").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/part8')

# COMMAND ----------

Airline.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Airline")
Airport.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Airport")
part1.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part1")
part2.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part2")
part3.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part3")
part4.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part4")
part5.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part5")
part6.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part6")
part7.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part7")
part8.coalesce(1).write.format('Delta').mode("overwrite").save("dbfs:/mnt/sil/gurpreet_sl/Part8")

# COMMAND ----------

# DBTITLE 1,Reading delta files
Airline = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Airline')
Airport = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Airport')
Part1 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part1')
Part2 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part2')
Part3 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part3')
Part4 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part4')
Part5 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part5')
Part6 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part6')
Part7 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part7')
Part8 = spark.read.format("Delta").option("inferSchema",'True').option("header", "True").load('dbfs:/mnt/sil/gurpreet_sl/Part8')

# COMMAND ----------

# Part1.count()
# display(Part1.na.drop(how = 'any',thresh = 2))

# COMMAND ----------

# Part1.union(Part2).count()

# COMMAND ----------

# DBTITLE 1,merging multiple files using union and distinct for removing duplicate values
Flight = Part1.union(Part2).distinct()
Flight = Flight.union(Part3).distinct()
Flight = Flight.union(Part4).distinct()
Flight = Flight.union(Part5).distinct()
Flight = Flight.union(Part6).distinct()
Flight = Flight.union(Part7).distinct()
Flight = Flight.union(Part8).distinct()
Flight.count()

# COMMAND ----------

display(Flight)

# COMMAND ----------

# DBTITLE 1,fill = for replace, when for condition also we can use case for multiple condition
# Flight.na.fill({'CANCELLATION_REASON':'null','type': 'unknown'})
# Flight.na.fill("null",["CANCELLATION_REASON"]) \
#     .na.fill("unknown",["type"]).show()
# Flight=Flight.na.fill(value = 'Unknown',subset=['CANCELLATION_REASON'])
# Flight=Flight.na.fill(value = '',subset=['AIRLINE_DELAY'])
Flight=Flight.na.fill(value = 0,subset=['AIR_SYSTEM_DELAY'])
Flight=Flight.na.fill(value = 0,subset=['SECURITY_DELAY'])
Flight=Flight.na.fill(value = 0,subset=['AIR_SYSTEM_DELAY'])
Flight=Flight.na.fill(value = 0,subset=['AIRLINE_DELAY'])
Flight=Flight.na.fill(value = 0,subset=['LATE_AIRCRAFT_DELAY'])
Flight=Flight.na.fill(value = 0,subset=['WEATHER_DELAY'])

# from pyspark.sql.functions import when
# Flight = Flight.withColumn("CANCELLATION_REASON", 
#       when(Flight.DIVERTED > 0 , 'Flight Diverted' ))

from pyspark.sql.functions import when
Flight = Flight.withColumn("CANCELLATION_REASON", 
     when(Flight.DIVERTED > 0 ,'Flight Diverted' ) 
               .when (Flight.CANCELLED != 1 ,'Flight on Track')
                          .otherwise(Flight.CANCELLATION_REASON))


# COMMAND ----------

display(Flight)

# COMMAND ----------

# DBTITLE 1,Removing  null values form particular column
# Flights=Flight.na.drop(how = 'any', subset ='CANCELLATION_REASON')
# Flights=Flight.na.drop(how = 'any', subset ='ARRIVAL_TIME')
# Flights=Flight.na.drop(how = 'any', subset ='MONTH')
# Flights=Flight.na.drop(how = 'any', subset ='AIRLINE')
# Flights=Flight.na.drop(how = 'any', subset ='ORIGIN_AIRPORT')
# Flights=Flight.na.drop(how = 'any', subset ='SCHEDULED_DEPARTURE')
# Flights=Flight.na.drop(how = 'any', subset ='DEPARTURE_DELAY')
# Flights=Flight.na.drop(how = 'any', subset ='SCHEDULED_TIME')
# Flights=Flight.na.drop(how = 'any', subset ='ARRIVAL_DELAY')
# clean=Flight.na.drop(how = 'any', subset ='ORIGIN_AIRPORT')


# COMMAND ----------

display(Airport)
# Airline.selectExpr('any(vals == "Abilene Regional Airport")').show()

# COMMAND ----------

# DBTITLE 1,Removing null values
# dbutils.fs.ls('/mnt/gol')
# Airport = Airport.withColumnRenamed('IATA_CODE','IATACODE')
Airport=Airport.na.drop()
Airline =Airline.na.drop()


# COMMAND ----------

# DBTITLE 1,Droping Hive table from adb
# from pyspark.sql import HiveContext
# sqlContext = HiveContext(sc)
# sqlContext.sql('drop table Airline')

# COMMAND ----------

# DBTITLE 1,Creating hive table 
Airline.coalesce(1).write.format('Delta').mode("overwrite").saveAsTable("Airline")
Airport.coalesce(1).write.format('Delta').mode('overwrite').saveAsTable('Airport')
# Flights.coalesce(1).write.format('Delta').mode("overwrite").saveAsTable("dbfs:/mnt/sil/gurpreet_sl/Flight")
Flight.write.mode("overwrite").saveAsTable("Flight")
# 

# COMMAND ----------

# DBTITLE 1,Remove null values from dataset
# clean=Part1.select([when(col(c)=="",None).otherwise(col(c)).alias(c) for c in df.columns])
# clean = Part1.select([when(col(c)=='',None
# clean=part1.na.drop(how = 'any', thresh = 1)
# clean.count()
                           # part1.na.fill('null')
# fillna(part1, subset =None)
# ew=part1.na.drop()
# ew.count()
# display(Part1.dropDuplicates())
# c = Part1.drop_duplicates()
# c.count()
# display(Part1.na.fill("null"))
# cl = Part1.na.fill("null")
# clean=cl.na.drop(how = 'any', thresh = 1, subset =None)
# clea = cl.na.drop("all")
# clea.count()
# display(clea)

# COMMAND ----------

# DBTITLE 1,Unmount
dbutils.fs.unmount("/mnt/bro")
dbutils.fs.unmount("/mnt/sil")
dbutils.fs.unmount("/mnt/gol")

# COMMAND ----------


