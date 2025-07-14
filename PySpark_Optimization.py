# Databricks notebook source
# MAGIC %md
# MAGIC ### SCANNING OPTIMIZATION

# COMMAND ----------

spark

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------

spark.conf.get("spark.sql.adaptive.enabled")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format('csv').option('inferschema','true').option('header','true').load('/FileStore/rowdata/BigMart_Sales.csv')
df.limit(5).display()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing Default Partitions Size to 128KB

# COMMAND ----------

# Changing the default partitions size to 128 KB

spark.conf.set("spark.sql.files.maxPartitionBytes",'131072')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Changing the default fise size to 128 MB

# COMMAND ----------

spark.conf.set('spark.sql.files.maxPartitionBytes','134217728')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Repartitioning

# COMMAND ----------

df = df.repartition(10)

# COMMAND ----------

df.rdd.getNumPartitions()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Get Partitions Info

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.withColumn('Partition_id',spark_partition_id()).display(5)

# COMMAND ----------

df.write.format('parquet').mode('append').option('path','FileStore/rowdata/ParquetWrite').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### New Data Reading

# COMMAND ----------

df_new = spark.read.format('parquet').load('/FileStore/rowdata/ParquetWrite')

df_new.filter(col('Outlet_Location_Type') == 'Tier 1').display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###  **Scanning** Optimization

# COMMAND ----------

df.write.format('parquet')\
        .mode('append')\
        .partitionBy('Outlet_Location_Type')\
        .option('path','/FileStore/rowdata/ParquetWriteOpt')\
        .save()

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df_new = spark.read.format('parquet').load('/FileStore/rowdata/ParquetWriteOpt')

df_new.filter(col('Outlet_Location_Type') == 'Tier 1').display()



# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Optimization

# COMMAND ----------

# DataFrame 1 with higher-numbered IDs
data1 = [
    (10, "Alice"),
    (20, "Bob"),
    (30, "Charlie"),
    (40, "David")
]
df1 = spark.createDataFrame(data1, ["id", "name"])

# DataFrame 2 with corresponding and extra IDs
data2 = [
    (10, "HR"),
    (20, "Engineering"),
    (50, "Finance")
]
df2 = spark.createDataFrame(data2, ["id", "department"])

# COMMAND ----------

df1.display()

# COMMAND ----------

df_join = df1.join(df2,df1['id']==df2['id'],'inner')

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_join_opt = df1.join(broadcast(df2),df1['id']==df2['id'],'inner')
df_join_opt.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark SQL Hints

# COMMAND ----------

df1.createOrReplaceTempView('dt1')
df2.createOrReplaceTempView('dt2')

# COMMAND ----------

df_sql_opt = spark.sql(''' 
           SELECT *  /* broadcast(dt2) */ 
           FROM dt1 join dt2 
           on dt1.id = dt2.id

           ''')

df_sql_opt.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### CACHING AND PERSISTENCE

# COMMAND ----------

df.display()

# COMMAND ----------

df.cache()

# COMMAND ----------

df_n = df.filter(col('Outlet_Location_Type') == 'Tier 1')

# COMMAND ----------

df_n1 = df.filter(col('Outlet_Location_type') == 'Tier 2')

# COMMAND ----------

df.unpersist()

# COMMAND ----------

from pyspark.storagelevel import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Resource Allocation 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### use spark docs for more detials about DRA

# COMMAND ----------

# MAGIC %md
# MAGIC ## AQE

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------

df_new11 = df.groupBy(col('Item_Fat_Content')).count()
df_new11.display()

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')
spark.conf.get('spark.sql.adaptive.enabled')

# COMMAND ----------

df.display()


# COMMAND ----------

df_new11 = df.groupBy(col('Item_Fat_Content')).count()
df_new11.display()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Partition Prunning 

# COMMAND ----------

# Refer spark docs for more details :::

# COMMAND ----------

# MAGIC %md
# MAGIC #### Turn OFF AQE , DPP and AutoBroadcast

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','false')
spark.conf.set('spark.sql.optimizer.dynamicPatitionPrunning.enabled','false')
spark.conf.set('spark.sql.autoBroadcastJoinThreshold',-1)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preparing the partitioned data

# COMMAND ----------

df.write.format('parquet').mode('append')\
        .partitionBy('Outlet_Type')\
        .option('path','FileStore/rowdata/dpp_partitions')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Non partitioned data

# COMMAND ----------

df.write.format('parquet').mode('append')\
        .option('path','FileStore/rowdata/dpp_nonpartitions')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DataFrame

# COMMAND ----------

df1 = spark.read.format('parquet').load('/FileStore/rowdata/dpp_partitions')

df2 = spark.read.format('parquet').load('/FileStore/rowdata/dpp_nonpartitions')

# COMMAND ----------

# MAGIC %md
# MAGIC **Joins**

# COMMAND ----------

df_join = df1.join(df2.filter(col('Outlet_Type')== 'Grocery Store'),df1['Item_Identifier']==df2['Item_Identifier'],'inner')

# COMMAND ----------

df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Turn ON AQE , DPP , AutoBroadcast

# COMMAND ----------

spark.conf.set('spark.sql.adaptive.enabled','true')
spark.conf.set('spark.sql.optimizer.dynamicPatitionPrunning.enabled','true')
spark.conf.set('spark.sql.autoBroadcastJoinThreshold', 5* 1024 * 1024)

# COMMAND ----------

df_join = df1.join(df2.filter(col('Outlet_Type')=='Supermarket Type1'),df1['Outlet_Type']==df2['Outlet_Type'],'inner')
df_join.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast Variable

# COMMAND ----------

# Dataframe 

df = spark.createDataFrame([('1001',),('1002',),('1003',)],['product_id'])

# Lookup dictionary ::

product_dict = {
  '1001' : 'Iphone',
  '1002' : 'Samsung',
  '1003' : 'RealMe'
 }

# COMMAND ----------

df.display()

# COMMAND ----------

product_dict

# COMMAND ----------

# Broadcasting dictionary variable 

broad_var = spark.sparkContext.broadcast(product_dict)

# COMMAND ----------

broad_var.value.get('1001')

# COMMAND ----------

# Our Function 
def myfun(x):
    return broad_var.value.get(x)

# COMMAND ----------

myfun_udf = udf(myfun)

# COMMAND ----------

df_with_names = df.withColumn('Product_name',myfun_udf('product_id'))

# COMMAND ----------

df_with_names.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SALTING OOM

# COMMAND ----------

 df = spark.createDataFrame([('A','100'),('A','200'),('A','300'),('B','400'),('C','500')],['user_id','purchase'])
 df.display()

# COMMAND ----------

df = df.withColumn('salt_id',floor(rand()*3))
df.display()

# COMMAND ----------

df = df.withColumn('user_id_salt',concat('user_id',lit('-'),'salt_id'))
df.display()

# COMMAND ----------

df = df.groupBy('user_id_salt').agg(sum('purchase'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DELTA LAKE OPTIMIZATION

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ak_shcema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ak_shcema.aktable( id INT , salary INT) 
# MAGIC USING DELTA
# MAGIC LOCATION 'FileStore/rowdata/deltatbl'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ak_shcema.aktable
# MAGIC values (6,102434300),(7,23434000)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE ak_shcema.aktable