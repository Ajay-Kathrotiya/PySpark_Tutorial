# Databricks notebook source
# MAGIC %md
# MAGIC ### SPARK session 

# COMMAND ----------

spark

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILE READ

# COMMAND ----------

flight_df = spark.read.format('csv').option('header','true').option('inferschema','false')\
    .option('mode','FAILFAST').load('/FileStore/tables/2010_summary.csv')

flight_df.show(5)

# COMMAND ----------

flight_df = spark.read.format('csv').option('header','true').option('inferschema','false')\
    .option('mode','FAILFAST').load('/FileStore/tables/2010_summary.csv')

flight_df.show(5)

# COMMAND ----------

flight_df.display()

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------

flight_df = spark.read.format('csv').option('header','true').option('inferschema','true')\
    .option('mode','FAILFAST').load('/FileStore/tables/2010_summary.csv')

flight_df.display()

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------

from pyspark.sql.types import * 

# COMMAND ----------

my_schema = StructType([
        StructField('DEST_COUNTRY_NAME',StringType(),True),
        StructField('ORIGIN_COUNTRY_NAME',StringType(),True),
        StructField('count',IntegerType(),True)
])

# COMMAND ----------

flight_df = spark.read.format('csv').schema(my_schema).load('/FileStore/tables/2010_summary.csv')\
                      .display()

# COMMAND ----------

df = spark.read.format('parquet').option('header','true')\
.load('/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz-1.parquet').show()

# COMMAND ----------

df = spark.read.parquet('/FileStore/tables/part_r_00000_1a9822ba_b8fb_4d8e_844a_ea30d0801b9e_gz-1.parquet')
df.show()

# COMMAND ----------

df = spark.read.format('csv').option('header','true').load('/FileStore/tables/demo.csv')
df.show()

# COMMAND ----------

print(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILE WRITE

# COMMAND ----------

df.write.format('csv').option('mode','overwrite').option('header','true').option('path','/FileStore/tables/csv_write/').save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/csv_write/')

# COMMAND ----------

df.repartition(3).write.format('csv').option('mode','overwrite').option('header','true').option('path','/FileStore/tables/csv_write_partition/').save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/csv_write_partition/')

# COMMAND ----------

df  = spark.read.format('csv').option('header','true').load('/FileStore/tables/demo2.csv')
df.show()

# COMMAND ----------

df.write.format('csv').option('mode','overwrite').partitionBy('address').option('header','true')\
                      .option('path','/FileStore/tables/partitioned_by/').save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/partitioned_by/')

# COMMAND ----------

df.write.format('csv').option('mode','overwrite').partitionBy('address','gender').option('header','true')\
                      .option('path','/FileStore/tables/partitioned_by_address_gender/').save()

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/partitioned_by_address_gender/')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/partitioned_by_address_gender/address=INDIA/')

# COMMAND ----------

print(df)

# COMMAND ----------

df.write.format('csv').option('mode','overwrite').bucketBy(3,'id').option('header','true')\
                      .option('path','/FileStore/tables/bucket_by/').saveAsTable('bucket_by_id_table')

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/bucket_by/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Frame Creation

# COMMAND ----------

my_data = [(1,2),(2,3),(3,4),(4,5),(5,6),(6,7),(7,8)]

# COMMAND ----------

my_schema = ['id','num']

# COMMAND ----------

my_df = spark.createDataFrame(data=my_data,schema=my_schema)
my_df.show()

# COMMAND ----------

my_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT
# MAGIC

# COMMAND ----------

my_df.select('*').show()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

my_df.select(col('num') * 10 ).show(4)

# COMMAND ----------

# MAGIC %md
# MAGIC Spark SQL

# COMMAND ----------

my_df.createOrReplaceTempView('my_tbl')

# COMMAND ----------

spark.sql(
    """
           select * from my_tbl where id%2 = 0 and num <= 5 
    """
).show()

# COMMAND ----------

data = [(1,'ajay',25,50000),(2,'makodo',30,60000),(3,'monalisa',25,30000)]

# COMMAND ----------

columns = ['id','name','age','salary']

# COMMAND ----------

df = spark.createDataFrame(data,columns)
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('id').alias('employee_id'),'name','age','salary').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### filter / where clause

# COMMAND ----------

df.filter(col('salary') > 30000).show()

# COMMAND ----------

df.where(col('salary') > 30000).show()

# COMMAND ----------

df.filter((col('age') < 26) & (col('salary') > 2000)).show()

# COMMAND ----------

# MAGIC %md
# MAGIC literal

# COMMAND ----------

df.select('*',lit('kumar').alias('last_name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC adding columns / modify column => withColumn

# COMMAND ----------

df.withColumn("surname", lit("singh")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC rename column

# COMMAND ----------

df.withColumnRenamed('id','employee_id').display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC CAST

# COMMAND ----------

df.withColumn('id',col('id').cast('string'))\
  .withColumn('salary',col('salary').cast('integer')).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DROP COLUMN

# COMMAND ----------

df.drop('id',col('salary')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC UNION 
# MAGIC

# COMMAND ----------


data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17)]

columns = ['id','name','salary','age']

# COMMAND ----------

df1 = spark.createDataFrame(data,columns)
df1.show()

# COMMAND ----------

data1=[(19 ,'Sohan',50000, 18),
(20 ,'Sima',75000,  17)] 

columns = ['id','name','salary','age']

df2 = spark.createDataFrame(data1,columns)
df2.show()

# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

df1.union(df2).count()

# COMMAND ----------

wrong_column_data=[(19 ,50000, 18,'Sohan'),
(20 ,75000,  17,'Sima')]

columns = ['age','salary','id','name']

df3 = spark.createDataFrame(wrong_column_data,columns)
df3.show()

# COMMAND ----------

# MAGIC %md
# MAGIC WHEN COLUMNS ORDER ARE DIFF AND DOING UNION 

# COMMAND ----------

df2.union(df3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC unionByName
# MAGIC

# COMMAND ----------

df2.unionByName(df3).show()

# COMMAND ----------

wrong_column_data2=[(19 ,50000, 18,'Sohan',10),
(20 ,75000,  17,'Sima',20)]

columns = ['age','salary','id','name','bonus']

df4 = spark.createDataFrame(wrong_column_data2,columns)
df4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC WHEN COLUMNS NUMBER R DIFF AND DOING UNION

# COMMAND ----------

df3.union(df4).show()

# COMMAND ----------

df4.select('age','salary','id','name').union(df3).show()

# COMMAND ----------

# MAGIC %md
# MAGIC Repartitioned and coalesce

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------

flight_df.rdd.getNumPartitions()

# COMMAND ----------

repartitioned_flight_df = flight_df.repartition(4)

# COMMAND ----------

repartitioned_flight_df.rdd.getNumPartitions()

# COMMAND ----------

repartitioned_flight_df.withColumn('partition_id',spark_partition_id()).groupBy('partition_id').count().show()

# COMMAND ----------

partitioned_on_columns =flight_df.repartition(300,'ORIGIN_COUNTRY_NAME')
partitioned_on_columns.rdd.getNumPartitions() 

# COMMAND ----------

partitioned_on_columns.withColumn('partition_id',spark_partition_id()).groupBy('partition_id').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC COALESCE

# COMMAND ----------

coalesce_flight_df = flight_df.repartition(8)

# COMMAND ----------

coalesce_flight_df.withColumn('part_id',spark_partition_id()).groupBy('part_id').count().show()

# COMMAND ----------

three_coalesce_df = coalesce_flight_df.coalesce(3)

# COMMAND ----------

three_coalesce_df.withColumn('part_id',spark_partition_id()).groupBy('part_id').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC IF ELSE | CASE WHEN | OTHERWISE

# COMMAND ----------

emp_data = [
(1,'manish',26,20000,'india','IT'),
(2,'rahul',None,40000,'germany','engineering'),
(3,'pawan',12,60000,'india','sales'),
(4,'roshini',44,None,'uk','engineering'),
(5,'raushan',35,70000,'india','sales'),
(6,None,29,200000,'uk','IT'),
(7,'adam',37,65000,'us','IT'),
(8,'chris',16,40000,'us','sales'),
(None,None,None,None,None,None),
(7,'adam',37,65000,'us','IT')
]

columns = ['id','name','age','salary','country','department']

emp_df = spark.createDataFrame(emp_data,columns)
emp_df.show()

# COMMAND ----------

emp_df.withColumn('Adult',when(col('age')<18 , 'No').when(col('age')>18,'Yes').otherwise('No value')).show()

# COMMAND ----------

emp_df.withColumn('age',when(col('age').isNull(),lit(19)).otherwise(col('age')))\
      .withColumn('adult',when(col('age')<18,'No').when(col('age')>18,'Yes')).show()

# COMMAND ----------

data=[(10 ,'Anil',50000, 18),
(11 ,'Vikas',75000,  16),
(12 ,'Nisha',40000,  18),
(13 ,'Nidhi',60000,  17),
(14 ,'Priya',80000,  18),
(15 ,'Mohit',45000,  18),
(16 ,'Rajesh',90000, 10),
(17 ,'Raman',55000, 16),
(18 ,'Sam',65000,   17),
(15 ,'Mohit',45000,  18),
(13 ,'Nidhi',60000,  17),      
(14 ,'Priya',90000,  18),  
(18 ,'Sam',65000,   17)
     ]

columns = ['id','name','salary','manager_id']

manager_df = spark.createDataFrame(data,columns)
manager_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC DISTINCT

# COMMAND ----------

manager_df.distinct().show()

# COMMAND ----------

manager_df.select('id','name').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC dropDuplicates

# COMMAND ----------

manager_df.dropDuplicates(['id','name','salary','manager_id']).show()

# COMMAND ----------

# MAGIC %md
# MAGIC sort

# COMMAND ----------

manager_df.sort('salary').show()

# COMMAND ----------

manager_df.sort(col('salary').desc(),col('name').desc()).show()

# COMMAND ----------

leet_code_data = [
    (1, 'Will', None),
    (2, 'Jane', None),
    (3, 'Alex', 2),
    (4, 'Bill', None),
    (5, 'Zack', 1),
    (6, 'Mark', 2)
]

columns = ['id','name','refer_id']

leet_code_df = spark.createDataFrame(leet_code_data,columns)
leet_code_df.show()

# COMMAND ----------

leet_code_df.filter((col('refer_id')!= 2) | (col('refer_id').isNull())).select('name').show()

# COMMAND ----------

# MAGIC %md
# MAGIC count

# COMMAND ----------

emp_df.count()

# COMMAND ----------

emp_df.select(count('name')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC sum , min , max

# COMMAND ----------

emp_df.select(sum('salary').alias('total_salary'),min('salary').alias('min_salary'),max('salary').alias('max_salary')).show()

# COMMAND ----------

emp_df.select(sum('salary'),count('salary'),avg('salary').cast('int').alias('avg_salary')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC groupBy

# COMMAND ----------

emp_df.groupBy('department').agg(sum('salary')).filter(col('department').isNotNull()).show()

# COMMAND ----------

emp_df.groupBy(col('country'),col('department')).agg(sum('salary')).filter(col('country').isNotNull()).sort('country').show()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC JOIN

# COMMAND ----------

customer_data = [(1,'manish','patna',"30-05-2022"),
(2,'vikash','kolkata',"12-03-2023"),
(3,'nikita','delhi',"25-06-2023"),
(4,'rahul','ranchi',"24-03-2023"),
(5,'mahesh','jaipur',"22-03-2023"),
(6,'prantosh','kolkata',"18-10-2022"),
(7,'raman','patna',"30-12-2022"),
(8,'prakash','ranchi',"24-02-2023"),
(9,'ragini','kolkata',"03-03-2023"),
(10,'raushan','jaipur',"05-02-2023")]

customer_schema=['customer_id','customer_name','address','date_of_joining']

customers_df = spark.createDataFrame(customer_data,customer_schema)
customers_df.show()

# COMMAND ----------

sales_data = [(1,22,10,"01-06-2022"),
(1,27,5,"03-02-2023"),
(2,5,3,"01-06-2023"),
(5,22,1,"22-03-2023"),
(7,22,4,"03-02-2023"),
(9,5,6,"03-03-2023"),
(2,1,12,"15-06-2023"),
(1,56,2,"25-06-2023"),
(5,12,5,"15-04-2023"),
(11,12,76,"12-03-2023")]

sales_schema=['customer_id','product_id','quantity','date_of_purchase']

sales_df = spark.createDataFrame(sales_data,sales_schema)
sales_df.show()

# COMMAND ----------

product_data = [(1, 'fanta',20),
(2, 'dew',22),
(5, 'sprite',40),
(7, 'redbull',100),
(12,'mazza',45),
(22,'coke',27),
(25,'limca',21),
(27,'pepsi',14),
(56,'sting',10)]

product_schema=['id','name','price']

product_df = spark.createDataFrame(product_data,product_schema)
product_df.show()

# COMMAND ----------

customers_df.join(sales_df,sales_df['customer_id']==customers_df['customer_id'],'inner').show()

# COMMAND ----------

customers_df.join(sales_df,sales_df['customer_id']==customers_df['customer_id'],'left').show()

# COMMAND ----------

# MAGIC %md
# MAGIC WINDOW FUNCTIONS

# COMMAND ----------

emp_df.show(1)

# COMMAND ----------

from pyspark.sql.window import Window

emp_df.withColumn('row_number',row_number().over(Window.partitionBy('department').orderBy('salary')))\
      .withColumn('rank',dense_rank().over(Window.partitionBy('department').orderBy(desc('salary'))))\
      .filter((col('rank')<=2) & col('id').isNotNull()).show()

# COMMAND ----------

product_data = [
(2,"samsung","01-01-1995",11000),
(1,"iphone","01-02-2023",1300000),
(2,"samsung","01-02-2023",1120000),
(3,"oneplus","01-02-2023",1120000),
(1,"iphone","01-03-2023",1600000),
(2,"samsung","01-03-2023",1080000),
(3,"oneplus","01-03-2023",1160000),
(1,"iphone","01-01-2006",15000),
(1,"iphone","01-04-2023",1700000),
(2,"samsung","01-04-2023",1800000),
(3,"oneplus","01-04-2023",1170000),
(1,"iphone","01-05-2023",1200000),
(2,"samsung","01-05-2023",980000),
(3,"oneplus","01-05-2023",1175000),
(1,"iphone","01-06-2023",1100000),
(3,"oneplus","01-01-2010",23000),
(2,"samsung","01-06-2023",1100000),
(3,"oneplus","01-06-2023",1200000)
]

product_schema=["product_id","product_name","sales_date","sales"]

product_df = spark.createDataFrame(data=product_data,schema=product_schema)

product_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### flatten nested json

# COMMAND ----------

resturant_json_df = spark.read.format('json')\
                         .option('multiline','true')\
                         .option('inferschema','true')\
                         .load('/FileStore/tables/resturant_json_data.json')

# COMMAND ----------

resturant_json_df.printSchema()

# COMMAND ----------

resturant_json_df.show(10)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

resturant_json_df.select('*',explode('restaurants').alias('new_restaurants'))\
                 .drop('restaurants').printSchema()

# COMMAND ----------

resturant_json_df.select('*',explode('restaurants').alias('new_restaurants'))\
                 .drop('restaurants')\
                 .select('*','new_restaurants.restaurant.R.res_id',explode_outer('new_restaurants.restaurant.establishment_types').alias('establishment_types_new'),
                  'new_restaurants.restaurant.name').drop('new_restaurants','code','message','results_found',
                                                          'results_start','status','results_shown').show(10)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC