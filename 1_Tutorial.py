# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema',True)\
                     .option('header',True)\
                     .option('multiline',False)\
                     .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('FileStore/tables/')

# COMMAND ----------

df=spark.read.format('csv').option('Inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

# DBTITLE 1,em
df.printSchema() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

my_ddl_schema = '''                     
                     Item_Identifier STRING, Item_Weight STRING,    
                     Item_Fat_Content STRING,
                     Item_Visibility STRING, 
                     Item_Type STRING,
                     Item_MRP DOUBLE, 
                     Outlet_Identifier STRING,
                     Outlet_Establishment_Year INTEGER, 
                     Outlet_Size STRING,    
                     Outlet_Location_Type STRING,   
                     Outlet_Type STRING,     
                     Item_Outlet_Sales DOUBLE

            '''

# COMMAND ----------

df = spark.read.format('csv')\
           .schema(my_ddl_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
    StructField('Item_Identifier',StringType(),True),
    StructField('Item_Weight',StringType(),True),
    StructField('Item_Fat_Content',StringType(),True),
    StructField('Item_Visibility',StringType(),True),
    StructField('Item_Type',StringType(),True),
    StructField('Item_MRP',StringType(),True),
    StructField('Outlet_Identifier',StringType(),True),
    StructField('Outlet_Establishment_Year',StringType(),True),
    StructField('Outlet_Size',StringType(),True),
    StructField('Outlet_Location_Type',StringType(),True),
    StructField('Outlet_Type',StringType(),True),
    StructField('Item_Outlet_Sales',StringType(),True),
])

# COMMAND ----------

df = spark.read.format('csv').schema(my_struct_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df_sel = df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df_sel = df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER / WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------



# COMMAND ----------

df.filter( (col('Item_type')=='Soft Drinks') & (col('Item_weight') < 10 ) ).display()  

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 3 

# COMMAND ----------

df.filter( (col('Outlet_location_type').isin('Tier 1','Tier 2')) & (col('Outlet_size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Column Renamed
# MAGIC

# COMMAND ----------

df.withColumnRenamed('Item_weight','Item_wt').display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### With Column
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### scenario - 1

# COMMAND ----------

df = df.withColumn('flag',lit('new'))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('multiply', col('Item_weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenrio - 2 

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg")).withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),'Low Fat','lf')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Type Casting

# COMMAND ----------

df = df.withColumn('Item_weight',col('Item_weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sort / OrderBY

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1 

# COMMAND ----------

df.sort(col('Item_weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2 

# COMMAND ----------

df.sort(col('Item_visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 3 

# COMMAND ----------

df.sort(['Item_weight','Item_visibility'],ascending=[0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 4 

# COMMAND ----------

df.sort(['Item_weight','Item_visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limit

# COMMAND ----------

df.limit(4000).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1

# COMMAND ----------

df.drop('Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2 

# COMMAND ----------

df.drop('Item_visibility','Item_type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DROP_DUPLICATES

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2 

# COMMAND ----------

df.dropDuplicates(subset=['Item_type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

data1 = [('1','kad'),
        ('2','sid')]
schema1 = 'id STRING, name STRING' 

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
        ('4','jas')]
schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### UNION

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------


data1 = [('kad','1',),
        ('sid','2',)]
schema1 = 'name STRING, id STRING' 

df1 = spark.createDataFrame(data1,schema1)

df1.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Functions

# COMMAND ----------

# MAGIC %md
# MAGIC #### INITCAP

# COMMAND ----------

df.select(initcap('Item_Type').alias('Item_T')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### LOWER()

# COMMAND ----------

df.select(lower("ITEM_TYPE")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPPER()

# COMMAND ----------

df.select(upper('Item_type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Date Funtion

# COMMAND ----------

# MAGIC %md
# MAGIC #### CURRENT_DATE

# COMMAND ----------

df = df.withColumn('curr_date',current_date())

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date_Add()

# COMMAND ----------

df = df.withColumn('week after',date_add('current date',7)

# COMMAND ----------


df = df.withColumn('week_after',date_add('curr_date',7))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Date_Sub()

# COMMAND ----------

df.withColumn('week_before',date_sub('curr_date',7)).display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7)) 

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DATEDIFF()

# COMMAND ----------

df = df.withColumn('date_diff',datediff('week_after','curr_date'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### date_format()

# COMMAND ----------



df = df.withColumn('week_before',date_format('week_before','dd-MM-yyyy'))

df.display()

# COMMAND ----------


df = df.withColumn('week_before',date_format(to_date('week_before'),'dd-MM-yyyy'))

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### HANDALING NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Dropping Nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropna(subset=['Item_Weight']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filling Nulls

# COMMAND ----------

df.display()

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------


     df.fillna('NotAvailable',subset=['Outlet_Size']).display()
     

# COMMAND ----------

# MAGIC %md
# MAGIC #### SPLIT And Indexing

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPLIT

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Indexing

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Explode

# COMMAND ----------


df_exp = df.withColumn('Outlet_Type',split('Outlet_Type',' '))

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type',explode('Outlet_Type')).display()

# COMMAND ----------

df_exp.display()

# COMMAND ----------


df_exp.withColumn('Type1_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### GROUP BY

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1 

# COMMAND ----------

df.display()

# COMMAND ----------

df.groupBy('Item_Type').agg(sum('Item_MRP').alias('total_price')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP').alias('Average_value')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 3 

# COMMAND ----------

df.groupBy('Item_Type','Outlet_size').agg(sum('Item_MRP').alias('Sum of Item MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 4 
# MAGIC

# COMMAND ----------

df.groupBy('Item_type','Outlet_size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### COLLECT_LIST

# COMMAND ----------


data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]

schema = 'user string, book string'

df_book = spark.createDataFrame(data,schema)

df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### PIVOT

# COMMAND ----------


df.select('Item_Type','Outlet_Size','Item_MRP').display()

# COMMAND ----------

df.groupBy('Item_type').pivot('Outlet_size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN-OTHERWISE

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 1 

# COMMAND ----------

df = df.withColumn('Veg_flag',when(col('Item_type')=='Meat','None-veg').otherwise('veg'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario - 2 

# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('Veg_flag')=='veg') & (col('Item_MRP')<100)),'veg-inexpensive')\
                       .when(((col('Veg_flag')=='veg') & (col('Item_MRP')>100)),'veg-expensive')\
                        .otherwise('none-veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS

# COMMAND ----------

# MAGIC %md
# MAGIC #### INNER-JOIN

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
          ('2','kit','d02'),
          ('3','sam','d03'),
          ('4','tim','d03'),
          ('5','aman','d05'),
          ('6','nad','d06')] 

schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING' 

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
          ('d02','Marketing'),
          ('d03','Accounts'),
          ('d04','IT'),
          ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)
     


# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### left-join

# COMMAND ----------

df1.join(df2,df1["dept_id"]==df2['dept_id'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### right-join

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### ANTI-JOIN

# COMMAND ----------

df1.join(df2,df1['dept_id']==df2['dept_id'],'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WINDOW FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC #### ROW_NUMBER()

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.window import Window 
from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### RANK

# COMMAND ----------

df.withColumn('rank',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### DENSE_RANK

# COMMAND ----------

df.withColumn('denserank',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cumulative Sum

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_type'))).display()

# COMMAND ----------

df.withColumn('cumsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.currentRow))).display()

# COMMAND ----------

df.withColumn('totalsum',sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding,Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTIONS (UDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### setp - 1

# COMMAND ----------

def my_fun(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ### step - 2 

# COMMAND ----------

my_udf = udf(my_fun)

# COMMAND ----------

df.withColumn('newcol',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATA WRITING

# COMMAND ----------

# MAGIC %md
# MAGIC ####  CSV

# COMMAND ----------

df.write.format('csv').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### APEND

# COMMAND ----------

df.write.format('csv').mode('append').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

df.write.format('csv').mode('append').option('path','/FileStore/tables/CSV/data.csv').save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OVERWRITE

# COMMAND ----------

df.write.format('csv').mode('overwrite').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### ERROR

# COMMAND ----------

df.write.format('csv').mode('error').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### IGNORE

# COMMAND ----------

df.write.format('csv').mode('ignore').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARQUET

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC #### TABLE
# MAGIC

# COMMAND ----------


df.write.format('parquet')\
.mode('overwrite')\
.saveAsTable('my_table')

# COMMAND ----------


df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK SQL

# COMMAND ----------

# MAGIC %md
# MAGIC #### createTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from my_view where Item_weight > 10 and Item_fat_content = 'Low Fat'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_weight > 10 and Item_fat_content = 'Low Fat'")

# COMMAND ----------

df_sql.display()