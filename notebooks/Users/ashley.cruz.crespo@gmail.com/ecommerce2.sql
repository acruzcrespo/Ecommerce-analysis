-- Databricks notebook source
-- MAGIC %sh pwd

-- COMMAND ----------

-- MAGIC %sh 
-- MAGIC rm -rf /dbfs/data
-- MAGIC mkdir /dbfs/data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC dbutils.library.installPyPI("kaggle") 
-- MAGIC dbutils.library.restartPython()

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC 
-- MAGIC cd ~
-- MAGIC rm -rf .kaggle
-- MAGIC mkdir .kaggle
-- MAGIC 
-- MAGIC cd .kaggle
-- MAGIC touch kaggle.json
-- MAGIC echo '{"username":"ashleyccrespo","key":"31d75a6e210bab29f65396347a546231"}' > kaggle.json
-- MAGIC chmod 600 kaggle.json
-- MAGIC 
-- MAGIC cat kaggle.json

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from kaggle.api.kaggle_api_extended import KaggleApi
-- MAGIC api = KaggleApi()
-- MAGIC api.authenticate()
-- MAGIC 
-- MAGIC # Download all files of a dataset
-- MAGIC # Signature: dataset_download_files(dataset, path=None, force=False, quiet=True, unzip=False)
-- MAGIC api.dataset_download_files('snowwlex/etsy-shops',path='/dbfs/data',unzip='True')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from kaggle.api.kaggle_api_extended import KaggleApi
-- MAGIC api = KaggleApi()
-- MAGIC api.authenticate()
-- MAGIC 
-- MAGIC # Download all files of a dataset
-- MAGIC # Signature: dataset_download_files(dataset, path=None, force=False, quiet=True, unzip=False)
-- MAGIC api.dataset_download_files('juanmah/world-cities',path='/dbfs/data',unzip='True')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #read, load and display the csv file in order to make it accessible to work with the data in SQL
-- MAGIC #previous steps were for creating the space (here in the notebook) to work with a public dataset. install kaggle library in python #to install the kaggle api to attach user account and being able to download a public dataset into the notebook. Once the dataset #was download and unzip, the dataset was move to dbfs and formatted to in python to be able to access the data and analyze it. 
-- MAGIC 
-- MAGIC eshopsdata = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/data/shops.csv')
-- MAGIC display(eshopsdata)
-- MAGIC 
-- MAGIC eshopsdata.write.saveAsTable("eshops_data")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC citiesdata = spark.read.format('csv').options(header='true', inferSchema='true').load('dbfs:/data/worldcities.csv')
-- MAGIC display(citiesdata)
-- MAGIC 
-- MAGIC citiesdata.write.saveAsTable("cities_data")

-- COMMAND ----------

-- MAGIC  %sql show tables in default

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as f
-- MAGIC from pyspark.sql.functions import split
-- MAGIC 
-- MAGIC # df = spark.createDataFrame([("Jaipur, India",)],["seller_location"])
-- MAGIC # df.show()
-- MAGIC 
-- MAGIC # df_split = df.select(f.split(df.seller_location,",")).rdd.flatMap(
-- MAGIC #               lambda x: x).toDF(schema=["seller_state","seller_country"])
-- MAGIC 
-- MAGIC eshopsdata = eshopsdata.withColumn("seller_state",split("seller_location",", ").getItem(0))
-- MAGIC eshopsdata = eshopsdata.withColumn("seller_country",split("seller_location",", ").getItem(1))
-- MAGIC 
-- MAGIC eshopsdata.show()
-- MAGIC eshopsdata.write.saveAsTable("eshopsdata")

-- COMMAND ----------

drop table eshopsdata

-- COMMAND ----------

select *
from eshopsdata
where seller_country = 'United States'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ideas 
-- MAGIC 
-- MAGIC number of sales per year/
-- MAGIC country with the highest sales (sales by location)/ 
-- MAGIC is there a relation between the number of sales with the number of reviews?
-- MAGIC seller join date through the years//
-- MAGIC compare the number of sales with the number of reviews? is there a relation?//

-- COMMAND ----------

select seller_join_date, sum(number_of_sales) as number_of_sales_per_year 
from eshopsdata
where seller_join_date > 2004
group by 1


-- COMMAND ----------

with dates as (
select 1 as month, 24 as day, 2019 as year union
select 2 as month, 13 as day, 2019 as year union
select 3 as month, 2 as day, 2020 as year union
select 4 as month, 10 as day, 2020 as year
)
select to_date(concat(day,"/",month,"/",year),"d/m/yyyy") as date, date_add(to_date(concat(day,"/",month,"/",year),"d/m/yyyy"),30) as date_plus_30
from dates

-- COMMAND ----------

select seller_join_date, sum(number_of_sales) as number_of_sales_per_year 
from eshopsdata
where seller_join_date > 2004
group by seller_join_date
order by seller_join_date DESC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select seller_country, count(number_of_sales) as number_of_sales_per_country
-- MAGIC from eshopsdata
-- MAGIC where seller_country is not null
-- MAGIC group by seller_country
-- MAGIC order by number_of_sales_per_country desc
-- MAGIC limit 10

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select seller_country, seller_join_date as year, sum(number_of_sales) as number_of_sales_per_country
-- MAGIC from eshopsdata
-- MAGIC where seller_country is not null
-- MAGIC group by seller_country, seller_join_date
-- MAGIC order by seller_join_date desc
-- MAGIC limit 10 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select seller_country, seller_join_date as year, sum(number_of_sales) as number_of_sales_per_country
-- MAGIC from eshopsdata
-- MAGIC where seller_country is not null and seller_country = 'United States'
-- MAGIC group by seller_country, seller_join_date
-- MAGIC order by seller_join_date desc

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC select seller_state as state, sum(number_of_sales) as number_of_sales
-- MAGIC from eshopsdata
-- MAGIC where seller_country = 'United States'
-- MAGIC group by seller_state
-- MAGIC order by 1 desc

-- COMMAND ----------

