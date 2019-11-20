// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC #Learning ADB Professionally
// MAGIC ## This is a simple Notebook for understadning ADB capabilities

// COMMAND ----------

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> "828b0e4c-cbb5-4f5b-aefc-a834261308c0",
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope = "training-scope", key = "appsecret"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/72f988bf-86f1-41af-91ab-2d7cd011db47/oauth2/token")

// COMMAND ----------

  dbutils.fs.mount(
  source = "abfss://data@adbMaheshadb.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

// MAGIC %fs
// MAGIC ls /mnt/data/customer

// COMMAND ----------

  dbutils.fs.mount(
  source = "abfss://data@adbMaheshadb.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs) 

// COMMAND ----------

  dbutils.fs.mount(
  source = "abfss://data@adbadlgen2storagesource.dfs.core.windows.net/",
  mountPoint = "/mnt/data",
  extraConfigs = configs)

// COMMAND ----------

import spark.implicits._

case class Customer (customerid: Int, customername: String, address: String, credit: Int, status: Boolean, remarks: String)


// COMMAND ----------

val customersCsvLocation = "/mnt/data/customer/*.csv"
val customers = 
  spark
    .read
    .option("inferSchema", true)
    .option("header", true)
    .option("sep", ",")
    .csv(customersCsvLocation)
    .as[Customer]


// COMMAND ----------

display(customers)

// COMMAND ----------

customers.createOrReplaceTempView("customers")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT address AS CustomerLocation, COUNT(*) AS NoOfCustomers FROM customers
// MAGIC GROUP BY address
// MAGIC ORDER BY address

// COMMAND ----------

