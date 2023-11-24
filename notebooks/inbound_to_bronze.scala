// Databricks notebook source
// DBTITLE 1,Validacao de dados
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/data/inbound")

// COMMAND ----------

// DBTITLE 1,Lendo os dados na camada de inbound
val path = "dbfs:/mnt/data/inbound/dados_brutos_imoveis.json"
val data = spark.read.json(path)

// COMMAND ----------

display(data)

// COMMAND ----------

// DBTITLE 1,Removendo colunas
val data_anuncio = data.drop("imagens", "usuario")
display(data_anuncio) 

// COMMAND ----------

// DBTITLE 1,Criando um id
import org.apache.spark.sql.functions.col


// COMMAND ----------

val df_bronze = data_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// DBTITLE 1,Salvando os dados na camada bronze.
val path = "dbfs:/mnt/data/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


