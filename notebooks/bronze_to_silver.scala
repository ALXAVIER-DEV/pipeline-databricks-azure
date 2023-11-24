// Databricks notebook source
// DBTITLE 1,Conferindo se os dados foram montados e se temos acesso a pasta bronze
// MAGIC %python
// MAGIC dbutils.fs.ls("/mnt/data/bronze")

// COMMAND ----------

// DBTITLE 1,Lendo os dados na camada bronze
val path = "dbfs:/mnt/data/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)

// COMMAND ----------

display(df)


// COMMAND ----------

// DBTITLE 1,Transformando os campos do json em colunas
 display(df.select("anuncio.*"))

// COMMAND ----------

 display(
  df.select("anuncio.*"
  ,"anuncio.endereco.*")
 )

// COMMAND ----------

val data_detail =    df.select("anuncio.*"
  ,"anuncio.endereco.*")

// COMMAND ----------

display(data_detail)

// COMMAND ----------

// DBTITLE 1,Removendo colunas
val df_silver = data_detail.drop("caracteristicas","endereco")
display(df_silver)

// COMMAND ----------

// DBTITLE 1,Salvando na camada silver
val path = "dbfs:/mnt/data/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------


