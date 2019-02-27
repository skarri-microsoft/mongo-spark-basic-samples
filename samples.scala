// Databricks notebook source
// DBTITLE 1,Simple Insert for shard collection
import com.mongodb.spark.config.WriteConfig
import org.bson.Document
import com.mongodb.spark.{MongoConnector, MongoSpark}

val writeConfig=WriteConfig(Map("uri" -> "mongodb://username:password@username.documents.azure.com:10255/test.coll?ssl=true&replicaSet=globaldb"))

val documents = sc.parallelize((1 to 10).map(i => Document.parse(s"{test:  1}")))
MongoSpark.save(documents,writeConfig)

// COMMAND ----------

// DBTITLE 1,Import JSON file
//https://teststorage.blob.core.windows.net/test/100Recs_1.json
spark.conf.set(
  "fs.azure.account.key.posteteststorage.blob.core.windows.net",
  "Storagekey")

val df = spark.read.json("wasbs://test@testststorage.blob.core.windows.net/100Recs_1.json")
df.printSchema()


 df.write.option("spark.mongodb.output.uri",
        "mongodb://username" +
          ":password" +
          "@username.documents.azure.com:10255/test.test?ssl=true&replicaSet=globaldb")
        .option("maxBatchSize",20).format("com.mongodb.spark.sql")
        .mode("append").save()


