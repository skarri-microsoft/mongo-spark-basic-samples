# HD Insights sample for Spark

SSH on to HdInsights cluster using command prompt:

Download the custom jar using the following command:

wget {reach out to Cosmos DB to get the latest jar}
 
sshuser@hn0-skarri:~$ spark-shell --conf "spark.mongodb.input.uri=mongodb://uri" --conf "spark.mongodb.
output.uri=mongodb://uri" --conf "spark.mongodb.input.database=aims-swim" --conf="spark.mongodb.input.c
ollection=flight" --conf "spark.mongodb.output.database=aims-swim" --conf="spark.mongodb.output.collection=flight" --jars {above download jar}
 
scala> import com.mongodb.spark._
import com.mongodb.spark._
 
scala> import org.bson.Document
import org.bson.Document
 
scala> val rdd = MongoSpark.load(sc)
18/01/22 07:09:35 WARN SparkSession$Builder: Using an existing SparkSession; some configuration may not take effect.
rdd: com.mongodb.spark.rdd.MongoRDD[org.bson.Document] = MongoRDD[0] at RDD at MongoRDD.scala:52
 
scala> println(rdd.count)
520
 
scala> println(rdd.first.toJson)
{ "_id" : { "$oid" : "5a5ce9f78abf9306dda59467" }, "gufi" : "8d5d094f-b079-4de6-8ab8-c5d522b586ac", "flight_id" : "ASQ4033", "destination_airport" : "KEWR", "origination_airport" : "KDCA", "departure_time" : { "$date" : 1513124640000 }, "arrival_time" : { "$date" : 1513127580000 }, "flight_duration_mins" : 49.0, "db_timestamp" : { "$date" : 1513127630563 }, "aircraft_type" : "", "airline" : "ASQ", "has_tracks" : 1, "source" : "HV" }
 
scala> import com.mongodb.spark.config._
import com.mongodb.spark.config._
 
scala> val readConfig = ReadConfig(Map("collection" -> "flight-track", "readPreference.name" -> "secondaryPreferred"), Some(ReadConfig(sc)))
readConfig: com.mongodb.spark.config.ReadConfig = ReadConfig(aims-swim,flight-track,Some(mongodb://uri),1000,DefaultMongoPartitioner,Map(),15,ReadPreferenceConfig(secondaryPreferred,None),ReadConcernConfig(None),false)
 
scala> val flightTrackRdd = MongoSpark.load(sc, readConfig)
18/01/22 07:12:39 WARN SparkSession$Builder: Using an existing SparkSession; some configuration may not take effect.
flightTrackRdd: com.mongodb.spark.rdd.MongoRDD[org.bson.Document] = MongoRDD[1] at RDD at MongoRDD.scala:52
 
scala> println(flightTrackRdd.count)
888017

