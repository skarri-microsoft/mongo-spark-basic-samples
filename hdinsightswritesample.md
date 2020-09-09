Login with SSH credentials in Cmd prompt
//Get latest jar:
wget 'latest jar'

spark-shell --jars mongo-spark-connector-assembly-2.4.1-SNAPSHOT0824.jar

import java.util.Date

import com.mongodb.client.MongoCollection
import com.mongodb.client.model.Indexes
import com.mongodb.spark.config.{ReadConfig, WriteConfig}
import com.mongodb.spark.{MongoConnector, MongoSpark}
import com.mongodb.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.bson.{BsonDocument, Document}

// Note database and collection name in the below URI
val uri="mongodb://bugvalidation:password@bugvalidation.mongo.cosmos.azure.com:10255/synapse.testdata?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@bugvalidation@"

val writeConfig = WriteConfig(Map("spark.mongodb.output.uri" -> uri))

val documents = sc.parallelize((1 to 1000000).map(i => Document.parse(s"{vendor_nbr:  $i}")),25)
