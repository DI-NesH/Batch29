package SparkPack

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.sql.functions
import org.apache.kafka.clients.consumer._
import java.lang.IllegalArgumentException
import org.apache.spark.SparkDriverExecutionException
import org.apache.spark.sql.functions.explode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.util.Try
import org.apache.spark.sql.types.DecimalType._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.StructType
import org.apache.spark._
import org.apache.spark.storage.StorageLevel

object SparkKinesis {
  def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					.set("spark.driver.allowMultipleContexts","true")

					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession
					.builder()
					.getOrCreate()
					import spark.implicits._

					val ssc = new StreamingContext(conf,Seconds(2))

					val kparams = Map[String, Object]("bootstrap.servers" -> "localhost:9092",
							"key.deserializer" -> classOf[StringDeserializer],
							"value.deserializer" -> classOf[StringDeserializer],
							"group.id" -> "g22",
							"auto.offset.reset" -> "latest",
							"enable.auto.commit" -> (true: java.lang.Boolean))

					val topics = Array("zeyocmcheck")

					val stream = KafkaUtils
					.createDirectStream[String, String](ssc,
							PreferConsistent,
							Subscribe[String, String](topics, kparams)).map(x=>x.value())

					stream.print()

					stream.foreachRDD( x=>

					if(!x.isEmpty())

					{

						val df = spark.read.json(x).select("nationality")

								df.write.format("org.elasticsearch.spark.sql")
								.option("es.nodes.wan.only","true")
								.option("es.port","443")
								.option("es.net.http.auth.user","root")
								.option("es.net.http.auth.pass","Haasya@usa908")
								.option("es.nodes", "https://search-zeyoes-jx7md3pckkpodpjoysbdd33lym.ap-south-1.es.amazonaws.com")
								.mode("append")
								.save("shridinesh_index")

								println("data written elastic search")

					}

							)

					ssc.start()
					ssc.awaitTermination()

	}
}