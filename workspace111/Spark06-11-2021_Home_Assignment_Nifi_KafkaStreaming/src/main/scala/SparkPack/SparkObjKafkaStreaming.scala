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
object SparkObjKafkaStreaming {
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
							"group.id" -> "example22","auto.offset.reset" -> "latest",
							"enable.auto.commit" -> (false: java.lang.Boolean))


//					val topics = Array("zeyotk29")
					val topics = Array("zeyot29")

					val stream = KafkaUtils
					.createDirectStream[String, String](ssc,
							PreferConsistent,
							Subscribe[String, String](topics, kparams)).map(x=>x.value())

					stream.print()
					stream.foreachRDD(   x=>

					if(!x.isEmpty())		  
					{

						val df = x.toDF().show()

					}

        )
					ssc.start()
					ssc.awaitTermination()

	}
}