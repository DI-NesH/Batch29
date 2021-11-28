package SparkPack

import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming._

object SparkObjNifiStreaming {
	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")

					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession
					.builder()

					.getOrCreate()
					import spark.implicits._


					val ssc = new StreamingContext(conf,Seconds(2))

					val stream = ssc.textFileStream("file:///C:/nout")

					stream.print()


					ssc.start()
					ssc.awaitTermination()


	}
}