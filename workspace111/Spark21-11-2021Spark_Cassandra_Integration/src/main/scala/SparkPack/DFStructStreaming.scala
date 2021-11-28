package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DFStructStreaming {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("ES")
			.setMaster("local[*]")
			.set("spark.driver.allowMultipleContexts","true")

					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val schema = StructType(Array(
							StructField("name", StringType, true)));  


    			val csvdf = spark
    					.readStream
    					.format("csv")
    					.schema(schema)
    					.load("file:///C:/ss_streaming/srcdata")

					csvdf
					.writeStream
					.format("console")
					.option("checkpointLocation","file:///C://checekswwwssss/")
					.start()
					.awaitTermination()
	}

}