package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkGoogleMapsJson {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._

					println("******************Reading google Map Json****************")
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/Practise/google_maps.json")
					df.show()
					df.printSchema()

					println("=======flattened google Map df =======")
					val finaldf = df.withColumn("markers", explode(col("markers")))

					.select(
							"markers.*"
							)
//					.withColumn("position", explode(col("position")))  
					.withColumn("location", explode(col("location")))
					
					finaldf.show()
					finaldf.printSchema()
	}
}