package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object SparkSampleJson {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._

					println("******************Reading sample****************")
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/Practise/sample.json")
					df.show()
					df.printSchema()

					println("=======flattened sample df =======")
					val finaldf = df
					.withColumn("content", explode(col("content")))
					.withColumn("dates",explode(col("dates")))  // use here as it is outside the content Array
					.select(
							"content.bar",
							"content.foo",
							"dates",
							"reason","status","user"
							)
					finaldf.show()
					finaldf.printSchema()

					println("=======Complex Data=======")  

					val compdf = finaldf.select(
							col("user"),col("status"),col("reason"),col("foo"),col("bar"),col("dates")
							).groupBy("user","status","reason")
					.agg(
							collect_list(
									struct(
											"foo","bar"
											)    
									).alias("content"),
							collect_list(
									struct(
											"dates"
											)    
									).alias("dates"))

					compdf.show()
					compdf.printSchema()
	}
}