package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkProductsJson {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._

					println("******************Reading Products Json****************")
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/Practise/products.json")
					df.show()
					df.printSchema()

					println("=======flattened Products df =======")					
					val finaldf = df.select(
							"_id.$oid",
							"product_name",
							"quantity",
							"supplier",
							"unit_cost"
							
							/*
							 	col("_id.$oid").alias("oid"),
  							col("product_name"),
  							col("quantity"),
  							col("supplier"),
  							col("unit_cost") 
							 */
							)
							
					finaldf.show()
					finaldf.printSchema()
					
					 println("=======Complex Data=======")  
					 
					val compdf =  finaldf.select(
                        struct(
      											  "$oid"  
		                    ).alias("_id"), 
					                      col("product_name") , col("quantity") , col("supplier") , col("unit_cost")
		                    )

          compdf.show()
					compdf.printSchema()
	}
}