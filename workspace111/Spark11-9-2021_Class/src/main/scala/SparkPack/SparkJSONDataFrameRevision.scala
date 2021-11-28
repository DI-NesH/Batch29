package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkJSONDataFrameRevision {

	def main(args:Array[String]):Unit={

			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val simpleSchema = StructType(Array(
							StructField("txnno",StringType,true),
							StructField("txndate",StringType,true),
							StructField("custno",StringType,true),
							StructField("amount", StringType, true),
							StructField("category", StringType, true),
							StructField("product", StringType, true),
							StructField("city", StringType, true),
							StructField("state", StringType, true),
							StructField("spendby", StringType, true)
							))


					val df = spark.read.format("csv").option("delimiter","~")
					.load("file:///C://data//datasetsRevision//file4.json").toDF("data")

					df.show(false)
					df.printSchema()

					val finaldf = df.select(

							from_json(col("data"),simpleSchema).alias("data")

							)

					finaldf.show(false)

					val fdf = finaldf.select("data.*")

					fdf.show()

	}

}