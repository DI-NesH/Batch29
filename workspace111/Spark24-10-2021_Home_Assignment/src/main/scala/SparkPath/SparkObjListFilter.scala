package SparkPath

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.SparkSession
import scala.io.Source


object SparkObjListFilter {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df= spark.read.format("csv").option("header","true").load("file:///C:/data/file1.csv")
					df.show(false)

					val df2= spark.read.format("csv").option("header","true").load("file:///C:/data/file2.csv")
					df2.show()
					
					val listValues = df2.select("id").map(x=>x.getString(0)).collect.toList  // scala list
					println(listValues)
			
					val finaldf = df.filter(!col("id").isin(listValues:_*))
					finaldf.show()
	}
}