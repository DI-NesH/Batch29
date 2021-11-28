package SparkPack21_8_21_Tasks

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object Task01_218201_RowRddStructTypeObj {

def main(args:Array[String]):Unit={
		val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

				val sc =  new SparkContext(conf)  // President
				sc.setLogLevel("ERROR") 

				println("=================raw data============")
				val data = sc.textFile("file:///C:/data/txnsample.txt")
				data.foreach(println)

				println("==============Map Split=========")
				val mapsplit = data.map( x => x.split(","))

				println("==============Row RDD=========")
				val schemardd = mapsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))
				
				
				val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
				import spark.implicits._
				
				println("==============Implementing Struct Schema=========")
				
				val structschema = StructType(Array(
				             StructField("c0",StringType,true),  
			               StructField("c1",StringType,true),
			               StructField("c2",StringType,true),
			               StructField("c3",StringType,true),
			               StructField("c4",StringType,true),
			               StructField("c5",StringType,true),
			               StructField("c6",StringType,true),
			               StructField("c7",StringType,true),
			               StructField("c8",StringType,true)
				))
				
				val df = spark.createDataFrame(schemardd, structschema)
				df.show()
}
}