package SparkPack21_8_21_Tasks

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession

object Task04_2182021_RowRddDFToRDD {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					println("=================raw data============")

					val data = sc.textFile("file:///C:/data/usdatawh.csv")
					data.foreach(println)

					println("==============Map Split=========")
					val mapsplit = data.map( x => x.split(","))

					println("==============Row RDD=========")
					val schemardd = mapsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))


					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._


					println("==============Implementing Struct Schema=========")

					val structschema = StructType(Array(
							StructField("first_name",StringType,true),  
							StructField("last_name",StringType,true),
							StructField("company_name",StringType,true),
							StructField("address",StringType,true),
							StructField("city",StringType,true),
							StructField("county",StringType,true),
							StructField("state",StringType,true),
							StructField("zip",StringType,true),
							StructField("age",StringType,true),
							StructField("phone1",StringType,true),
							StructField("phone2",StringType,true),
							StructField("email",StringType,true),
							StructField("web",StringType,true)
							))

					val df = spark.createDataFrame(schemardd, structschema)

					println("==============DataFrame Raw=========")
					df.show(false)
					
				println("==============Convert DataFrame back into Row RDD =========")
				//val df2rdd = df.rdd.take(10) // not working
				
				val rows: org.apache.spark.rdd.RDD[org.apache.spark.sql.Row] = df.rdd
				rows.take(10).foreach(println)
				
				

	}
}