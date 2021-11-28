package SparkPack21_8_21_Tasks

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession


object Task02_2182021_SchemaRddDF {
case class schema(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:String,age:String,phone1:String,phone2:String,email:String,web:String)

def main(args:Array[String]):Unit={
		val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

				val sc =  new SparkContext(conf)  // President
				sc.setLogLevel("ERROR") 
				
				val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
				import spark.implicits._

				println("=================Raw Data============")
				val data = sc.textFile("file:///C:/data/usdatawh.csv")
				data.foreach(println)
				
				println("==============Map Split=========")
				val mapsplit = data.map( x => x.split(","))

				println("==============Schema RDD=========")
				val schemardd = mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11),x(12)))

				println("=================Schema DataFrame================")
				val df = schemardd.toDF()
				df.show()   // here show is action
}
}