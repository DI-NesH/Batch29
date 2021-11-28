package SparkSchemaRddToDF

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object SparkSchemaRddToDFObj {
	//case class needs to be declare outside the main method for schema RDD
case class schema(c0:String,c1:String,c2:String,c3:String,c4:String,c5:String,c6:String,c7:String,c8:String)

def main(args:Array[String]):Unit={
		val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

				val sc =  new SparkContext(conf)  // President
				sc.setLogLevel("ERROR") 

				println("=================raw data============")
				val data = sc.textFile("file:///C:/data/txnsample.txt")
				data.foreach(println)

				println("==============Map Split=========")
				val mapsplit = data.map( x => x.split(","))

				println("==============Schema RDD=========")
				val schemardd = mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

				//Initialize SparkSession Object
				val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
				import spark.implicits._

				// define case class outside the main method

				// convert schema rdd into DataFrame
				
				println("=================dataframe================")
				val df = schemardd.toDF()
				df.show()   // here show is action
				df.createOrReplaceTempView("txndf")

				val resultdf =  spark.sql("select * from txndf where c5 like '%Gymnastics%'")
				 
				println("=================resultant dataframe================")
				resultdf.show()   // here show is action
				
	/*   println("=============filter schema rdd==============")
     
     	val filtersceham = schemardd.filter(x=>x.c5.contains("gymnastics"))
     
     	filtersceham.foreach(println)
  */
}

}