package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession

object SparkModes {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._
					
					println("=================CSV Read Data============")
					val  df =  spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")
					
					println("=================No Mode With Parquet Write Data============")
					 df.write.format("parquet").save("file:///C:/data/sparkmodes")
	}
}