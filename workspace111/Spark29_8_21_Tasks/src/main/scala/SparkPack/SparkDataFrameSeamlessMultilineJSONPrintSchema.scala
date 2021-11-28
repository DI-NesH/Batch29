package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession

object SparkDataFrameSeamlessMultilineJSONPrintSchema {
  
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("=================Check JSON Read Data============")
					val jsondf= spark.read.format("json").option("multiline","true").load("file:///C:/data/randomeuser5.json")
					jsondf.show()
						
					println("=================JSON Schema Print============")
					jsondf.printSchema()
	}
  
}