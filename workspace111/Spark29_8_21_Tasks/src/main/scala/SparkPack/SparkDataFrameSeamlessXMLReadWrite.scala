package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession

object SparkDataFrameSeamlessXMLReadWrite {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("=================Check XML Read Data============")
//					val  xmldf =  spark.read.format("com.databricks.spark.xml").load("file:///C:/data/data.xml")
			  val xmldf= spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///C:/data/bookdata.xml")
			  xmldf.show()
			  
			  println("=================Write XML as Avro============")
			  xmldf.write.format("com.databricks.spark.avro").mode("overwrite").save("file:///C:/data/xmltoavro")
			 
	}
}