package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
object SparkXmlWrite {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate() //==============Do the import for sparksession declare at the top=========

					import spark.implicits._

					println("=================CSV Read Data============")
					val df= spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv")

					df.show()

					println("=================Xml Write Data============")
					df.write.format("com.databricks.spark.xml").option("rowTag","usdata").mode("overwrite").save("file:///C:/data/xmlwritespark")

					println("Done")

	} 
}