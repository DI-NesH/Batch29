package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkComplexDataStructArray {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("=================Json Read Data============")
					val df= spark.read.format("json").option("multiLine","true").load("file:///C:/data/complexjson/file1.json")

					df.show()
					df.printSchema()

					
					val flattdf1 = df.
					  select(
    					    "Students",
    					    "address.*",
    							"orgname",
    							"trainer"
							)

					flattdf1.show()
					flattdf1.printSchema()



					val flattdf2 =  flattdf1
					            .withColumn("Students",explode(col("Students")))


					flattdf2.show()
					flattdf2.printSchema()

	} 
}