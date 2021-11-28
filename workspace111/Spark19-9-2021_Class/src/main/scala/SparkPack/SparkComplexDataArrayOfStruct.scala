package SparkPack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkComplexDataArrayOfStruct {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("=================Json Read Data============")
					val df= spark.read.format("json").option("multiLine","true").load("file:///C:/data/complexjson/file2.json")

					df.show(false)
					df.printSchema()

					val flatdf =  df
					            .withColumn("Students",explode(col("Students")))


					flatdf.show()
					flatdf.printSchema()
					
					val finalflat = flatdf.
					  select(
    					    "Students.*",
    					    "address.*",
    							"orgname",
    							"trainer"
						
							)

					finalflat.show()
					finalflat.printSchema()

	} 
}