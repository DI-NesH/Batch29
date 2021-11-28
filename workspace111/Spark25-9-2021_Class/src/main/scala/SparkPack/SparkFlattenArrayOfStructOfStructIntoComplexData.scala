package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkFlattenArrayOfStructOfStructIntoComplexData {
   def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					
					
			println("=======Reading complex3.json=======")
			val df=spark.read.format("json").option("multiLine", "true").load("file:///C://data/complexjson/complex3.json")
			df.show()
      df.printSchema()
      
      println("=======Explode Array=======")
      val explodedf = df.withColumn("Students",explode(col("Students")))
      explodedf.show()
      explodedf.printSchema()
//         
      println("=======Flatten Data=======")  
      val flatdf = explodedf.select(
                    col("Students.user.*"),
                    col("address.*"),
                    col("orgname"),
                    col("trainer")
                  )
      flatdf.show()
      flatdf.printSchema()
//      
      println("=======Complex Data=======")
      
      val compdf = flatdf.select(
                    col("location"),
                    col("name"),
                    struct(
                         col("permanent_address"),
                         col("temporary_address")
                        ).alias("address"),
                    col("orgname"),
                    col("trainer")
                    )
                  .groupBy("address","orgname","trainer").agg(
                      collect_list(
                          struct(
                              struct(
                                  "location",
                                  "name"
                                  ).alias("user")
                              )  
                          ).alias("Students")
                   )
  
					compdf.show()
					compdf.printSchema()      
	}
  
}