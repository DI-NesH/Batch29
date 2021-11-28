package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkFlattenToComplex {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					
					
			println("=======Reading complex2.json=======")
			val df=spark.read.format("json").option("multiLine", "true").load("file:///C://data/complexjson/complex2.json")
			df.show()
      df.printSchema()
      
      println("=======Explode Array=======")
      val explodedf =  df.withColumn("Students",explode(col("Students")))      
      explodedf.show()
      explodedf.printSchema()
         
      println("=======Flatten Data=======")  
      val flatdf = explodedf.select(
                    col("Students"),
                    col("address.permanent_address"),
                    col("address.temporary_address"),
                    col("orgname"),
                    col("trainer")
                  )
      flatdf.show()
      flatdf.printSchema()
      
      println("=======Struct Creation=======")
      val structdf =  flatdf.select(

							col("Students"),
							struct(
									col("permanent_address"),
									col("temporary_address")
									).alias("address"),

							col("orgname"),
							col("trainer")
          )
					structdf.show()
					structdf.printSchema()
      
					println("=======Array Creation=======")
					val compdf = structdf.groupBy("address","orgname","trainer")
                       .agg(collect_list("Students").alias("Students"))    
          compdf.show()
					compdf.printSchema()
          
          
	}
}