package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkFlattenJsonDataToComplexJsonData {
   def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					
					
			println("=======Reading reqres.json=======")
			val df=spark.read.format("json").option("multiLine", "true").load("file:///C://data/complexjson/reqres.json")
			df.show()
      df.printSchema()
      
      println("=======Explode Array=======")
      val explodedf =  df.withColumn("data",explode(col("data")))      
      explodedf.show()
      explodedf.printSchema()
         
      println("=======Flatten Data=======")  
      val flatdf = explodedf.select(
                    col("data.*"),
                    col("page"),
                    col("per_page"),
                    col("support.text"),
                    col("support.url"),
                    col("total"),
                    col("total_pages") 
                  )
      flatdf.show()
      flatdf.printSchema()
      
      println("=======Complex Data=======")
      val comptdf =  flatdf.select(
              col("avatar"),
              col("email"),
              col("first_name"),
              col("id"),
              col("last_name"),
							col("page"),
              col("per_page"),
							struct(
									col("text"),
									col("url")
									).alias("support"),

							col("total"),
							col("total_pages")
          ).groupBy("page","per_page","support","total","total_pages")
           .agg(collect_list(struct("avatar","email","first_name","id","last_name")).alias("data"))
					comptdf.show()
					comptdf.printSchema()       
	}
}