package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession



object SparkYoutubeJson {
   def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					
					println("******************Reading Youtube JSON****************")
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/Practise/youtube.json")
					df.show()
					df.printSchema()
					
					println("=======Explode Array=======")
					val flatdf =  df.withColumn("items",explode(col("items")))
					flatdf.show()
					flatdf.printSchema()
					
					println("=======Flatten Data=======")  
					val finalflat = flatdf.select(
					                "kind","etag","nextPageToken","regionCode","pageInfo.*",
					                "items.kind","items.etag","items.id.*"
					                )
          finalflat.show()
          finalflat.printSchema()
          
          println("=======Complex Data=======")  
          
          val compdf = finalflat
//                          .select(
//                            col("kind"),col("etag"),col("nextPageToken"),col("regionCode"),
//                            struct(
//                                     col("totalResults"),col("resultsPerPage")
//                                    )
//                          )
                          .groupBy("nextPageToken","regionCode")   //"kind","etag",
                          .agg(
//                              collect_list(
//                                    struct(
//                                        col("kind"),col("etag")   
//                                    )   
//                              ).alias("items"),
                              collect_list(
                                    struct(
                                      col("channelId"),col("videoId")
                                    )
                               ).alias("id")
                                    
                            )  
            compdf.show()
            compdf.printSchema()
   }
}