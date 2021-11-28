package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkFlattenedFiles {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					/*
					println("******************Reading Picture_multiline****************")
					
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/picture_multiline.json")
					df.show()
					df.printSchema()
					
					  println("=======flattened picture multiline df =======")
					
					val pic_flatdf = df
					    .select (
					                "id",
            							"image.height",
            							"image.url",
            							"image.width",
            							"name",
            							"thumbnail.height",
            							"thumbnail.url",
            							"thumbnail.width",
            							"type"
					             )
            pic_flatdf.show()
            pic_flatdf.printSchema()
          */
			/*		println("=======Reading reqapi.json flattened=======")
			val raqapi_df=spark.read.format("json").option("multiLine", "true").load("file:///C://data/reqapi.json")
			raqapi_df.show()
      raqapi_df.printSchema()
      
      println("=======flattened reqapi df =======")
      
			val raqapi_flatdf =   raqapi_df
			     .select (
			                "data.*",
        							"support.*",
        							"page",
        							"per_page",
        							"support.text",
        							"support.url",
        							"total",
        							"total_pages"
			             )
           raqapi_flatdf.show()
           raqapi_flatdf.printSchema()
       */
		 /*			
				println("=======Reading pets.json=======")
			val pets_df=spark.read.format("json").option("multiLine", "true").load("file:///C://data/pets.json")
			pets_df.show()
      pets_df.printSchema()
      
      println("=======flattened pets df =======")
      val pets_flatdf =  pets_df
            .select  (
                        "Address.Permanent address",
      									"Address.current Address",
      									"Boolean",
      									"Name",
      									"Pets"
                     ) 
       pets_flatdf.show()
       pets_flatdf.printSchema()
     */
					
			println("=======Reading actors.json=======")
			val actors_df=spark.read.format("json").option("multiLine", "true").load("file:///C://data/actors.json")
			actors_df.show()
      actors_df.printSchema()
      
      val flat_actorsdf =  actors_df
          .withColumn("Actors",explode(col("Actors")))
              .select (
                        "Actors.*",
                        "country",
                        "version"
                      )
          .withColumn("Children",explode(col("Children")))
                      
      flat_actorsdf.show()
      flat_actorsdf.printSchema()
             
     // You can do it below way    
     /* println("===========CHILDREN=====================")
      val flat_childdf = flat_actorsdf
              .withColumn("Children",explode(col("Children")))
          
      flat_childdf.show()
      flat_childdf.printSchema()
    */
          
          
	}
}