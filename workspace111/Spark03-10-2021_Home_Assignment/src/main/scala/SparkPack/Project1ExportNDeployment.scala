package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.HiveContext

object Project1ExportNDeployment {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)
			
					sc.setLogLevel("ERROR")
					
					val spark =   SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate() 
					import spark.implicits._
					
					val hc = new HiveContext(sc)
	        import hc.implicits._
	        
	        println("======================= Step 2 ========Raw data========================================")
			    val df = spark.read.format("com.databricks.spark.avro").load("file:///C:/data/Project/projectsample.avro")
			    df.show()
			    df.printSchema()
			    
			    println("======================== Step 3 ========Url data===================================")
			    val urldata = scala.io.Source.
			              fromURL("https://randomuser.me/api/0.8/?results=10").mkString
          val rdd = sc.parallelize(List(urldata))
          val urldf = spark.read.json(rdd)
					urldf.show()
			    urldf.printSchema()
			    
			    println("========================step 4  =======flatten dataframe===========================")
			    
			    val flatdf = urldf.withColumn("results",explode(col("results")))
			                          .select (
			                                  "nationality",
			                                  "results.user.cell","results.user.dob","results.user.email",
                              					"results.user.gender",
                              					"results.user.location.city","results.user.location.state",
                              					"results.user.location.street","results.user.location.zip",
                              					"results.user.md5",
                              					"results.user.name.first","results.user.name.last",
                              					"results.user.name.title",
                              					"results.user.password","results.user.phone",
                              					"results.user.picture.large","results.user.picture.medium",
                              					"results.user.picture.thumbnail",
                              					"results.user.registered",
                              					"results.user.salt","results.user.sha1",
                              					"results.user.sha256","results.user.username",
                              					"seed",
                              					"version"
			                                  )
            flatdf.show()
            
            println("=================== Step 5  =======removed numericals Dataframe======================")
            
            val rmnumflatdf = flatdf.withColumn("username",regexp_replace(col("username"),"[0-9]",""))
            rmnumflatdf.show()
            
            println("=================== Step 6 =======Joined Dataframe===================================")
            
            val joindf = df.join(broadcast(rmnumflatdf), Seq("username"), "left")
            joindf.show()
            
            println("================== Step 7 a ========Not available customers==========================")
			          
            val df_non_available_customer = joindf.filter(col("nationality").isNull)
            df_non_available_customer.show()
            
            println("================== Step 7 b ========available customers==============================")
            
            val df_available_customer = joindf.filter(col("seed").isNotNull)
            df_available_customer.show()
            
            println("========Write both available and not available customer dataframe to Hive============")
            
//            df_available_customer.write.format("parquet").saveAsTable("customerdb.available_customer")
////            
//            df_non_available_customer.write.format("parquet").saveAsTable("customerdb.available_non_customer")
            
            
            
            
            
	}
}