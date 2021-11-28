package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkProject1 {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._

					println("========================= Step 2 ========Raw Avro data================================================")
					val  avrodf =  spark.read.format("com.databricks.spark.avro").load("file:///C:/data/Project/projectsample.avro");
			avrodf.show()

			println("========================= Step 3 ======== WebApi Url data=============================================")
			val data = scala.io.Source
			.fromURL("https://randomuser.me/api/0.8/?results=10").mkString

			val rdd = sc.parallelize(List(data))
			val df  = spark.read.json(rdd)    /// strikethrough is on json as it is depricated
			df.show()
			df.printSchema()

			val flatdf = df.withColumn("results",explode(col("results")))
//			.withColumn("username",regexp_replace(col("results.user.username"),"[0-9]",""))
			flatdf.show()
			//    			flatdf.printSchema()

			println("======================== Step 4 flatten dataframe=====================================================")
			val finalflat = flatdf.select(
					"nationality",
					"results.user.cell",
					"results.user.dob",
					"results.user.email",
					"results.user.gender",
					"results.user.location.city",
					"results.user.location.state",
					"results.user.location.street",
					"results.user.location.zip",
					"results.user.md5",
					"results.user.name.first",
					"results.user.name.last",
					"results.user.name.title",
					"results.user.password",
					"results.user.phone",
					"results.user.picture.large",
					"results.user.picture.medium",
					"results.user.picture.thumbnail",
					"results.user.registered",
					"results.user.salt",
					"results.user.sha1",
					"results.user.sha256",
					"results.user.username",
//					"username",
					"seed",
					"version"
					)
			finalflat.show()
			//				   finalflat.printSchema()

			println("========================step 5 removed numericals Dataframe=============================================")
			val albhafinalflat = finalflat.withColumn("username",regexp_replace(col("username"),"[0-9]",""))
			albhafinalflat.show()
			
			println("====================== Step 6 =========Joined Dataframe=============================================")
			val joindata = broadcast(avrodf).join(albhafinalflat,Seq("username"),"left")
			joindata.show()
//			joindata.printSchema()
			//    			 joindata.filter("person_country = 'Cuba'")
			//    			       joindata.select("*").show(false)

			//    			 val condition = col("nationality") == "null"
			val df1 = joindata.filter(col("nationality").isNull)
			//    			 val df1 = joindata.filter(condition)
			//    			joindata.createOrReplaceTempView("CUSTOMER")
			//    			val not_available = joindata.filter(col("nationality") === "beautifulpeacock")
			//    			not_available.show()
			println("=================== Step 7 a ============Not available customers=============================================")
			df1.show()

			println("==================  Step 7 b =============available customers=============================================")
			val df2 = joindata.filter(col("seed").isNotNull)
			//    			val df2 = spark.sql("select * from CUSTOMER where nationality = null")
			df2.show()

			println("=============== Step 8 ================Null handled dataframe=============================================")

			val df3 = df1.na.fill("Not available")
			val df4 = df3.na.fill(0)
			df4.show(false)
			//    			df1.withColumn("id", when($"id".isNull, 0).otherwise(1)).show(false)
			//    			 df3.na.fill(0).show(false)
			//    			df2.printSchema()

			println("=============== Step 9 a ================not available customers with current date dataframe=============================================")

			df4.withColumn("current_date", date_format(current_timestamp(), "yyyy-MM-dd")).show()

			println("=============== Step 9 b ================available customers with current date dataframe=============================================")

			df2.withColumn("current_date",date_format(current_timestamp(),"yyyy-MM-dd")).show()
	}
}