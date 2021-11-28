package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.SparkSession
import scala.io.Source
import com.databricks.spark.avro
import org.apache.spark.sql.hive.HiveContext
import java.time._
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object SparkObj {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					val hc = new HiveContext(sc)
	        import hc.implicits._
	        
	        val previousday = ZonedDateTime.now(ZoneId.of("UTC")).plusDays(-1)
					val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
					val yesterday_date = formatter format previousday
						        val previousDay = LocalDate.now.plusDays(-1)
//						        val yesterday_date= java.time.LocalDate.now.minusDays(1)
						          println(previousDay)
					println(yesterday_date)  
			
					println("======================= Step 2 ========Raw data=============================================")
//					val data = spark.read.format("com.databricks.spark.avro").load(s"file:///C://data//projdata//$yesterday_date/projectsample.avro")
					val data = spark.read.format("com.databricks.spark.avro").load(s"s3://zeyosaii29/yesterday_date/")

					data.show()
					
					println("======================= Step 3 ========url data=============================================")
					val html = Source.fromURL("https://randomuser.me/api/0.8/?results=1000")
				  val s = html.mkString
				  val rdd = sc.parallelize(List(s))
					val urldf = spark.read.json(rdd)
					urldf.show()
					urldf.printSchema()
					
					println("========================step 4 flatten dataframe=============================================")
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
					
					println("========================step 5 removed numericals Dataframe==================================")
					val rm = flatdf.withColumn("username",regexp_replace(col("username"),"[0-9]",""))
					rm.show()

					println("====================== Step 6 =========Joined Dataframe======================================")
					val joindf = data.join(broadcast(rm), Seq("username"), "left")
					joindf.show()
					
					println("================= Step 7 a ========Not available customers===================================")

					val dfnull  = joindf.filter(col("nationality").isNull)
					dfnull.show()

					println("================= Step 7 b ========available customers=======================================")

					val dfnotnull = joindf.filter(col("nationality").isNotNull)
					dfnotnull.show()
					
					println("================ Step 8 ===Null handled dataframe============================================")

					val replacenull= dfnull.na.fill("Not Available").na.fill(0)
					replacenull.show()
					
					println("=============== Step 9 a ==not available customers with current date dataframe===============")
					val replacenull_with_current_date=replacenull.withColumn("current_date",current_date)
					.withColumn("current_date",col("current_date").cast(StringType))

					replacenull_with_current_date.show()


					println("=============== Step 9 b ===available customers with current date dataframe==================")

					val notnull_with_current_date=dfnotnull.withColumn("current_date",current_date)
					.withColumn("current_date",col("current_date").cast(StringType))
					
					notnull_with_current_date.show()

					notnull_with_current_date.printSchema()
					
					val writedir=args(0)

				  notnull_with_current_date.write.format("parquet").save(s"s3://zeyobucketprod//$writedir//availableCust")
				  replacenull_with_current_date.write.format("parquet").save(s"s3://zeyobucketprod//$writedir//notavailableCust")
				  
/*
				notnull_with_current_date.write.format("parquet").mode("overwrite")
				.saveAsTable(s"zeyodb.sai_available")

*/
	}
}