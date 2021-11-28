package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkTwitterJson {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/Practise/twitter.json")
					
					df.show()
					df.printSchema()
					
//					val finaldf = df.
//					                 select(
//					                     "created_at","id","id_str","text","truncated","entities.hashtags.*","entities.symbols"
//					                     ,"entities.user_mentions"
//					                     ,"urls.url","urls.expanded_url","urls.display_url","urls.indices"
//					                     ,"source","user.","","","",
//					                     )
//					                .withColumn("hashtags", explode(col("hashtags")))
//					                .withColumn("symbols", explode(col("symbols")))
//					                .withColumn("user_mentions", explode(col("user_mentions")))

  }
}