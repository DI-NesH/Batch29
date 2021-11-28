package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkKafkaObj {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					.set("spark.driver.allowMultipleContexts","true")

					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val kafkadf = spark.readStream
      					.format("kafka")
      					.option("kafka.bootstrap.servers","localhost:9092")
      					.option("subscribe","sparksstp")
      					.load()
      					.withColumn("value", expr("cast(value as string)"))
      					.selectExpr("concat(value,',zeyo') as value")


					kafkadf.writeStream
    					.format("kafka")
    					.option("kafka.bootstrap.servers","localhost:9092")
    					.option("topic","spark_out")
    					.option("checkpointLocation","file:///C:/data/sstr2eamk")
    					.start()
    					.awaitTermination()
	}
}