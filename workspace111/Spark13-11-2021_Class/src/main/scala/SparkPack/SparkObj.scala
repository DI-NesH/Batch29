package SparkPack

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import org.apache.spark.streaming.Duration
import org.apache.spark._
import org.apache.spark.sql._
import com.amazonaws.protocol.StructuredPojo
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration
import com.amazonaws.services.dynamodbv2.model.BillingMode
import com.amazonaws.services.cloudwatch.AmazonCloudWatch
import org.apache.hadoop.fs.s3a.S3AFileSystem
import com.fasterxml.jackson.dataformat.cbor.CBORFactory
import com.fasterxml.jackson.core.TSFBuilder
import org.apache.spark.streaming.kinesis.KinesisUtils

object SparkObj {
  
  def b2s(a: Array[Byte]): String = new String(a)
  
   def main(args:Array[String]):Unit={
     val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")

					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession
					.builder()
					.getOrCreate()

					import spark.implicits._
					
					val ssc = new StreamingContext(conf,Seconds(2))
					
					val kinesisStream111= KinesisUtils.createStream(
					ssc, 
					"dineshshri",  //uniquename
					"dinesh",     // qname
					"https://kinesis.ap-south-1.amazonaws.com",
					"ap-south-1",
					InitialPositionInStream.TRIM_HORIZON, 
					Seconds(1), 
					StorageLevel.MEMORY_AND_DISK_2)
					
					
					val finalstream=kinesisStream111.map(x=>b2s(x))
					
					finalstream.print()
					
					ssc.start()
					ssc.awaitTermination()
   }
}