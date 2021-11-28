package SparkPack

import org.apache.spark.sql.functions.explode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object SparkObj {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("ES").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
			
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._

					val df = spark.read.option("header", "true").csv("file:///C:/data/txns_head")
					df.show()
					
					df.write.format("org.apache.spark.sql.cassandra")
					.option("spark.cassandra.connection.host","localhost")
					.option("spark.cassandra.connection.port","9042")
					.option("keyspace", "zeyotxn")
					.option("table","txnrecords")
					.save()
					
		//.set("spark.cassandra.connection.host", "localhost")
			//		.set("spark.cassandra.connection.port", "9042")
//
	}
}