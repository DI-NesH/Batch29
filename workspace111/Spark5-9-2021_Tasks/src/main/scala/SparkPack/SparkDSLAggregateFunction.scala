package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDSLAggregateFunction {
   def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("================= CSV Read Data============")
					val  df =  spark.read.format("csv").option("header","true").load("file:///C:/data/txns_head")
					df.persist()
					df.show()

					println("================Aggregate Function============")
					val fdf= df.groupBy("category").agg(sum("amount").as("total_amount"),count("txnno").as("txn_count"))
					
					fdf.show()
	}
}