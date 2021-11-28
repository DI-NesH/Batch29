package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession

object SparkDataFrameSeamlessAvroReadWrite {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("================= Avro Read Data Check============")
					val  avrodf =  spark.read.format("com.databricks.spark.avro").load("file:///C:/data/partdata.avro");
			avrodf.show()
			
			    println("================= Avro Write Data Check============")    
			    avrodf.write.format("com.databricks.spark.avro").save("file:///C:/data/avrodata")
	}
}