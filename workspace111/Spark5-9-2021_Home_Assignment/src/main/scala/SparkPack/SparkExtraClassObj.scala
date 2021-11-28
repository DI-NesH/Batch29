package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkExtraClassObj {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("================= CSV Raw Read Data============")
					val  avrodf =  spark.read.format("csv").option("header","true").load("file:///home/cloudera/data/txns")
					avrodf.show()

					println("================Avro Format============")
					avrodf.write.format("com.databricks.spark.avro").mode("overwrite").save("hdfs:/user/cloudera/avrodatawrite")

				
	}
}