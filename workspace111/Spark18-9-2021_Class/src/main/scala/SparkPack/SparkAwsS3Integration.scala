package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkAwsS3Integration {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder()
					.config("fs.s3a.access.key","AKIAQJ35YRX5GAI3I3WR")
					.config("fs.s3a.secret.key","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")
					.getOrCreate()                       //==============Do the import for sparksession declare at the top=========

					import spark.implicits._

					println("=================CSV Read Data============")
					val df= spark.read.format("csv").option("header","true").load("s3a://zeyonifibucket/srcdir/usdata.csv")
					
					df.show() 
					
					println("=================Avro Write Data============")
					df.write.format("com.databricks.spark.avro").mode("overwrite").save("s3a://zeyonifibucket/tardir/ShriDinesh_write")
					
					println("Done")

	} 				

}