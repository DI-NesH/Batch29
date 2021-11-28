package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession

object SparkDataFrameSeemlessWrite {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._


			println("=================Read CSV File & Write as parquet File============")
			val txndf =  spark.read.format("csv").option("header","true").option("delimiter","~").load("file:///C:/data/txnsamplepipe.txt");
			txndf.show()
			//Write the data as parquet to directory (ensure non existing directory)
			//	txndf.coalesce(1).write.format("parquet").mode("overwrite").save("file:///C:/data/parquetdirectory")
			txndf.write.format("parquet").save("file:///C:/data/parquetdirectory")

			println("=================Read Parquet File & Write as JSON File ============")
			// Read the generated parquet file from that directory
			val parquetdf =  spark.read.format("parquet").load("file:///C:/data/parquetdirectory");
			parquetdf.show()
			//	Write the data as json
			//	txndf.coalesce(1).write.format("json").mode("overwrite").save("file:///C:/data/jsondirectory")
			parquetdf.write.format("json").save("file:///C:/data/jsondirectory")

			println("=================Read JSON File & Write as ORC File============")
			// Read the generated json from that directory
			val jsondf = spark.read.format("json").load("file:///C:/data/jsondirectory");
			jsondf.show()
			//Write the as orc
			//	txndf.coalesce(1).write.format("orc").mode("overwrite").save("file:///C:/data/orcdirectory")
			jsondf.write.format("orc").save("file:///C:/data/orcdirectory")

			println("=================Read ORC File & Write as CSV File============")
			//Read the generated orc from that directory
			val orcdf = spark.read.format("orc").load("file:///C:/data/orcdirectory");
			orcdf.show()
			
			//Write the as csv with header true and delimiter ~ to a new directory
			orcdf.write.format("csv").option("header","true").option("delimiter","~").save("file:///C:/data/txntarget")

			
			/*
			    txndf.show(false)

			    txndf.createOrReplaceTempView("txndf")

			    val finaldf = spark.sql("select * from txndf where age > 20")
			    
			    finaldf.show()
			 */




	}
}