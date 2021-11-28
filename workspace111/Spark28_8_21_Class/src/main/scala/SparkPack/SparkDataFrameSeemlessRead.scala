package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession

object SparkDataFrameSeemlessRead {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._
					
					println("=================Read CSV Data without Header Option============")
					val def1 =  spark.read.format("csv").load("file:///C:/data/usdata.csv");
			    def1.show(false)
			    
			    println("=================Read CSV Data with Header Option============")
					val def2 =  spark.read.format("csv").option("header","true").load("file:///C:/data/usdata.csv");
			    def2.show(false)
			    
			    println("=================Read Parquet Data============")
					val def3 =  spark.read.format("parquet").load("file:///C:/data/part_par.parquet");
			    def3.show(false)
			    
			    println("=================Read ORC Data============")
					val def4 =  spark.read.format("orc").load("file:///C:/data/part_orc.orc");
			    def4.show(false)
					
	}
}