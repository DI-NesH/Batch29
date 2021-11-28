package TxnDuplicatePack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class

object TxnDuplicateObj {

	def main(args:Array[String]):Unit={

			val conf =  new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc = new SparkContext(conf)     // President
					sc.setLogLevel("ERROR") 

					println("==================== Raw Data =============") 
					val data = sc.textFile("file:///C:/data/txnsample.txt")
					data.foreach(println)
					
					println("==================== Remove Duplicates in RDD =============")
					val distinctdata = data.distinct()
					distinctdata.foreach(println)
	}
}