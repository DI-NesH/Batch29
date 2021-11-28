package SparkTxnsPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class

object TxnsDataObj {

	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President

					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR")

					/*  println("==================== Raw Data =============")
    val data = sc.textFile("file:///C:/data/txns")
    data.take(10).foreach(println)

    println("==================== FilData =============")
    val gymdata = data.filter( x => x.contains("Gymnastics"))
    gymdata.take(10).foreach(println)

    gymdata.coalesce(1).saveAsTextFile("file:///C:/data/gymdataprocessed")

    println("==================== Data Written =============")
					 * 
					 */

					println("==================== Raw Data =============")
					val data = sc.textFile("file:///C:/data/usdata.csv")
					data.take(10).foreach(println)

					println("==================== FilData =============")
					val fildata = data.filter( x => x.length >200)
					fildata.take(10).foreach(println)

					println("====================flat Data=============")
					val flatdata = fildata.flatMap(x =>x.split(","))
					flatdata.foreach(println)

					println("==================== sPace remove Data =============")
					val nospacedata = flatdata.map( x => x.replace(" ",""))
					nospacedata.foreach(println)

					println("==================== Concat usdata with ,zeyo =============")
					val concatdata  = nospacedata.map(x=>x + ",zeyo")
					concatdata.foreach(println)

					concatdata.coalesce(1).saveAsTextFile("file:///C:/data/finalusdataprocessed")

          println("==================== Data Written =============")

	}

}