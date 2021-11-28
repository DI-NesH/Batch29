package SparkPackData

import org.apache.spark._
import sys.process._

object SparkDataObj {


	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("data").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")

					val data = sc.textFile("file:///C:/data/txns")
					val gymdata=data.filter(x=>x.contains("Gymnastics"))
					gymdata.foreach(println)

	}
}