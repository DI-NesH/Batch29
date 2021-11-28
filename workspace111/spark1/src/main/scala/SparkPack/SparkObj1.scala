package SparkPack

import org.apache.spark._
import sys.process._

object SparkObj1 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("data").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")


					val data = sc.textFile("file:///home/cloudera/data/txns")
					val gymdata=data.filter(x=>x.contains("Gymnastics"))

					"hadoop fs -rmr /user/cloudera/gymdata_prd_data".!

					gymdata.saveAsTextFile("/user/cloudera/gymdata_prd_data")

	}

}