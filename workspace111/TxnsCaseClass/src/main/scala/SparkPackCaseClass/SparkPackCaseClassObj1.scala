package SparkPackCaseClass

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class

object SparkPackCaseClassObj1 {

def main(args:Array[String]):Unit={

		    val conf =  new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
				val sc = new SparkContext(conf)     // President
				sc.setLogLevel("ERROR") 

				println("==================== Raw Data =============")
				val data = sc.textFile("file:///C:/data/txns")
				data.foreach(println)
				
				
				println("==================== Filter Gymnastics Data =============")
				val gymdata = data.filter( x=>x.contains("Gymnastics"))
				gymdata.foreach(println)
				
				println("==================== Filter Team Sports Data =============")
				val teamsportsdata = data.filter( x=>x.contains("Team Sports"))
				teamsportsdata.foreach(println)
				
				
				println("==================== Union Gymnastics & Team Sports Data =============")
				val gymteamsportunionsdata = gymdata.union(teamsportsdata)
				gymteamsportunionsdata.foreach(println)

				
				println("==================== write it to a file =============")
				gymteamsportunionsdata.coalesce(1).saveAsTextFile("file:///C:/data/final_union_gym_tem_data")
				
				
				println("==================== Data Written =============")
				
}
}