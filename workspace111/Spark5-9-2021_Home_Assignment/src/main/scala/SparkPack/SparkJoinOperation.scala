package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkJoinOperation {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					val df1 =spark.read.format("csv").option("header","true").load("file:///C:/data/j1.csv")
					df1.persist()
					println("===============Raw df1 show======================")
					df1.show()
					val df2 =spark.read.format("csv").option("header","true").load("file:///C:/data/j2.csv")
					df2.persist()
					println("===============Raw df2 show======================")
					df2.show()
					println("===============inner join======================")
					val inner_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"inner").show()
					println("===============outer join======================")
					val outer_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"outer").show()
					println("===============left join======================")
					val left_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"left").show()
					println("===============right join======================")
					val right_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"right").show()
					println("===============leftanti join======================")
					val left_anti_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"leftanti").show()
					println("===============leftsemi join======================")
					val left_semi_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"leftsemi").show()
					println("===============rightanti join======================")
					val right_anti_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"rightanti").show()
					println("===============rightsemi join======================")
					val right_semi_joindf = df1.join(df2,df1("txnno")===df2("txnno"),"rightsemi").show()
	}
}