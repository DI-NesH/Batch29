package SparkPack

import org.apache.spark.SparkContext  // imported SparkContext Class
import org.apache.spark.SparkConf    // imported SparkConf Class
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkDSLOperator_API {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("================= CSV Read Data============")
					val  df =  spark.read.format("csv").option("header","true").load("file:///C:/data/txns_head")
					df.persist()
					df.show()
					
					println("*******IN Operator*******")
					val inopdf = df.filter(col("category").isin("Gymnastics","Team Sports"))
					inopdf.show()
					
					println("******NOT IN Operator******")
					val notinopdf = df.filter(!col("category").isin("Gymnastics","Team Sports"))
					notinopdf.show()
					
					println("******Filter ,AND operator and equal operator ******")
					val filNop = df.filter(col("category") === "Gymnastics" && col("spendby") === "cash")
					filNop.show()
					
					println("******* withcolumn and withColumnRenamed API's *******")
					val withcolrenamedf = df.withColumn("txndate",expr("split(txndate,'-')[2]")).withColumnRenamed("txndate","year").withColumnRenamed("txnno","txnno")
					withcolrenamedf.show()				
					
					println("******* Add new column with provided expression resultset******")
					val expresdf = df.withColumn("Year",expr("split(txndate,'-')[2]"))
					expresdf.show()
					
					println("******* LIKE Operator*******")
					val likedf = df.filter(col("category").like("Gymnastics%")) 
					likedf.show()
					
					println("*******Case statement ********")
					val casestatedf = df.withColumn("Spend By Check",expr("case when spendby= 'credit' then 0 else 1 end"))
					casestatedf.show()

					println("================Aggregate Function============")
					val fdf= df.groupBy("category").agg(sum("amount").as("total_amount"),count("txnno").as("txn_count"))
					fdf.show()
	}
}