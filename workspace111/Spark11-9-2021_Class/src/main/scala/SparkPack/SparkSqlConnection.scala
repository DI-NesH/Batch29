package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.mysql.jdbc._

object SparkSqlConnection {
	def main(args:Array[String]):Unit={

			val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

					val sc = new SparkContext(conf)   // president
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					val sqldf = spark.
					read.
					format("jdbc").
					option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/txn").
					option("driver","com.mysql.jdbc.Driver").
					option("dbtable","cash_table").
					option("user","root").
					option("password","Aditya908").
					load()

					sqldf.show()

	}

}