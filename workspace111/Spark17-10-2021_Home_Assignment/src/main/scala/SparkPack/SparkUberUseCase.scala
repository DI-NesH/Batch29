package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.SparkSession
import java.util.Properties
import com.mysql.jdbc._;

object SparkUberUseCase {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					
					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					println("=================Check Text Read Data============")
					val data = spark.read.format("csv").option("header","true").load("file:///home/cloudera/data/uber")
//					val data = spark.read.format("csv").option("header","true").load("file:///c:/data/uber")
					    data.show()
					    data.printSchema()     
		     println("=================Change Date Format & Convert Date to Day============")
					    val df = data.withColumn("date",to_timestamp(col("date"),"MM/dd/yyyy"))
					    .withColumn("active_vehicles",col("active_vehicles").cast(IntegerType))
					    .withColumn("trips", col("trips").cast("integer"))
					    .withColumn("Day", date_format(col("date"),"E"))  // convert date to day
					    df.show()
					    
					    println("================Aggregate Function============")
					    val maxTrip = df.groupBy("dispatching_base_number").agg(max("trips").as("max_trips"))
					     maxTrip.show()  
					     
					    println("================JOIN Condition============")
					    val final_df = df.join(maxTrip, "dispatching_base_number").filter(col("trips") === col("max_trips"))
					     .selectExpr("dispatching_base_number", "Day", "trips")
					     
					     final_df.show()
					     
				       final_df.write.format("jdbc")
                    .option("url","jdbc:mysql://localhost:3306/retail_db")
                    .option("driver","com.mysql.jdbc.Driver")
                    .option("dbtable","TripData")
                    .option("user","root")
                    .option("password","cloudera")
                    .option("useSSL","true")
                    .mode("append")
                    .save();
  }
}