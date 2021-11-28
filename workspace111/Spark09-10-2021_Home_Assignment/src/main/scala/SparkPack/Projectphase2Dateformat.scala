package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.SparkSession
import com.databricks.spark.avro
import org.apache.spark.sql.hive.HiveContext
import java.time._
import java.time.{ZonedDateTime, ZoneId}
import java.time.format.DateTimeFormatter

object Projectphase2Dateformat {
    	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)
			
					sc.setLogLevel("ERROR")
					
					val spark =   SparkSession.builder().enableHiveSupport().config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate() 
					import spark.implicits._
					
					val hc = new HiveContext(sc)
	        import hc.implicits._
	        
	        val previousday = ZonedDateTime.now(ZoneId.of("UTC")).plusDays(-1)
          val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
          val yesterday_date = formatter format previousday
	        val previousDay = LocalDate.now.plusDays(-1)
//	        val yesterday_date= java.time.LocalDate.now.minusDays(1)
	          println(previousDay)
	            println(yesterday_date)
			          
			    println("======================= Step 2 ========Raw data========================================")
//			    val df = spark.read.format("com.databricks.spark.avro").load("hdfs:/user//cloudera//data/projectsample.avro")
//			    df.show()
//			    df.printSchema()       
			
    	}
}