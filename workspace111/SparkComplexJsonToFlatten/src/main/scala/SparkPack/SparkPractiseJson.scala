package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkPractiseJson {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._
					
					println("******************Reading Practise JSON****************")
					val df = spark.read.format("json").option("multiLine","true").load("file:///c:/data/Practise/Practise.json")
					df.show()
					df.printSchema()
					
					println("******************Flattened Practise Data****************")
					val flatdf = df.withColumn("colors", explode(col("colors")))
					               .select("colors.color","colors.category","colors.type","colors.code.rgba","colors.code.hex")
					               .withColumn("rgba", explode(col("rgba")))
          flatdf.show()
					flatdf.printSchema()
  }
  
}