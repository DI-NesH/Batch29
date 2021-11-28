package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkDataRevision {

	//case class needs to be declare outside the main method for schema RDD
case class schema(txnno:String,txndate:String,custno:String,amount:String,category:String,product:String,city:String,state:String,spendby:String)
val collist= List("txnno","txndate","custno","amount","category","product","spendby","state","city")
def main(args:Array[String]):Unit={

		val conf= new SparkConf().setAppName("Spark").setMaster("local[*]") // vice president 

				val sc = new SparkContext(conf)   // president
				sc.setLogLevel("ERROR")

				val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._

				/*println("=====================3======================")
					val lis =  List(1,3,6,7)
					val res = lis.map(x =>x+2) // here x is accumulator & x+2 iterator
					res.foreach(println)
				 */
				/*
					println("=====================4======================")
					val str = List("zeyobron","zeyo","analytics")
					val strres = str.filter(x=>x.contains("zeyo"))
					strres.foreach(println)
				 */
				
					println("=====================5======================")
//					val fil1 =  sc.textFile("C:///data//datasetsRevision//file1.txt")
					val file_1 =  sc.textFile(args(0)) // parameterize file path
					val gymdata = file_1.filter(x=>x.contains("Gymnastics"))
					gymdata.take(10).foreach(println)
				 
						
					 println("===================6 schema RDD=======================")

					// You need to declare case class outside the main method for schema RDD
//					val fil1 =  sc.textFile("C:///data//datasetsRevision//file1.txt")  // 
           val fil1 =  sc.textFile(args(0))       // parameterize file path
					val mapsplit =  fil1.map(x=>x.split(","))

					val schemardd  = mapsplit.map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

					val gymprod = schemardd.filter(x=>x.product.contains("Gymnastics"))
//					val df = gymprod.toDF()
					gymprod.take(10).foreach(println)

					println("=================7 Row RDD==============")

//					val fil2 =  sc.textFile("C:///data//datasetsRevision//file2.txt")
					val fil2 =  sc.textFile(args(1)) // parameterize file path
					val mpsplit =  fil2.map(x=>x.split(","))

					val rowrdd = mpsplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)))

					val filterdata =  rowrdd.filter(x=>x(8).toString.contains("cash"))

					filterdata.take(10).foreach(println)

					println("=================8==============")

					val schemadf = schemardd.toDF()
					println("=================Create DataFrame Using Schema RDD==============")
					schemadf.show() 

					val simpleSchema = StructType(Array(
						StructField("txnno",StringType,true),
						StructField("txndate",StringType,true),
						StructField("custno",StringType,true),
						StructField("amount", StringType, true),
						StructField("category", StringType, true),
						StructField("product", StringType, true),
						StructField("city", StringType, true),
						StructField("state", StringType, true),
						StructField("spendby", StringType, true)
						))

						val rowdf = spark.createDataFrame(rowrdd,simpleSchema)
						println("=================Create DataFrame Using Row RDD==============")	
						rowdf.show()
				 
			
				println("=================9==============")
//				val fil3 = spark.read.format("csv").option("header","true").load("file:///C://data//datasetsRevision//file3.txt")
				val fil3 = spark.read.format("csv").option("header","true").load(args(2))  // parameterize file path
				fil3.show()
				
				println("=================10==============")
//				val fil4 = spark.read.format("json").load("file:///C://data//datasetsRevision//file4.json")
				val fil4 = spark.read.format("json").load(args(3)) // parameterize file path
				fil4.show()
//				val fil5 = spark.read.format("parquet").load("file:///C://data//datasetsRevision//file5.parquet")
				val fil5 = spark.read.format("parquet").load(args(4)) // parameterize file path
				// parameterize file path
				fil5.show()
				
				
				println("=================11==============")
//				val fil6 = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load("file:///C://data//datasetsRevision//file6")
			  val fil6 = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load(args(5)) // parameterize file path

				fil6.show()	
				
				println("=================12==============")

				val uniondf = schemadf.select(collist.map(col):_*)
				.union(rowdf.select(collist.map(col):_*))
				.union(fil3.select(collist.map(col):_*))
				.union(fil4.select(collist.map(col):_*))
				.union(fil5.select(collist.map(col):_*))
				.union(fil6.select(collist.map(col):_*))
				uniondf.show()
				
				println("=================13==============")
							
			val dsldf = uniondf.withColumn("txndate",expr("split(txndate,'-')[2]"))
			.withColumnRenamed("txndate","year")
			.withColumn("status",expr("case when spendby='cash' then 1 else 0 end"))
			.filter(col("txnno") < 50000)
			
			dsldf.show()
			
			println("=================14 Group BY Condition & Aggregate Function(Sum, Count)=============")
			
			val aggdf= dsldf.groupBy("category").agg(sum("amount") alias("sum_amount") , count("status") as ("status_count"))
			
			aggdf.show()
			
			println("=================15=============")
			aggdf.write.format("jdbc")
			.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/txn")
			.option("driver","com.mysql.jdbc.Driver")
			.option("dbtable","cummulative_results")
			.option("user","root")
			.option("password","Aditya908")
			.mode("overwrite")
			.save()
			aggdf.show()
			println("=================Data Written to MYSQL=============")
			
			println("=================16=============")
			aggdf.coalesce(1).write
				.format("com.databricks.spark.avro")
				.mode("append")
				.partitionBy("category")
//				.save("file:///C://data//writevarorevision/avro_16") 
				.save(args(6)) // parameterize file path

}
}