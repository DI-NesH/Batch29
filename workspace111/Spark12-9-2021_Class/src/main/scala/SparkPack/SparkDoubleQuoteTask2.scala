package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._ 
object SparkDoubleQuoteTask2 { 
case class schema(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,
		state:String,zip:String,age:String,phone1:String,phone2:String,email:String,web:String)
    def main(args:Array[String]):Unit={
    		val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
    				val sc =  new SparkContext(conf)  // President
    				sc.setLogLevel("ERROR") 
    
    				val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
    				import spark.implicits._
    
    				println("=================CSV Read Data============")
    				val df = spark.read.format("csv").option("header", "true").load("file:///C:/data/usdata.csv")
    				df.show(false)
    
    				val data = sc.textFile("file:///C:/data/usdata.csv")
    				val header = data.first()                              // contains only header
    				val datawoh = data.filter(x => !(x.contains(header)))  // contains data without header
    
    				val wq = datawoh.filter(x => x.contains("\""))          // contains data with quotes
    				val woq = datawoh.filter(x => !(x.contains("\"")))      // contains data without quotes
    
    				// without quotes data
    				val woqdf = woq.map(x => x.split(","))
    				.map(x => schema(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12)))
    				.toDF()                               
    
    				val wqdf = wq.map(x => x.split(","))
    				.map(x => schema(x(0), x(1), x(2) + x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13)))
    				.toDF()
    
    				  
    				val uniondf = wqdf.union(woqdf)
    
    				uniondf.show(200)
    
    }
}