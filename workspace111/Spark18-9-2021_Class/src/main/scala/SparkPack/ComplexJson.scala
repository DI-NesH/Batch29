package SparkPack


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ComplexJson {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("=================Json Read Data============")
					val df= spark.read.format("json").option("multiLine","true").load("file:///C:/data/complexjson/complex1.json")

					df.show()
					df.printSchema()

					val flattdf = df.select(
							"orgname",
							"trainer",
							"address.permanent_address",
							"address.temporary_address"
							)

					println
					println("=================Flattened dataframe============")

					flattdf.show()
					flattdf.printSchema()

	} 
}