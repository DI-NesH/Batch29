package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkComplexJsonURLToFlattenNRevertsToComplex {
  def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("Spark").setMaster("local[*]")  // Vice President
					val sc =  new SparkContext(conf)  // President
					sc.setLogLevel("ERROR") 

					val spark = SparkSession.builder().getOrCreate()   //==============Do the import for sparksession declare at the top=========
					import spark.implicits._

					println("=======Reading json Fom API=======")
					val data = scala.io.Source
					           .fromURL("https://randomuser.me/api/0.8/?results=10").mkString
				  
				  val rdd = sc.parallelize(List(data))
				  val df  = spark.read.json(rdd)    /// strikethrough is on json as it is depricated
				  df.show()
				  df.printSchema()
				  
				  println("=======Explode Array=======")
				  val flatdf = df.withColumn("results",explode(col("results")))
				  flatdf.show()
				  flatdf.printSchema()
				  
				  println("=======Flatten Data=======")  
				  val finaflat = flatdf
				                .select(
                            "nationality",
                            "results.user.cell",
                				    "results.user.dob",
                				    "results.user.email",
                				    "results.user.gender",
                				    "results.user.location.city",
                				    "results.user.location.state",
                				    "results.user.location.street",
                				    "results.user.location.zip",
                				    "results.user.md5",
                				    "results.user.name.first",
                				    "results.user.name.last",
                				    "results.user.name.title",
                				    "results.user.password",
                				    "results.user.phone",
                				    "results.user.picture.large",
                				    "results.user.picture.medium",
                				    "results.user.picture.thumbnail",
                				    "results.user.registered",
                				    "results.user.salt",
                				    "results.user.sha1",
                				    "results.user.sha256",
                				    "results.user.username",
                				    "seed",
                				    "version"
				                ) 
				                         
				   finaflat.show()
				   finaflat.printSchema()
				    println("=======Complex Data=======")  
             val compdf = finaflat.select(
                          col("nationality"),
                          struct(
                                col("cell"),
                                col("dob"),
                                col("email"),
                                col("gender"),
                                struct(
                                       col("street"),
                                       col("city"),
                                       col("state"),
                                       col("zip")
                                     ).alias("location"),
                               col("md5"),
                                struct(
                                       col("title"),
                                       col("first"),
                                       col("last")
                                     ).alias("name"),
                              col("password"),
                              col("phone"),
                              struct(
                                     col("large"),
                                     col("medium"),
                                     col("thumbnail")
                                   ).alias("picture"),
                             col("registered"),
                             col("salt"),
                             col("sha1"),
                             col("sha256"),
                             col("username")
                 ).alias("user"),
                  col("seed"),
                  col("version")
                 ).groupBy("nationality","seed","version")
                  .agg(
                      collect_list(
                          struct(
                              "user"
                              )
                          ).alias("result"))

           compdf.show()
				   compdf.printSchema()
	}
}