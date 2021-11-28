package SparkPack

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object SparkComplexXMLProcessing {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("spark").setMaster("local[*]")
					val sc = new SparkContext(conf)

					sc.setLogLevel("ERROR")
					val spark =   SparkSession.builder().getOrCreate() 
					import spark.implicits._

					println("=======================Reading Transcation.xml=======================")

					val df = spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").
					load("file:///c:/data/complexjson/transactions.xml")
					df.show()
					df.printSchema()

					println("=======================Flatten Data====================") 

					val flatdf = df.withColumn("Transaction",explode(col("Transaction")))
					.withColumn("LineItem", explode(col("Transaction.RetailTransaction.LineItem")))
					.withColumn("Total",explode(col("Transaction.RetailTransaction.Total")))
					.select(
							col("Transaction.BusinessDayDate"),
							col("Transaction.ControlTransaction.OperatorSignOff.*"),
							col("Transaction.ControlTransaction.ReasonCode"),
							col("Transaction.ControlTransaction._Version"),
							col("Transaction.CurrencyCode"),
							col("Transaction.EndDateTime"),
							col("Transaction.OperatorID.*"),
							col("Transaction.RetailTransaction.ItemCount"),
							col("LineItem.Sale.Description"),
							col("LineItem.Sale.DiscountAmount"),
							col("LineItem.Sale.ExtendedAmount"),
							col("LineItem.Sale.ExtendedDiscountAmount"),
							col("LineItem.Sale.ItemID"),
							col("LineItem.Sale.Itemizers.*"),
							col("LineItem.Sale.MerchandiseHierarchy.*"),
							col("LineItem.Sale.OperatorSequence"),
							col("LineItem.Sale.POSIdentity.*"),
							col("LineItem.Sale.Quantity"),
							col("LineItem.Sale.RegularSalesUnitPrice"),
							col("LineItem.Sale.ReportCode"),
							col("LineItem.Sale._ItemType"),
							col("LineItem.SequenceNumber"),
							col("LineItem.Tax.*"),
							col("LineItem.Tender.Amount"),
							col("LineItem.Tender.Authorization.*"),
							col("LineItem.Tender.OperatorSequence"),
							col("LineItem.Tender.TenderID"),
							col("LineItem.Tender._TenderDescription"),
							col("LineItem.Tender._TenderType"),
							col("LineItem.Tender._TypeCode"),
							col("LineItem._EntryMethod"), 
							col("LineItem._weightItem"),
							col("Transaction.RetailTransaction.PerformanceMetrics.*"),
							col("Transaction.RetailTransaction.ReceiptDateTime"),
							col("Total.*"), 
							col("Transaction.RetailTransaction.TransactionCount"),
							col("Transaction.RetailTransaction._Version"),
							col("Transaction.SequenceNumber"),
							col("Transaction.WorkstationID")
							)
					flatdf.show()
					flatdf.printSchema()
	}
}