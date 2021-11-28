package SparkPack

object SparkObj3 {

	def main(args:Array[String]):Unit={
	  
	    println("==================== Raw Data=============")
			val rawdata=List ("State->TamilNadu~City->Chennai",
					"State->Karnataka~City->Bangalore",
					"State->Maharashtra~City->Mumbai")
					
			rawdata.foreach(println)		
			
			println("==================== Raw Data=============")
			val flatdata = rawdata.flatMap( x => x.split("~"))
			flatdata.foreach(println)		
		
			println("==================== Pick State and City=============")
			val statelist = flatdata.filter( x => x.contains("State"))
			statelist.foreach(println)		
			val citylist = flatdata.filter( x => x.contains("City"))
			citylist.foreach(println)		
			
			println("==================== Concat State with ->state =============")
			val finalstate  = statelist.map(x=>x + "->state")
			finalstate.foreach(println)
			
			println("==================== Concat City with ->city =============")
			val finalcity  = citylist.map(x=>x + "->city")
			finalcity.foreach(println)
			
			/*Class Task 2 --- The above created 2 List should be further flattened again with  "->" */
			
			println("==================== Final State List Flat Map=============") 
			val finalstateflatmap =  finalstate.flatMap( x => x.split("->"))
			finalstateflatmap.foreach(println)
			
			println("==================== Final City List Flat Map=============")
			val finalcityflatmap = finalcity.flatMap( x => x.split("->"))
			finalcityflatmap.foreach(println)

	}
}