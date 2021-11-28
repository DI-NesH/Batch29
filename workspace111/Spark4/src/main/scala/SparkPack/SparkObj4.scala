package SparkPack



object SparkObj4 {

	def main(args:Array[String]):Unit={
			/*
			val data=List(1,2,3,4,6)

					data.foreach(println)

			 */
			/*
			println("============RawData================")

			val data= List(10,20,30,40) 

			data.foreach(println)

			println("=============MUl Data==============")

			val mulData=  data.map( x=>x*10)

			mulData.foreach(println)

			println("=============Div Data==============")

			val divData=  mulData.map( x=>x/5)

			divData.foreach(println)

			println("=============Fil Data==============")

			val filData=  divData.filter(x=>x > 50)

			filData.foreach(println)
			 */
			/*
						println("=============Raw Data==============")

						val rawdata=List("zeyobron","analytics","zeyo")
						rawdata.foreach(println)

						println("=============Filter Data==============")

						val fildata  = rawdata.filter(x=>x.contains("zeyo"))
						fildata.foreach(println)

						println("=============Map Data==============") 
						val mapdata  = fildata.map(x=>x + ", ShriDinesh")
						mapdata.foreach(println)

						println("=============Map Data1==============")
						val mapdata1  = mapdata.map(x => x.replace("ShriDinesh","Ughade"))
						mapdata1.foreach(println)
			 */
			/*
						println("=============Raw Data==============")

						val rawdata=List("zeyobron~analytics~zeyo","sai~aditya")

						rawdata.foreach(println)


						println("====================flatten data=============")


						val flatdata = rawdata.flatMap( x =>  x.split("~"))

						flatdata.foreach(println)

			 */
			println("==================== Raw Data=============")
			val rawdata=List (
					"State->TamilNadu~City->Chennai",
					"State->Karnataka~City->Bangalore",
					"State->Maharashtra~City->Mumbai"
					)

			rawdata.foreach(println)

			println("==================== Raw Data=============")
			val flatdata = rawdata.flatMap(x =>x.split("~"))
			flatdata.foreach(println)

			println("==================== Pick State and City=============")
			val statelist = flatdata.filter( x => x.contains("State"))
			val citylist = flatdata.filter( x => x.contains("City"))

			println("==================== State list=============")
			statelist.foreach(println)
			println("==================== City list=============")
			citylist.foreach(println)

			println("==================== Replace state and city=============")
			val finalstate  = statelist.map(x => x.replace("State->",""))
			val citystate  = citylist.map(x => x.replace("City->",""))

			println("==================== State final list=============")
			finalstate.foreach(println)

			println("==================== City final list=============")
			citystate.foreach(println)

			println("==================== final Merge State and City =============")
			val finalstatecity = finalstate.union(citystate)

			finalstatecity.foreach(println)

	}
}