package SparkPack01


object SparkObj01 {

	def main(args:Array[String]):Unit={

			val int_list = List(1,2,3,4,5,6,7,8,9,10);

			println("=== Raw Data ==");

			int_list.foreach(println);

			println("=== Filtered Data ==");

			val filtered_list = int_list.filter(x=>x<4);

			filtered_list.foreach(println);

			println("Multiply 4 to each");

			val multbyfour = int_list.map(x=>x*4)

					multbyfour.foreach(println);

			println("=====Raw String Data======")

			val str_list = List("Zeyobron","Zeyo","Bron","HelloZeyo")

			str_list.foreach(println)

			println("=====Filtering String Data======")

			val filteredstring = str_list.filter(x=>x.contains("Zeyo"))

			filteredstring.foreach(println);

			println("==== Map data concat =====")

			val mapstring = str_list.map(x=>"Mr. "+x);

			mapstring.foreach(println);

			println("===== Delimited Raw data ======")

			val delimitedstrings = List ("Ashok, Kumar", "Ganesh, Prasad", "Parani, Rajan")

			delimitedstrings.foreach(println);

			println("===== Flatterend Raw data ======")

			val flatterend = delimitedstrings.flatMap(x=>x.split(","))

			flatterend.foreach(println);

			println("===== Replace data ===")

			val replaced = flatterend.map(x=>x.replace("Parani","Aravind"))

			replaced.foreach(println)

			println("===== Complex Raw data ======")

			val data_str = List (
					"State -> Andrapradesh ~ City -> Vijayawada",
					"State -> Tamilnadu ~ City -> Chennai",
					"State -> Maharastra ~ City -> Mumbai");

			val flattern = data_str.flatMap(x=>x.split("~"))

					println("===== Flatterend data ======")

					flattern.foreach(println)

					val city = flattern.filter(x=>x.contains("City ->"))

					println("===== City data ======")

					city.foreach(println)

					val state = flattern.filter(x=>x.contains("State ->"))

					println("===== City data ======")

					state.foreach(println)
					
					println("===== State names ======")
					
					val statenames = state.map(x=>x.replace("City ->",""))
					
					statenames.foreach(println)
					
					println("===== City names ======")
					
					val citynames = state.map(x=>x.replace("State ->",""))
					
					citynames.foreach(println)
	}
}