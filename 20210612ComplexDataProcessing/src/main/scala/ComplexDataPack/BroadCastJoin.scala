package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;


object BroadCastJoin {

	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("JOINS").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();

			import spark.implicits._;

			val peopleDF = Seq(
					("andrea", "medellin"),
					("rodolfo", "medellin"),
					("abdul", "bangalore")
					).toDF("first_name", "city");

			peopleDF.show();

			val citiesDF = Seq(
					("medellin", "colombia", 2.5),
					("bangalore", "india", 12.3)
					).toDF("city", "country", "population")

					citiesDF.show();

			/* The Spark null safe equality operator (<=>) is used to perform this join. */
			
			println;
			println("=================");
			println;
			
			peopleDF.join(
					broadcast(citiesDF),
					peopleDF("city") <=> citiesDF("city")
					).explain();

			println;
			
			peopleDF.join(
					citiesDF,
					peopleDF("city") <=> citiesDF("city")
					).explain();

			println;
			println("=================");
			println;
		
			peopleDF.join(
					broadcast(citiesDF),
					Seq("city")
					).explain();

			println;
			
			peopleDF.join(
					citiesDF,
					Seq("city")
					).explain();
			
	}
}