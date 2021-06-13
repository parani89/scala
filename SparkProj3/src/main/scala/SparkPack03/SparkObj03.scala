package SparkPack03

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkObj03 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val data = sc.textFile("file:///E:Hadoop/Hadoop_Data/usdata.csv");

			println;
			
			print(" === Data extracted ========");

			val laStates = data.filter(x=>x.contains(",LA"));

			print(" === LA states ========");

			laStates.take(10).foreach(println);

			println;
			
			val repRow = laStates.map(x=>x.replace(",","~"));

			print(" === Replace , with ~ ========");

			repRow.take(10).foreach(println);

			println;
			
			val flatRow = repRow.flatMap(x=>x.split("~"));

			print(" === Flatted data ========");

			flatRow.take(10).foreach(println);

			println;
			
			val addZeyo = flatRow.map(x=>"zeyo,"+x+",zeyo");

			print(" === Added Zeyo before and after ========");

			addZeyo.take(10).foreach(println);

			println;
			
			addZeyo.coalesce(1).saveAsTextFile("file:///E:Hadoop/Hadoop_Data/output/us_data2");

			print(" === Saved to File ========");

	}
}