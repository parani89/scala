package SparkPack04

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkObj04 {

case class columns(category:String,product:String,city:String,state:String,spendby:String) 

def main(args:Array[String]):Unit={

		val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")


				val data = sc.textFile("file:///E:Hadoop/Hadoop_Data/txns");

		println;
		println;

		print(" === Data extracted ========");

		val mapCommaSplit = data.map(x=>x.split(","));

		val imposeRdd = mapCommaSplit.map(x=>columns(x(4),x(5),x(6),x(7),x(8)));

		val finalData = imposeRdd.filter(x=>x.spendby.contains("cash"));

		println;
		println;

		print("=== Final result ==");

		println;
		
		finalData.take(10).foreach(println)
}
}