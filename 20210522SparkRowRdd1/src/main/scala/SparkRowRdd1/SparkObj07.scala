package SparkRowRdd1

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.Row

object SparkObj06 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns")

					val mapSplit = allData.map(x=>x.split(","));

					val rowRdd = mapSplit.map(x=>Row(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));
					
					val filteredData = rowRdd.filter(x=>x(0).toString.toInt > 3000);
					
					print("===== Final Data ======");
					println;
					
					filteredData.take(10).foreach(println);
	}
}