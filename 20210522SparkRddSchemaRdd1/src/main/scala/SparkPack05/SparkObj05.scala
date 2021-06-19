package SparkPack05

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkObj05 {

  case class txn_columns(txn_no:String, txn_date:String, cust_no:String, amount:String, category:String, 
      product:String, city:String, state:String, spendby:String)
  
	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")
					
					val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns");
			
			print("== All Data loaded to RDD == ");
			
			allData.take(10).foreach(println);
			
			println;
			val mapSplit = allData.map(x=>x.split(","));
			
			print("== After Map Split == ");
			println;
			
			mapSplit.take(10).foreach(println)
			
			println;
			print("== Impose Schema to mapSplit == ");
			println;
			
			val imposeData = mapSplit.map(x=>txn_columns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));
			
			print("== Final data after impose == ");
			println;
			imposeData.take(10).foreach(println);
			
			print("== Apply the first filter == ");
			println;
			
			val firstFilter = imposeData.filter(x=>x.product.contains("Gymnastics"));
			firstFilter.take(10).foreach(println);
			
      print("== Apply the second filter == ");
			println;
			
			val secondFilter = firstFilter.filter(x=>x.spendby.contains("cash"));
			secondFilter.take(10).foreach(println)
			
			val finalFilter = imposeData.filter(x=>x.product.contains("Gymnastics") & x.spendby.equals("cash"));
			
			print("== Print Gymnastics only data == ");
			println;
			
			val testData = imposeData.filter(x=>x.product.contains("Gymnastics"));
			
			testData.take(10).foreach(println);
			
			print("=== Single Line Filtered Data ===");
			println;
			
			finalFilter.take(10).foreach(println);
			
	}
}