package SparkDFProj01

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._
import org.apache.spark.sql.SparkSession

object SparkObj07 {

case class columns(txno:String, txndate:String, custno:String, amount:String, category:String,product:String, city:String, state:String, spendby:String)

def main(args:Array[String]):Unit={

		val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")

				val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns");

		print("=== Data Loaded ===");
		println;
		
		val spark = SparkSession.builder().getOrCreate()
				import spark.implicits._

				val mapSplit = allData.map(x=>x.split(","));

		val schemardd = mapSplit.map(x=>columns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));

		val df = schemardd.toDF();

		print("==== Showing DF ====")
		println;

		df.show();

		df.createOrReplaceTempView("txndf");

		print("==== Showing Query Output ====")
		println;

		val gymData = spark.sql("select * from txndf where product like '%Gymnastics%' and spendby='cash'");
		println;

		gymData.show();
}
}