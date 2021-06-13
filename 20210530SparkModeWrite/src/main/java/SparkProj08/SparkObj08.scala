package SparkProj08

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object ScalaObject {

case class TxnSchema(txn_no:String, txn_date:String, cust_no:String, amount:String, category:String, 
		product:String, city:String, state:String, spendby:String);

def main(args:Array[String]): Unit ={

		val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
		val sc = new SparkContext(conf);
		sc.setLogLevel("ERROR");

		val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns");

		print("== Data Loaded ==");
		println;

		val mapSplit = allData.map(x=>x.split(","));

		val imposeData = mapSplit.map(x=>TxnSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));

		val spark = SparkSession.builder().getOrCreate();
		import spark.implicits._;

		val finDF = imposeData.toDF();
		
		finDF.show();
		
		println;
		print("== Before write mode_test ==");
		println;
		//finDF.write.format("csv").mode("error").save("file:///E:/Hadoop/Hadoop_Data/output/mode_test")
		println;
		print("== After write mode_test ==");
		println;
		
		println;
		print("== Before write mode_test append ==");
		println;
		finDF.write.format("csv").mode("append").save("file:///E:/Hadoop/Hadoop_Data/output/mode_test")
		println;
		print("== After write mode_test append ==");
		println;
		
		
}
}