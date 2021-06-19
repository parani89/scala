package RddtodfndftorddPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._;

object Rddtodfndftordd {

	val schema_struct = StructType(Array(
			StructField("txnno",IntegerType,true),
			StructField("txndate",StringType,true),
			StructField("custno",StringType,true),
			StructField("amount", StringType, true),
			StructField("category", StringType, true),
			StructField("product", StringType, true),
			StructField("city", StringType, true),
			StructField("state", StringType, true),
			StructField("spendby", StringType, true)
			))

case class txn_columns(txn_no:String, txn_date:String, cust_no:String, amount:String, category:String, 
		product:String, city:String, state:String, spendby:String);

def main(args:Array[String]):Unit ={

		val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
		val sc = new SparkContext(conf);
		sc.setLogLevel("ERROR");

		val spark = SparkSession.builder().getOrCreate();
		import spark.implicits._;

		val allDataRdd = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns");

		print("=== Data Loaded ===");
		println;

		allDataRdd.take(10).foreach(println);

		val mapSplit = allDataRdd.map(x=>x.split(","));

		print("=== Map Split ===");
		println;

		mapSplit.take(10).foreach(println);

		val rowRdd = mapSplit.map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));

		val rowDF = spark.createDataFrame(rowRdd, schema_struct);

		rowDF.show();

		val mapStr = rowDF.rdd.map(x=>x.mkString(","));

		println;
		print("=====Printing RDD from Row DF converted DF======");
		println;
		mapStr.take(10).foreach(println);

		//========== RDD -> DF -> RDD [ rowrdd method ] 

		//========== RDD -> DF -> RDD [ Schema RDD  method ] 

		print("=== RDD -> DF -> RDD [ Schema RDD  method ] ");
		println;

		val schemaRdd = mapSplit.map(x=>txn_columns(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));
		
		val schemaDF = schemaRdd.toDF();
		
		schemaDF.show();
		
		println;
		println("=== Converting DF to RDD ====");
		
		val rdd2 = schemaDF.rdd.map(x=>x.mkString(","));
		
		println;
		print("=====Printing RDD from Schema DF converted DF======");
		println;
		rdd2.take(10).foreach(println);
		
		println("========== RDD -> ROW RDD ===========");
		val rdd3 = schemaDF.rdd;
		

	}
}