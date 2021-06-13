package SparkRowRddPack;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.SparkSession;

object ScalaObject {

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

			def main(args:Array[String]): Unit ={

					val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
					val sc = new SparkContext(conf);
					sc.setLogLevel("ERROR");

					val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns");

					print("=== all data loaded === ");
					println;

					val mapSplit = allData.map(x=>x.split(","));

					val rowRdd = mapSplit.map(x=>Row(x(0).toString.toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));

					val spark = SparkSession.builder().getOrCreate();
					import spark.implicits._;

					val rowDf = spark.createDataFrame(rowRdd, schema_struct);

					print("=== Dataframe created ===");
					println;

					rowDf.createOrReplaceTempView("txn_data");

					print("=== Query output ===");
					println;

					val outputData = spark.sql("select * from txn_data where spendby='cash'");

					outputData.show(10);

	}
}