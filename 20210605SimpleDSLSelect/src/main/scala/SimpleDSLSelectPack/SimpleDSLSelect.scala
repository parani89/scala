package SimpleDSLSelectPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._

object SimpleDSLSelect {

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

					val conf = new SparkConf().setAppName("DSL").setMaster("local[*]");
					val sc = new SparkContext(conf);
					sc.setLogLevel("ERROR");


					val spark = SparkSession.builder().getOrCreate();

					//val txnDf = spark.read.format("csv").option("inferSchema" , "true").load("file:///E:/Hadoop/Hadoop_Data/txns");
					val txnDf = spark.read.schema(schema_struct).format("csv").load("file:///E:/Hadoop/Hadoop_Data/txns");

					txnDf.show();
					txnDf.printSchema();

					println("===== Select columns =====");

					//val selRow = txnDf.select("_c0", "_c1", "_c2");
					//val selRow = txnDf.select("txnno", "txndate", "custno","category");
					val selRow = txnDf.select(col("txnno"), col("txndate"), col("custno"),col("category"));
					
					selRow.show();
					
					println("======= Before write filtered data to File =======");
					
					val filterGym = selRow.filter(col("category") === "Gymnastics");
					
					filterGym.write.format("parquet").mode("ignore").save("file:///E:/Hadoop/Hadoop_Data/output/txn_sel_dsl");
					
					println("======= After write filtered data to File =======");
					
					filterGym.show();
					
					println("=========== Single command write ===========");
					val finalDf = txnDf.filter(col("category") === "Gymnastics").select(col("txnno"), col("txndate"), col("custno"), col("category"));
					finalDf.show();
	}
}