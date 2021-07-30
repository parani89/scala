package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.functions.to_json;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions.struct;
import org.apache.spark.sql.functions.explode;
import org.apache.spark.sql.functions.collect_list;

object ArrayJson {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("COMPLEX").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			println("========= Regular JSON read ===========");

			val regularDF = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/array.json");
			regularDF.show();
			regularDF.printSchema();

			// With column's second column requires, the column to be present in select
			/*			val flatternArray = regularDF.select(
					"first_name",
					"second_name",
					"Students",
					"address.temporary_address",
					"address.Permanent_address"
					).withColumn("Students1", explode(col("Students")));*/

			/*			println("========= Array to Flattern =========");
			val flatternArray = regularDF.select(
					"first_name",
					"second_name",
					"Students",
					"address.temporary_address",
					"address.Permanent_address"
					).withColumn("Students", explode(col("Students")));*/


			//  We can use col or simple column name.  
			println("========= Array to Flattern =========");
			val flatternArray = regularDF.select(
					col("first_name"),
					col("second_name"),
					col("Students"),
					col("address.temporary_address"),
					col("address.Permanent_address")
					).withColumn("Students", explode(col("Students")));

			flatternArray.show();
			flatternArray.printSchema();

			println("========= Flattern to Complex again =========");

			val complexData = flatternArray.groupBy("first_name","second_name","temporary_address","Permanent_address")
			          .agg(collect_list("Students").alias("Students"));

			complexData.show();
			complexData.printSchema();

			val finalComplexDf = complexData.select(
					col("Students"),
					col("first_name"),
					col("second_name"),
					struct(
							col("temporary_address"),
							col("Permanent_address")
							).alias("address")
					);

			finalComplexDf.show();
			finalComplexDf.printSchema();
	}

}