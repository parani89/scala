package HiveZipWithIndexPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql._;
import java.time.LocalDate;

object ZipWithIndex1 {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("Project_1").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;
			
			val txnDf = spark.read.format("csv").load("file:///E:/Hadoop/Hadoop_Data/txns");
			txnDf.show();

			val resultDf = addColumnIndex(spark, txnDf);
			resultDf.show();
			
			println("========= DATE Functions =========");
			val myDate = LocalDate.now();
			println("====== Current Date ========");
			println(myDate);
			println("====== Current Date -1 ========");
			println(myDate.minusDays(1));
			
			val file1Df = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/file1_join.txt");
			val file2Df = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/file2_join.txt");
			
			file1Df.show();
			file2Df.show();
			
			val file2List = file2Df.select("id").map(x=>x.getString(0)).collect().toList;
			//val file2List = file2Df.select("id").collect().toList;
			println("====== LIST ==========");
			println(file2List);
			
			println("=== Minus record =====");
			val finalList = file1Df.filter(!col("id").isin(file2List:_*));
			finalList.show();
			
	}

	def addColumnIndex(spark: SparkSession,df: DataFrame) = {
			spark.sqlContext.createDataFrame(
					df.rdd.zipWithIndex.map {
					case (row, index) => Row.fromSeq(row.toSeq :+ index)
					},
					// Create schema for index column
					StructType(df.schema.fields :+ StructField("index", LongType, false)))
	}
	
}