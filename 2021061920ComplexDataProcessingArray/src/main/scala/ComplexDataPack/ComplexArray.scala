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

object ComplexArray {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("COMPLEX").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			println("========= Regular JSON read ===========");

			val regularDF = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/complex_array.json");
			regularDF.show(false);
			regularDF.printSchema();

			val stage1Df = regularDF.select(
			                                  col("Students"),
			                                  col("address.Permanent_address").alias("company_permanent_address"),
			                                  col("address.temporary_address").alias("company_temporary_address"),
			                                  col("first_name").alias("company_first_name"),
			                                  col("second_name").alias("company_last_name")
			                               );
			
			stage1Df.show(false);
			stage1Df.printSchema();
			
			val stage2Df = stage1Df.withColumn("Students", explode(col("Students")));
			stage2Df.show(false);
			stage2Df.printSchema();
			
			val finalDf = stage2Df.select(
			                              col("Students.user.address.Permanent_address").alias("student_permanent_addr"),
			                              col("Students.user.address.temporary_address").alias("student_temporary_address"),
			                              col("Students.user.gender").alias("student_gender"),
			                              col("Students.user.name.first").alias("student_name_first"),
			                              col("Students.user.name.last").alias("student_name_last"),
			                              col("Students.user.name.title").alias("student_name_title"),
			                              col("company_permanent_address"),
			                              col("company_temporary_address"),
			                              col("company_first_name"),
			                              col("company_last_name")
			                             );
			
			finalDf.show(false);
			finalDf.printSchema();
	}

}