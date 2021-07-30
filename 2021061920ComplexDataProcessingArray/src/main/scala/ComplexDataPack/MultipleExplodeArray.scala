package ComplexDataPack

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.types._;
import org.apache.spark.sql._;

object MultipleExplodeArray {

	def main(args:Array[String]): Unit ={


			val conf = new SparkConf().setAppName("Complex_Process").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			println("========== Raw Data =========");
			val allData = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/multiple_explode_json_array.json");
			allData.show(false);
			allData.printSchema();

			println("============ Stage 1 ============= ");
			val stage2Df = allData.withColumn("Students", explode(col("Students")))
					stage2Df.show(false);
			stage2Df.printSchema();

			println("============ Stage 2 ============= ");
			//val stage3Df = stage2Df.withColumn("Students.user.components", explode(col("Students.user.components"))); Invalid
			val stage3Df = stage2Df.withColumn("components",explode(col("Students.user.components").alias("Component")));
			stage3Df.show(false);
			stage3Df.printSchema();

			println("============ Stage 3 ============= ");
			val stage4Df = stage3Df.select(
					col("first_name").alias("o_firstname"),
					col("second_name").alias("o_secondname"),
					col("address.Permanent_address").alias("permanent_addr"),
					col("address.temporary_address").alias("temporary_addr"),
					col("Students.user.address.Permanent_address").alias("student_permanent_addr"),
					col("Students.user.address.temporary_address").alias("student_temporary_address"),
					col("Students.user.gender").alias("student_gender"),
					col("Students.user.name.first").alias("student_f_name"),
					col("Students.user.name.last").alias("student_l_name"),
					col("Students.user.name.title").alias("student_l_title"),
					col("components").alias("components")
					);
			stage4Df.show(false);
			stage4Df.printSchema();

			println("====== Revert back to original =======");
			
			val revStage1Df = stage4Df.select(
					col("o_firstname").alias("first_name"),
					col("o_secondname").alias("second_name"),
					struct(
							col("student_permanent_addr"),
							col("student_temporary_address")
							).alias("address"),
					struct(
							col("student_gender").alias("gender"),
							struct(
									col("student_f_name").alias("first"),
									col("student_l_name").alias("last"),
									col("student_l_title").alias("title")
									).alias("name"),
							struct(
									col("student_permanent_addr").alias("Permanent_address"),
									col("student_temporary_address").alias("temporary_address")
									).alias("address")
							).alias("user"),
					struct(
							col("components")
							).alias("components")
					);

			revStage1Df.show(false);
			revStage1Df.printSchema();


/*			val revStage2Df = stage4Df.groupBy.("student_permanent_addr","student_temporary_address","student_gender","student_f_name","student_l_name","student_l_title")
					.agg(collect_list(struct(col("components"))).alias("components"));

		
*/	}

}