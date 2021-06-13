package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.SparkSession;

object JsonToFlatFlattoJson1 {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("JSON_PARSE").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");


			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			println("==== Simple JSON Read ====");
			val jsonReadDf = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/reqapi.json");
			jsonReadDf.show(false);

			jsonReadDf.printSchema();

			println("==== JSON's Schema ====");
			val jsonSchema = spark.read.option("multiline","true").json("file:///E:/Hadoop/Hadoop_Data/reqapi.json");
			jsonSchema.schema.fields.sortBy(_.name).foreach(println);
			
			println;
			
			println("==== JSON to flat Read ====");
			val jsonFlatData = jsonReadDf.select(
					col("data.avatar"),
					col("data.email"),
					col("data.first_name"),
					col("data.id"),
					col("data.last_name"),
					col("page"),
					col("per_page"),
					col("support.text"),
					col("support.url"),
					col("total"),
					col("total_pages")
					);

			jsonFlatData.show(false);
			jsonFlatData.printSchema();


			println("==== Flat to JSON ====");

			val toJsonDf = jsonFlatData.select(
					struct(
							col("avatar").alias("avatar"),
							col("email").alias("email"),
							col("first_name").alias("first_name"),
							col("id").alias("id"),
							col("last_name").alias("last_name")
							).alias("data"),
					col("page").alias("page"),
					col("per_page").alias("per_page"),
					struct(
							col("text").alias("text"),
							col("url").alias("url")
							).alias("support"),
					col("total").alias("total"),
					col("total_pages").alias("total_pages")
					);
			
			toJsonDf.show(false);
			toJsonDf.printSchema();
			
	}
}