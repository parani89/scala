package ComplexDataPack

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions._;

object JsonAPIReadFlattern {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("Complex_Process").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			val url = "https://randomuser.me/api/0.8/?results=50"
					val result = scala.io.Source.fromURL(url).mkString
					val rdd1 = sc.parallelize(List(result))
					val df = spark.read.json(rdd1)

					df.show(false);
			df.printSchema()


			//val df = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/json_50.json");

			df.show();
			df.printSchema();

			val flatternData2 = df.withColumn("results", explode(col("results")));
			flatternData2.show(false);
			flatternData2.printSchema();

			val finalDf = flatternData2.select(
					col("nationality"),
					col("seed"),
					col("version"),
					col("results.user.cell").alias("cell_number"),
					col("results.user.dob").alias("dob"),
					col("results.user.email").alias("email"),
					col("results.user.gender").alias("gender"),
					col("results.user.location.city").alias("city"),
					col("results.user.location.state").alias("state"),
					col("results.user.location.street").alias("street"),
					col("results.user.location.zip").alias("zip"),
					col("results.user.md5").alias("md5"),
					col("results.user.name.first").alias("firstname"),
					col("results.user.name.last").alias("lastname"),
					col("results.user.name.title").alias("title"),
					col("results.user.password").alias("password"),
					col("results.user.picture.large").alias("picture_large"),
					col("results.user.picture.medium").alias("picture_medium"),
					col("results.user.registered").alias("registered"),
					col("results.user.salt").alias("salt"),
					col("results.user.sha1").alias("sha1"),
					col("results.user.sha256").alias("sha256"),
					col("results.user.username").alias("username")
					);

			finalDf.show(false);
			finalDf.printSchema();
			println("======== Count is ======== "+finalDf.count());

	}
}