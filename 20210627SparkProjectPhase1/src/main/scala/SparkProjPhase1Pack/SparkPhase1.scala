package SparkProjPhase1Pack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions.explode;
import org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.functions.regexp_replace;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.DataFrameNaFunctions;

object SparkPhase1 {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("Project_1").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val srcDataFile1 = args(0);
			val matchedDataFile = args(1);
			val nonMatchedDataFile = args(2);
			
			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			println("==== RAW DataFrame 1 =====");
			//val df1 = spark.read.format("com.databricks.spark.avro").option("header","true").load("file:///E:/Hadoop/Spark_Project_Phase1/part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro");
			val df1 = spark.read.format("com.databricks.spark.avro").option("header","true").load(srcDataFile1);
			df1.show(4);

			println("======= Count of DF1 ======");
			println(df1.count());
			
			val url = "https://randomuser.me/api/0.8/?results=50"
			val result = scala.io.Source.fromURL(url).mkString
			val rdd1 = sc.parallelize(List(result))
			val df2 = spark.read.json(rdd1)
			
			println("==== RAW DataFrame 2 =====");
			df2.show();
			df2.printSchema();
			
			val explodedDf1 = df2.withColumn("results", explode(col("results"))).select(
			                                                                            col("nationality"),
			                                                                            col("results.user.cell").alias("cell"),
			                                                                            col("results.user.dob").alias("dob"),
			                                                                            col("results.user.email").alias("email"),
			                                                                            col("results.user.gender").alias("gender"),
			                                                                            col("results.user.location.city").alias("city"),
			                                                                            col("results.user.location.state").alias("state"),
			                                                                            col("results.user.location.street").alias("street"),
			                                                                            col("results.user.location.zip").alias("zip"),
			                                                                            col("results.user.md5").alias("md5"),
			                                                                            col("results.user.name.first").alias("first"),
			                                                                            col("results.user.name.last").alias("last"),
			                                                                            col("results.user.name.title").alias("title"),
			                                                                            col("results.user.password").alias("password"),
			                                                                            col("results.user.phone").alias("phone"),
			                                                                            col("results.user.picture.large").alias("large_picture"),
			                                                                            col("results.user.picture.medium").alias("medium_picture"),
			                                                                            col("results.user.picture.thumbnail").alias("thumbnail_picture"),
			                                                                            col("results.user.salt").alias("salt"),
			                                                                            col("results.user.sha1").alias("sha1"),
			                                                                            col("results.user.sha256").alias("sha256"),
			                                                                            col("results.user.username").alias("username"),
			                                                                            col("seed"),
			                                                                            col("version")
			                                                                           );
			
			println("===== Exploded DF =====");
			explodedDf1.show();
			explodedDf1.printSchema();
			
			println("===== Replace numbers in username ======");
			val explodedDf2 = explodedDf1.withColumn("username", regexp_replace(col("username"),"[0-9]","")); 
			explodedDf2.show(50);
			
			println("======= Count of DF2 ======");
			println(explodedDf2.count());
			
			println("======= BOTH DATAFAME IS CLEAN ========");
			
			println("======= LEFT JOINED DF =======");
			val leftJoinDf = df1.join(broadcast(explodedDf2),Seq("username"),"left")
			leftJoinDf.show(false);
			println("======= Left Joined Count ======");
			println(leftJoinDf.count());
			
			val matchedDf = leftJoinDf.filter(col("nationality").isNotNull).withColumn("current_date", current_date());
			val nonMatchedDf = leftJoinDf.filter(col("nationality").isNull).withColumn("current_date", current_date());
			
			println("======= Available DF  ======");
			matchedDf.show();
			println(matchedDf.count());
			
			println("======= Not Available DF ======");
			nonMatchedDf.show();
			println(nonMatchedDf.count());
			
			println("======= Not Available Handling NULLS ======");
			//val nullHandledDf = nonMatchedDf.na.fill("N/A",Seq("nationality", "email", "gender", "city", "state", "street", "first", "last", "title", "password", "large_picture", "medium_picture", "thumbnail_picture", "salt", "sha1", "sha256", "username", "seed", "md5")).na.fill(0);
			val nullNonMatchDf = nonMatchedDf.na.fill("N/A").na.fill(0);
			nullNonMatchDf.show();
			
			println("====== Final Aggregations ComplexData for Matched ========");
			val matchedComplexDf = matchedDf.groupBy("username").agg(
			                                                          collect_list("ip").alias("IP"),
			                                                          collect_list("id").alias("ID"),
			                                                          sum("amount").alias("TOTAL_AMOUNT"),
			                                                          struct(
			                                                                count("id").alias("id_count"),
			                                                                count("ip").alias("ip_count")
			                                                          ).alias("count")
			                                                        );
			matchedComplexDf.show();
			matchedComplexDf.printSchema();
			println(matchedComplexDf.count());
			
			println("====== Final Aggregations ComplexData for NotMatched ========");
			val nonMatchedComplexDf = nonMatchedDf.groupBy("username").agg(
			                                                          collect_list("ip").alias("IP"),
			                                                          collect_list("id").alias("ID"),
			                                                          sum("amount").alias("TOTAL_AMOUNT"),
			                                                          struct(
			                                                                count("id").alias("id_count"),
			                                                                count("ip").alias("ip_count")
			                                                          ).alias("count")
			                                                        );
			nonMatchedComplexDf.show();
			nonMatchedComplexDf.printSchema();
			println(nonMatchedComplexDf.count());
			
			
			println("====== Before Write to files ========");
			
			//matchedComplexDf.write.format("json").mode("ERROR").save("file:///E:/Hadoop/Spark_Project_Phase1/output/matched_data");
			//nonMatchedComplexDf.write.format("json").mode("ERROR").save("file:///E:/Hadoop/Spark_Project_Phase1/output/non_matched_data");
			
			matchedComplexDf.write.format("json").mode("ERROR").save(matchedDataFile);
			nonMatchedComplexDf.write.format("json").mode("ERROR").save(nonMatchedDataFile);
			
			println("====== After Write to files ========");
			
	}
}