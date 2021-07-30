package ProjectPhase2Pack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions.explode;
import org.apache.spark.sql.functions.expr;
import org.apache.spark.sql.functions.regexp_replace;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.DataFrameNaFunctions;
import java.time.LocalDate;
import org.apache.spark.sql.hive.HiveContext

object Phase2 {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("Project_1").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().enableHiveSupport()
					.config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate();
			import spark.implicits._;

			val hc = new HiveContext(sc);
			import hc.implicits._;
			
			val fileName = args(0);
			
			val actionDate = LocalDate.now().minusDays(1);
			println("======= Action Date =====");
			println(actionDate);
			println("=== Working file and Directory ===");
			//val fileName = s"file:///E:/Hadoop/Spark_Project_Phase2/${actionDate}/part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro";
			//val fileName = s"file:///home/cloudera/parani/data/${actionDate}/part-00000-1bd5ec9a-4ceb-448c-865f-305f43a0b8a9-c000.avro";
			
			println(fileName);

			println("==== RAW DataFrame 1 =====");
			val df1 = spark.read.format("com.databricks.spark.avro").option("header","true").load(fileName);
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
			//explodedDf1.show();
			//explodedDf1.printSchema();

			println("===== Replace numbers in username ======");
			val explodedDf2 = explodedDf1.withColumn("username", regexp_replace(col("username"),"[0-9]","")); 
			//explodedDf2.show(50);

			println("======= Count of DF2 ======");
			//println(explodedDf2.count());

			println("======= BOTH DATAFAME IS CLEAN ========");

			println("======= LEFT JOINED DF =======");
			val leftJoinDf = df1.join(broadcast(explodedDf2),Seq("username"),"left")
					leftJoinDf.show(false);
			println("======= Left Joined Count ======");
			println(leftJoinDf.count());

			println("========= GENERATED INDEX =============");

			val zippedDf = addColumnIndex(spark, leftJoinDf);
			zippedDf.show();

			println("========= REPLACE ID WITH INDEX COLUMN =============");
			val replacedDf = zippedDf.withColumn("id", col("index")).drop("index"); 
			replacedDf.show();

			println("====== GETTING MAX VALUE IN SPARK HIVE TABLE ======");
			val maxValuedf = hc.sql("select coalesce(max(id), 0) from zeyodb.spark_max_phase2");
			val latestMaxValue = maxValuedf.collect().map(x=>x.mkString("")).mkString("").toInt;
			
			//val latestMaxValue=maxvaldf.rdd.map(x=>x.mkString("")).collect().mkString("").toInt

			println("========= MAX VALUE ADD TO DF's ID column =============");
			val addedDf = replacedDf.withColumn("id", col("id") + lit(latestMaxValue));
			addedDf.show();

			println("========= Write to Hive table target =============");
			addedDf.write.format("hive").mode("append").saveAsTable("zeyodb.spark_max_phase2op")
			
			println("======= PHASE 2 Completed =======");
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