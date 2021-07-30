package ComplexDataPack

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.functions._;

object JsonAPIReadFlattern1 {

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

			
			println("======= RAW DATA =========");
			println;
			//val df = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/multiple_explode_json_array1.json");
			df.show();
			df.printSchema()
			
			println("======= AFTER EXPLODE =========");
			val flatternData2 = df.withColumn("results", explode(col("results")));
			flatternData2.show(false);
			flatternData2.printSchema();

/*			val finalDf = flatternData2.select(
			                                    col("nationality").alias("nationality"),
			                                  //  col("results.user.INSEE").alias("insee"),
			                                    col("results.user.cell").alias("cell"),
			                                    col("results.user.dob").alias("dob"),
			                                    col("results.user.email").alias("email"),
			                                    col("results.user.gender").alias("gender"),
			                                    col("results.user.location.city").alias("city"),
			                                    col("results.user.location.state").alias("state"),
			                                    col("results.user.location.street").alias("street")
			                                   );*/
			                                    
			val finalDf = flatternData2.select(
			                                    col("nationality").alias("nationality"),
			                                    col("results.user.cell").alias("cell"),
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
			                                    col("results.user.phone").alias("phone"),
			                                    col("results.user.picture.large").alias("large_picture"),
			                                    col("results.user.picture.medium").alias("medium_picture"),
			                                    col("results.user.picture.thumbnail").alias("thumbnail_picture"),
			                                    col("results.user.registered").alias("registered"),
			                                    col("results.user.salt").alias("salt"),
			                                    col("results.user.sha1").alias("sha1"),
			                                    col("results.user.sha256").alias("sha256"),
			                                    col("results.user.username").alias("username"),
			                                    col("seed").alias("seed"),
			                                    col("version").alias("version")
					                              );

			println("======= FINAL FLAT =======");
			finalDf.show();
			finalDf.printSchema();
			println("======== Count is ======== "+finalDf.count());
			
			println("=========== REVERT ==========");
			
			val revStage1Df = finalDf.select(
			                                  col("nationality").alias("nationality"),
			                                  
			                                      struct(
			                                          col("cell").alias("cell"),
			                                          col("dob").alias("dob"),
			                                          col("email").alias("email"),
			                                          col("gender").alias("gender"),
			                                          struct(
			                                                  col("city").alias("city"),
			                                                  col("state").alias("state"),
			                                                  col("street").alias("street"),
			                                                  col("zip").alias("zip")
			                                          ).alias("location"),
			                                          col("md5").alias("md5"),
			                                          struct(
			                                                  col("firstname").alias("first"),
			                                                  col("lastname").alias("last"),
			                                                  col("title").alias("title")
			                                          ).alias("name"),
			                                          col("password").alias("password"),
			                                          col("phone").alias("phone"),
			                                          struct(
			                                                  col("large_picture").alias("large"),
			                                                  col("medium_picture").alias("medium"),
			                                                  col("thumbnail_picture").alias("thumbnail")
			                                          ).alias("picture"),
			                                          col("registered").alias("registered"),
			                                          col("salt").alias("salt"),
			                                          col("sha1").alias("sha1"),
			                                          col("sha256").alias("sha256"),
			                                          col("username").alias("username")
			                                      ).alias("user"),
			                                  col("seed").alias("seed"),
			                                  col("version").alias("version")
			                                );

			revStage1Df.show();
			revStage1Df.printSchema();
			
			println("============ FINAL COMPLEX DATA 1 =============");
			
			val revStage2Df = revStage1Df.groupBy(col("nationality"),col("seed"),col("version")).agg(collect_list(struct(col("user"))).alias("results"));

			revStage2Df.show();
			revStage2Df.printSchema();
			
			println("============ FINAL COMPLEX DATA 2 =============");
			
			val revStage3Df = finalDf.groupBy(col("nationality"),col("seed"),col("version"))
			                        .agg(collect_list(
			                            struct(
			                                          col("cell").alias("cell"),
			                                          col("dob").alias("dob"),
			                                          col("email").alias("email"),
			                                          col("gender").alias("gender"),
			                                          struct(
			                                                  col("city").alias("city"),
			                                                  col("state").alias("state"),
			                                                  col("street").alias("street"),
			                                                  col("zip").alias("zip")
			                                          ).alias("location"),
			                                          col("md5").alias("md5"),
			                                          struct(
			                                                  col("firstname").alias("first"),
			                                                  col("lastname").alias("last"),
			                                                  col("title").alias("title")
			                                          ).alias("name"),
			                                          col("password").alias("password"),
			                                          col("phone").alias("phone"),
			                                          struct(
			                                                  col("large_picture").alias("large"),
			                                                  col("medium_picture").alias("medium"),
			                                                  col("thumbnail_picture").alias("thumbnail")
			                                          ).alias("picture"),
			                                          col("registered").alias("registered"),
			                                          col("salt").alias("salt"),
			                                          col("sha1").alias("sha1"),
			                                          col("sha256").alias("sha256"),
			                                          col("username").alias("username")
			                                      ).alias("user")

			                                    ).alias("results"));
			
			revStage3Df.show();
			revStage3Df.printSchema();
			
	}
}