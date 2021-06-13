package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;

object GetFromRestAPI {

	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("JOINS").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();

			val url = "https://randomuser.me/api/0.8/?results=10";
			val resultJson = scala.io.Source.fromURL(url).mkString

			println(resultJson);

			val rddData = sc.parallelize(List(resultJson));
			val finalDf = spark.read.json(rddData);

			println("====== Dataframe =====");
			finalDf.show(false);

			finalDf.printSchema();
	}
}