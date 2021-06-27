package SparkAwsProj

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.DataFrameNaFunctions;

object SparkAws {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("first").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")

					val spark = SparkSession.builder()
					.config("fs.s3a.access.key","AKIAQJ35YRX5ELDIRK7H")
					.config("fs.s3a.secret.key","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")
					.getOrCreate()


					import spark.implicits._



					val df=	spark.read.parquet("s3a://zeyonifibucket/cashdata_parquet/part-00000-5e2a219f-21ea-454c-9c18-8859c6df617f-c000.snappy.parquet")

					println("==== RAW DATA ======");
					df.show()

					df.write.format("json").save("s3a://zeyonifibucket/Batch28Writes/Parani1_Dir")

					println("data written")

					val df1=	spark.read.format("json").load("s3a://zeyonifibucket/Batch28Writes/Parani1_Dir/*")

					println("==== WRITTEN DATA ======");
					df.show()
	}
}