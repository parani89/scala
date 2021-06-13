package RdbmsReadPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;

import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import java.io.File
import org.apache.spark.sql.functions._

object RdbmsRead {

	def main(args:Array[String]): Unit ={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			//val spark = SparkSession.builder().master("local[*]").getOrCreate();
			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			val rdbmsdata=spark.read.format("jdbc")
					.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/zeyodb")
					.option("driver","com.mysql.jdbc.Driver")
					.option("dbtable","web_customer")
					.option("user","root")
					.option("password","Aditya908")
					.load();

			println
			println
			println("-----------------MYSQL DATA READ FROM AWS-----------------")
			rdbmsdata.show()
			rdbmsdata.printSchema()
			println("-----------------------------------------")
			println("---------- DATA WRITE TO AWS -----------");

			val writeDf = spark.read.load("file:///E:/Hadoop/Hadoop_Data/parquet_file_from_aws*");

			writeDf.show();

			println("============= Before write to AWS ===========");

			writeDf.write.format("jdbc")
			.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
			.option("driver","com.mysql.jdbc.Driver")
			.option("dbtable","parani_tab")
			.option("user","root")
			.option("password","Aditya908")
			.save();

			println("============= After write to AWS ===========");

			println("============= Before second Read from AWS ===========");

			val secondRead=spark.read.format("jdbc")
					.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
					.option("driver","com.mysql.jdbc.Driver")
					.option("dbtable","parani_tab")
					.option("user","root")
					.option("password","Aditya908")
					.load();

			println("============= After second Read from AWS ===========");

			secondRead.show();

	}
}