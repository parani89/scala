package SparkReadFileFormats

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.SparkSession;

object ReadFormatFiles {

	def main(args:Array[String]): Unit = {

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			val dfcsv = spark.read.option("header","true").format("csv").load("E:/Hadoop/Hadoop_Data/Format_files/usdata.csv");

			print("===== CSV READ =====");
			
			dfcsv.show();
			
			val dfparq = spark.read.format("parquet").load("E:/Hadoop/Hadoop_Data/Format_files/part_par.parquet");
			
			print("===== PARQ READ =====");
			
			dfparq.show();
			
			print("===== JSON READ =====");
			
			val dfjson = spark.read.format("json").load("E:/Hadoop/Hadoop_Data/Format_files/devices.json");
			
			dfjson.show(10, false);
			
			print("===== ORC READ =====");
			
			val dforc = spark.read.format("orc").load("E:/Hadoop/Hadoop_Data/Format_files/part_orc.orc");
			
			dforc.show();
			
			
			print("===== About to write Parquet ======");
			
			dfjson.write.format("parquet").mode("error").save("E:/Hadoop/Hadoop_Data/Output/devices_parquet");
			
			dfjson.write.format("json").mode("error").save("E:/Hadoop/Hadoop_Data/Output/devices_json");
			
			dfjson.write.format("csv").mode("error").save("E:/Hadoop/Hadoop_Data/Output/devices_csv");
			
			dfjson.write.format("orc").mode("error").save("E:/Hadoop/Hadoop_Data/Output/devices_orc");
			
			println;
			
			print("========== Write completed ========");
			
			
			print("===== AVRO Write === Loaded the JAR ==");
			
			dfjson.write.format("com.databricks.spark.avro").mode("error").save("E:/Hadoop/Hadoop_Data/Output/devices_avro");
			
			println;
			
			print("===== AVRO Write completed =====");
			
	}

}