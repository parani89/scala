package SparkReadFileFormats

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object ReadFileSchema {

	def main(args:Array[String]): Unit = {

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			// option header = true means prints header. If not it's coming as c0,c1_ etc. inferSchema means it give actual schema's data type.
			val dfcsv = spark.read.format("csv").option("header","true").option("inferSchema" , "true").load("file:///E:/Hadoop/Hadoop_Data/Format_files/usdata.csv");

			print("=== csv file read done ===");
			println;

			dfcsv.show(4);
			dfcsv.printSchema();

			print("=== print us_data.csv to AVRO ===");
			println;

			dfcsv.write.format("com.databricks.spark.avro").mode("overwrite").save("file:///E:/Hadoop/Hadoop_Data/Output/us_data_avro");

			print("=== Write completed as AVRO ===");
			println;

			print("=== Read the AVRO File ===");
			println;
			
			val dfavro = spark.read.format("com.databricks.spark.avro").load("file:///E:/Hadoop/Hadoop_Data/Output/us_data_avro/part*.avro")
			
			dfavro.show();
			
			print("=== Write the AVRO to CSV ===");
			println;
			
			dfavro.write.format("csv").mode("overwrite").save("file:///E:/Hadoop/Hadoop_Data/Output/us_data_csv");
			
			print("AVRO to CSV write done");
			
	}
}