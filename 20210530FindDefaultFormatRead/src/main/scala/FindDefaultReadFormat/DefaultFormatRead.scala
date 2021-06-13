package FindDefaultReadFormat

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;


object DefaultFormatRead {

	def main(args:Array[String]): Unit = {

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			//val spark = SparkSession.builder().master("local[*]").getOrCreate();
			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;


			println("Before csv read ");
			//spark.read.format("csv").load("file:///E:/Hadoop/Hadoop_Data/usdata.csv").show();
			println("After csv read ");

			println("Before avro read ");
			//spark.read.format("com.databricks.spark.avro").load("file:///E:/Hadoop/Hadoop_Data/avro_file.avro").show();
			//spark.read.load("file:///E:/Hadoop/Hadoop_Data/avro_file.avro").show();
			println("After avro read ");
			
			println("Before json read ");
			//spark.read.format("json").load("file:///E:/Hadoop/Hadoop_Data/Format_files/devices.json").show();
			//spark.read.load("file:///E:/Hadoop/Hadoop_Data/Format_files/devices.json").show();
			println("After json read ");

			println("Before orc read ");
			//spark.read.format("orc").load("file:///E:/Hadoop/Hadoop_Data/Format_files/part_orc.orc").show();
			//spark.read.load("file:///E:/Hadoop/Hadoop_Data/Format_files/part_orc.orc").show();
			println("After orc read ");
			
			println("Before parquet read ");
			//spark.read.format("parquet").load("file:///E:/Hadoop/Hadoop_Data/Format_files/part_par.parquet").show();
			spark.read.load("file:///E:/Hadoop/Hadoop_Data/Format_files/part_par.parquet").show();
			println("After parquet read ");
			
	}

}