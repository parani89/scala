package SparkTextSTreamPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.streaming._;

object TextStream {

	def main(args:Array[String]) : Unit = {

			val conf = new SparkConf().setAppName("Project_4").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");
			
			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;
			
			val ssc = new StreamingContext(conf, Seconds(2));
			
			val stream = ssc.textFileStream("file:///E:/Hadoop/fold2/NIFI_HTTP_STREAMING/");
			
			stream.print();
			
			ssc.start();
			ssc.awaitTermination();
	}
}