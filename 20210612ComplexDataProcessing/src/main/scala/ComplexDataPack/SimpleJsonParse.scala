package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object SimpleJsonParse {
  
  def main(args:Array[String]): Unit ={
    
    val conf = new SparkConf().setAppName("JSON").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    val jsonDf = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/zeyodata.json");
    
    jsonDf.show();
    jsonDf.printSchema();
    
    val flatternDf = jsonDf.select("No","Year","address.permanent_address","address.temporary_address","firstname","lastname");
    
    flatternDf.show();
    flatternDf.printSchema();
  }
}