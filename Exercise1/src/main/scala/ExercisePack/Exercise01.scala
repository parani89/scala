package ExercisePack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object Exercise01 {
  
  def main(args:Array[String]):Unit ={
    
    val conf = new SparkConf().setAppName("TEST").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    // RDD operation Spark context
    
    val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/usdata.csv");
    //val allData = sc.textFile("hdfs://E:/Hadoop/Hadoop_Data/usdata.csv");
    
    allData.take(10).foreach(println)
    
    
  }
  
}