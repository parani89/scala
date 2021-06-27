package SparkRowRdd2

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

object RowRdd2 {
  
  def main(args:Array[String]): Unit ={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val allData = sc.textFile("file:///E:Hadoop/Hadoop_Data/txns");
    
    println("== Data Loaded ==");
    
    val mapSplit = allData.map(x=>x.split(","));
    
    print("==== Map Splitted Value ===");
    
    mapSplit.take(10).foreach(println);
    
    println("====== Selected Column alone =====");
    val selRdd = mapSplit.map(x=>x(0));
    selRdd.take(10).foreach(println);
    
    val rddValue = mapSplit.map(x=>("Hello ",x(0),1,"Hi"));
    
    println("==== Final Value ===");
    println;
    rddValue.take(10).foreach(println);
    
  }
}