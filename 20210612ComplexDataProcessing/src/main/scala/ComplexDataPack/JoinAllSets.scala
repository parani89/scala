package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;

object JoinAllSets {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("JOINS").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    
    println("====== Reading the data ========");
    
    println("============ DF 1 ============");
    val df1 = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/j3.csv");
    df1.show();
    
    
    println("============ DF 2 ============");
    val df2 = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/j4.csv");
    df2.show();
    
    println("============ Inner Join ============");
    val innerJoin = df1.join(df2, Seq("txnno"), "inner");
    innerJoin.show();
    innerJoin.explain();
    
    println("============ Left Join ============");
    val leftJoin = df1.join(df2, Seq("txnno"), "left");
    leftJoin.show();
    
    println("============ Left Semi Join ============");
    val leftSemiJoin = df1.join(df2, Seq("txnno"), "leftsemi");
    leftSemiJoin.show();
    
    println("============ LeftAnti Join ============");
    val leftAntiJoin = df1.join(df2, Seq("txnno"), "leftanti");
    leftAntiJoin.show();
    
    println("============ Right Join ============");
    val rightJoin = df1.join(df2, Seq("txnno"), "right");
    rightJoin.show();
    
    println("============ Outer Join ============");
    val outerJoin = df1.join(df2, Seq("txnno"), "outer");
    outerJoin.show();
    
    println("=========== CONF =============");
    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold").toInt);
    
  }
}