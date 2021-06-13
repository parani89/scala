package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;

object JoinsAll {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("JOINS").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    
    println("====== Reading the data ========");
    
    println("============ DF 1 ============");
    val df1 = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/j1.csv");
    df1.show();
    
    println("============ DF 2 ============");
    val df2 = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/j2.csv");
    df2.show();
    
    println("============ Inner Join ============");
    val innerJoin = df1.join(df2, df1("txnno") === df2("txn_number"), "inner").drop("txn_number");
    innerJoin.show();
    
    println("============ Left Join ============");
    val leftJoin = df1.join(df2, df1("txnno") === df2("txn_number"), "left").drop("txn_number");
    leftJoin.show();
    
    println("============ Right Join ============");
    val rightJoin = df1.join(df2, df1("txnno") === df2("txn_number"), "right").drop("txnno");
    rightJoin.show();
    
    println("============ Outer Join ============");
    val outerJoin = df1.join(df2, df1("txnno") === df2("txn_number"), "outer");
    outerJoin.show();
  }
}