package XmlReadPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object XmlRead {

  def main(args:Array[String]): Unit ={
    
    val spark = SparkSession.builder().master("local[*]").getOrCreate();
    import spark.implicits._;
    
    val noteXmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","note").load("file:///E:/Hadoop/Hadoop_Data/note.xml");
    
    noteXmldf.show();
    noteXmldf.printSchema();
    
    val bookXmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","book").load("file:///E:/Hadoop/Hadoop_Data/book.xml");
    
    bookXmldf.show();
    bookXmldf.printSchema();
    
    val txnsXmldf = spark.read.format("com.databricks.spark.xml").option("rowTag","POSLog").load("file:///E:/Hadoop/Hadoop_Data/transactions.xml");
    
    txnsXmldf.show(false);
    txnsXmldf.printSchema();
    
  }
}