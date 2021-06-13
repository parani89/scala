package SparkDFProj02

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object ScalaObject {
  
  case class TxnSchema(txn_no:String, txn_date:String, cust_no:String, amount:String, category:String, 
      product:String, city:String, state:String, spendby:String);
  
  def main(args:Array[String]): Unit ={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val allData = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns");
    
    print("== Data Loaded ==");
    println;
    
    val mapSplit = allData.map(x=>x.split(","));
    
    val imposeData = mapSplit.map(x=>TxnSchema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));
    
    val spark = SparkSession.builder().getOrCreate();
      import spark.implicits._;
      
    val df = imposeData.toDF();
    
    print("=== Showing DF ===");
    println;
    
    df.show(10);
    
    df.createOrReplaceTempView("txn_df");
    
    print("=== DF View created ===");
    println;
    
    print("==== Showing Query Output ====")
		println;

    val cashData = spark.sql("select txn_no,cust_no,category from txn_df where spendby='cash'");
    		
    cashData.show(11);
  }
}