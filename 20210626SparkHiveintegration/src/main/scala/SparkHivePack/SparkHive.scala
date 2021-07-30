package SparkHivePack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.DataFrameNaFunctions;
import org.apache.spark.sql.hive.HiveContext

object SparkHive {
  		
  def main(args:Array[String]): Unit ={
    
    
    val conf = new SparkConf().setAppName("HIVE_TEST").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    val hc = new HiveContext(sc);
    import hc.implicits._;
    
    println("==== Print output ====");
    spark.sql("select * from zeyodb.txn_records").show()
    
  }
}