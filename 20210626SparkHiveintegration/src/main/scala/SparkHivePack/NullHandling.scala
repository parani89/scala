package SparkHivePack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.DataFrameNaFunctions;

object NullHandling {
  
  	val schema_struct = StructType(Array(
			StructField("txnno",IntegerType,true),
			StructField("txndate",StringType,true),
			StructField("custno",StringType,true),
			StructField("amount", StringType, true),
			StructField("category", StringType, true),
			StructField("product", StringType, true),
			StructField("city", StringType, true),
			StructField("state", StringType, true),
			StructField("spendby", StringType, true)
			))
			
  def main(args:Array[String]): Unit ={
    
    
    val conf = new SparkConf().setAppName("HIVE_TEST").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    println("=== RAW Data ===");
    val nullData = spark.read.format("csv").option("header", "true").load("file:///E:/Hadoop/Hadoop_Data/nulldata.csv");
    nullData.show();
    
    println("=== NULL Handled Data 1 ===");
    val nullHandled = nullData.withColumn("id", expr("nvl(id,0)")).withColumn("amount", expr("nvl(amount,0)")).withColumn("place", expr("nvl(place,'N/A')"));
    nullHandled.show();
    
    println("=== NULL Handled Data 2 ===");
    val nullHandled2 = nullData.na.fill("0",Seq("id")).na.fill("NA",Seq("name")).na.fill("0.0",Seq("amount")).na.fill("NA",Seq("place"));
    nullHandled2.show();
    
    println("=== NULL Handled Data 3 ===");
    val nullHandled3 = nullData.na.fill("0",Seq("id","amount")).na.fill("NA",Seq("name","place"));
    nullHandled3.show();
    
    println("=== Add currenttime and timestamp ===");
    val appendedDf = nullHandled.withColumn("current_date", current_date()).withColumn("current_time", current_timestamp());
    appendedDf.show(false);
    
    val txnDf = spark.read.format("csv").schema(schema_struct).load("file:///E:/Hadoop/Hadoop_Data/txns");
    
    println("===== TEMP VIEW =====");
    txnDf.createOrReplaceTempView("txn_df");
    val sqlOp = spark.sql("select * from txn_df");
    sqlOp.show();
    
    val maxValueDf = spark.sql("select max(txnno) max_txn from txn_df");

    println("=== Max value 1===");
    var max_value = maxValueDf.collectAsList().get(0).toString()
    println("Max value is "+max_value);

    println("=== Max value 2===");
    var max_value2 = maxValueDf.collect().map(x=>x.mkString("")).mkString("").toInt;
    println("Max value is "+max_value2);
    
    println("=== Max value 3===");
    var max_value3 = maxValueDf.collect().map(x=>x.mkString("")).mkString("").toInt;
    println("Max value is "+max_value3);
    
    
    
  }
}