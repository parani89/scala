package ReadWithSchema

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object SchemaReadCsv {
  
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
    
  	  val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
  	  val sc = new SparkContext(conf);
  	  sc.setLogLevel("ERROR");
  	  
    //val spark = SparkSession.builder().master("local[*]").getOrCreate();
  	  val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    val df = spark.read.schema(schema_struct).format("csv").load("file:///E:/Hadoop/Hadoop_Data/txns");
    //val df = spark.read.format("csv").load("file:///E:/Hadoop/Hadoop_Data/txns");
    
    df.show();
    
    println("==== Shown the data ====");
  }
}