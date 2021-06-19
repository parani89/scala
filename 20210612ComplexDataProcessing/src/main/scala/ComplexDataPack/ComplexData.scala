package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.functions.to_json;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions.struct;

object ComplexData {

  def main(args:Array[String]): Unit ={
     
    val conf = new SparkConf().setAppName("COMPLEX").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    println("========= Regular JSON read ===========");
    
    val regularDF = spark.read.format("json").load("file:///E:/Hadoop/Hadoop_Data/Format_files/devices.json");
    regularDF.show();
    
    // Intentionally made the data to complex.
    val complexDf = spark.read.format("csv").option("delimiter","~").load("file:///E:/Hadoop/Hadoop_Data/Format_files/devices.json");
    complexDf.show(false);
    
    val weblogschema = StructType(Array(
					StructField("device_id", StringType, true),
					StructField("device_name", StringType, true),
					StructField("humidity", StringType, true),
					StructField("lat", StringType, true),
					StructField("long", StringType, true),    
					StructField("scale", StringType, true),
					StructField("temp", StringType, true),
					StructField("timestamp", StringType, true),
					StructField("zipcode", StringType, true)));  
    
    println("=========== JSON to COLUMNS ==========");
    val fromJson = complexDf.withColumn("_c0", from_json(col("_c0"), weblogschema)).select(col("_c0.*"));
    fromJson.show();
    
    println;
    
    println("=========== COLUMNS to JSON ==========");
    val toJson = fromJson.select(to_json(struct($"device_id", $"device_name",$"humidity",$"lat",$"long",$"scale",$"temp",$"timestamp",$"zipcode"))).alias("JSON");
    toJson.show(false);
    
    toJson.printSchema();
    
    println("=========== COLUMNS to JSON  1 ==========");
    val toJson1 = fromJson.select(to_json(struct(col("*"))).alias("Column"));
    toJson1.show(false);
    toJson1.printSchema();
  }
  
}