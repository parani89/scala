package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions.from_json;
import org.apache.spark.sql.functions.to_json;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions.struct;

object JsonRead {

  def main(args:Array[String]): Unit ={
     
    val conf = new SparkConf().setAppName("COMPLEX").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    println("========= Regular JSON read ===========");
    
    val regularDF = spark.read.format("json").option("multiLine","true").load("file:///E:/Hadoop/Hadoop_Data/topping.json");
    regularDF.show();
    regularDF.printSchema();
    
    println("=========== JSON to COLUMNS ==========");
    val flatJson = regularDF.select(col("id"),
                                    col("type"),
                                    col("name"),
                                    col("ppu"),
                                    col("batters.batter.id").alias("batter_id"),
                                    col("batters.batter.type").alias("batter_type"),
                                    col("topping.id").alias("topping_id"),
                                    col("topping.type").alias("topping_type"));
    flatJson.show();
    flatJson.printSchema();
    
    println("=========== JSON to COLUMNS ==========");
    val complexJson = flatJson.select(col("id"),
                                      col("type"),
                                      col("name"),
                                      col("ppu"),
                                      struct(
                                          struct(
                                              col("id"),
                                              col("type")
                                          ).alias("batter")
                                      ).alias("batters"),
                                      struct(
                                          col("id"),
                                          col("type")
                                      ).alias("topping")
                                );
    complexJson.printSchema();
    
  }
  
}