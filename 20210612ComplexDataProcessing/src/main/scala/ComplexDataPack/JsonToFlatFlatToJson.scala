package ComplexDataPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;

object JsonToFlatFlatToJson {
  
  def main(args:Array[String]): Unit ={
    
    val conf = new SparkConf().setAppName("JSON").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    println("========= Regular Json Read ===========");
    val jsonDf = spark.read.format("json").load("file:///E:/Hadoop/Hadoop_Data/picture.json");
    
    jsonDf.show();
    jsonDf.printSchema();
    
    println("========= Flattern Json ===========");
    
    val flatternJson = jsonDf.select(col("id"),
                                     col("image.height").alias("image_height"),
                                     col("image.url").alias("image_url"),
                                     col("image.width").alias("image_width"),
                                     col("name").alias("name"),
                                     col("thumbnail.height").alias("thumbnail_height"),
                                     col("thumbnail.url").alias("thumbnail_url"),
                                     col("thumbnail.width").alias("thumbnail_width"),
                                     col("type")
                                    );
    
    flatternJson.show(false);
    flatternJson.printSchema();
    
    println("========= Back to Json ===========");
    
    val jsonBackDf = flatternJson.select(
                                      col("id"),
                                      
                                      struct(
                                        col("image_height").alias("height"),
                                        col("image_url").alias("url"),
                                        col("image_width").alias("width")
                                        
                                      ).alias("image"),
                                      
                                      col("name"),
                                      
                                      struct(
                                        col("thumbnail_height").alias("height"),
                                        col("thumbnail_url").alias("url"),
                                        col("thumbnail_width").alias("width")
                                      ).alias("thumbnail"),
                                      col("type")
    )
    						
    jsonBackDf.show(false);
    jsonBackDf.printSchema();
    
  }
}