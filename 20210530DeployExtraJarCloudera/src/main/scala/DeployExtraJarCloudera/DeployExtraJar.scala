package DeployExtraJarCloudera

import org.apache.spark.sql.SparkSession;

object DeployExtraJar {
  
  def main(args:Array[String]): Unit = {
    
    val spark = SparkSession.builder().master("local[*]").getOrCreate();
    import spark.implicits._;
    
    //val dftxn = spark.read.format("csv").load("file:///E:/Hadoop/Hadoop_Data/usdata.csv");
    val dftxn = spark.read.format("csv").load("file:////home/cloudera/data/usdata.csv");
    
    println;
    print("=== Read the us_data.csv ===");
    println;

    println;
    print("=== Write the us_data.csv ===");
    println;
    
    //dftxn.write.mode("append").format("com.databricks.spark.avro").save("file:///E:/Hadoop/Hadoop_Data/output/write_avro_clstr");
    dftxn.write.mode("append").format("com.databricks.spark.avro").save("hdfs:/user/cloudera/avro_dir_spark");
    
    println;
    print("=== Write completed write_avro_clstr ===");
    println;
    
  }
}