package ClouderaJarArgs

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

object ClouderajarArguments {

  def main(args:Array[String]): Unit ={
    
    val conf = new SparkConf().setAppName("ES").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    val src = args(0);
    val dest = args(1);
    val mode = args(2);
    val sourceFormat = args(3);
    val writeFormat = args(4);
    
    println("Src ",src);
    println("Dest ",dest);
    println("Mode ",mode);
    println("SourceFormat ",sourceFormat);
    println("WriteFormat ",writeFormat);
    
    println("==== Before read source ====");
    val srcDF = spark.read.format(sourceFormat).load(src);
    println("==== After read source ====");
    
    println("==== Before Write Dest ====");
    srcDF.write.format(writeFormat).mode(mode).save(dest);
    println("==== After Write Dest ====");
    
  }
}