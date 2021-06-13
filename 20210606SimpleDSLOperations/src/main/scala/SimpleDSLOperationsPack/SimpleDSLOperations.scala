package SimpleDSLOperationsPack

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkConf;

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SimpleDSLOperations {
  
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
    
    df.show();
    
    println("==== Shown the Raw data ====");
    
    println("==== Selected columns ====");
    val selCols = df.select("txnno","txndate","amount","category","product","spendby");
    selCols.show(3);

    println("==== Select using list ====");
    val col_list=List("txnno","txndate","amount","category","product")
    val col_listDf= df.select(col_list.map(col):_*)
    col_listDf.show(3);
    
    println("==== Selected columns txnno > 5000 and spendby =cash ====");
    val selColsFilter = df.select("txnno","txndate","amount","category","product","spendby").filter(col("txnno") > 5000 && col("spendby") === "cash");
    selColsFilter.show(3);
 
    println("==== Selected columns txnno > 5000 and spendby !=cash ====");
    val selColsFilter1 = df.select("txnno","txndate","amount","category","product","spendby").filter(col("txnno") > 5000 && col("spendby") != "cash");
    selColsFilter1.show(3);
    
    println("==== Selected columns IN condition ====");
    val incondDf = df.filter(col("category").isin("Exercise & Fitness","Team Sports"));
    incondDf.show(3);
    
    println("==== Selected columns NOT IN condition ====");
    val incondDf1 = df.filter(!col("category").isin("Exercise & Fitness","Team Sports"));
    incondDf1.show();
    
    println("==== Selected columns txnno > 5000 and spendby =cash  with product like weighlifting ====");
    val weighDF = selColsFilter.filter(col("product").like("Weightlifting%"));
    weighDF.show(3);
    
    println("==== Selected columns txnno > 5000 and spendby =cash  with product not like weighlifting ====");
    val weighNDF = selColsFilter.filter(!col("product").like("Weightlifting%"));
    weighDF.show(3);
    
    println("==== Split the Year existing column ====");
    val splitYearDf = df.withColumn("txndate", expr("split(txndate,'-')[2] as year"));
    splitYearDf.show(3);
    
    println("==== Split the Year New column ====");
    val splitYearDf1 = df.withColumn("year", expr("split(txndate,'-')[2] as year"));
    splitYearDf1.show();
    
    println("==== String Hardcode at end ====");
    val strHardcodeDf1 = df.withColumn("check",lit(100));
    strHardcodeDf1.show(3);

    println("==== Case condition at end ====");
    val strCaseDF = df.withColumn("validate", expr("case when spendby='cash' then 1 when spendby='credit' then 0 else -1 end"));
    strCaseDF.show();
    
    println("==== With Column take 0 index same column ====");
    val splitDf = df.withColumn("category", expr("split(category,' ')[0] as category"));
    splitDf.show(3);
    
    println("==== With Column take 0 index new column ====");
    val splitDf1 = df.withColumn("category_new", expr("split(category,' ')[0] as category"));
    splitDf1.show(3);
    
  }
}