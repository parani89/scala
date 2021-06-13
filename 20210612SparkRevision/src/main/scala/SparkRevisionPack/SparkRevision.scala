package SparkRevisionPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.Row;

object SparkRevision {

case class schema (txnno:Integer,txndate:String, custno:String, amount:String,category:String, product:String, city:String, state:String, spendby:String);

val schemastruct = StructType(Array(
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

def main(args:Array[String]):Unit ={

		val conf = new SparkConf().setAppName("DSL").setMaster("local[*]");
		val sc = new SparkContext(conf);
		sc.setLogLevel("ERROR");

		val spark = SparkSession.builder().getOrCreate();
		import spark.implicits._;

/*		val file1 = args(0);
		val file2 = args(1);
		val file3 = args(2);
		val file4 = args(3);
		val file5 = args(4);
		val file6 = args(5);
		val output = args(6);*/
		
		val columns_list= List("category","product","txnno","txndate","amount","city","state","spendby","custno")
		
		println("=========== 1 Create a scala List with 1,4,6,7 and Do an iteration and add 2 to it ===========");
		println;
		val myList = List(2,4,6,8);
		val addedList = myList.map(x=>x+2);

		addedList.foreach(println);
		println;

		println("=========== 2 Create a scala List with Zeyobron,zeyo and analytics and filter elements contains zeyo ===========");
		println;
		val strList = List("Zeyobron","zeyo","analytics");
		val zeyoList = strList.filter(x=>x.contains("zeyo"));

		zeyoList.foreach(println);
		println;

		println("=========== 3 Read file1 as an rdd and filter gymnastics rows ===========");
		println;
		val allfile1 = sc.textFile("file:///E:/Hadoop/Hadoop_Data/file1.txt");
		//val allfile1 = sc.textFile(file1);
		val fil1gymData = allfile1.filter(x=>x.contains("Gymnastics"));

		fil1gymData.take(5).foreach(println);
		println;

		println("=========== 4 Create a case class and Impose case class to it and filter product contains Weightlifting? ===========");
		println;
		val mapSplit = allfile1.map(x=>x.split(","));
		val schemaRdd = mapSplit.map(x=>schema(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));
		val gymData = schemaRdd.filter(x=>x.product.contains("Weightlifting"));

		gymData.take(10).foreach(println);
		println;

		println("=========== 5 Read file2 and filter last index equals Cash ===========");
		println;
		val allfile2 = sc.textFile("file:///E:/Hadoop/Hadoop_Data/file2.txt");
		//val allfile2 = sc.textFile(file2);
		val mapSplit1 = allfile2.map(x=>x.split(","));
		val rowRdd = mapSplit1.map(x=>Row(x(0).toInt,x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8)));
		val eigthColumn = rowRdd.filter(x=>x(8).toString().contains("cash"));

		eigthColumn.take(10).foreach(println);
		println;

		println("=========== 6 Create dataframe using schema rdd and row rdd ===========");
		println;

		println("=========== 6.1 Create dataframe using schema rdd ===========");
		println;
		
		val schRddtoDF = schemaRdd.toDF().select(columns_list.map(col):_*);
		schRddtoDF.show(5);

		println("=========== 6.2 Create dataframe using row rdd ===========");
		println;
		
		val rowRddtoDF = spark.createDataFrame(rowRdd, schemastruct).select(columns_list.map(col):_*);
		rowRddtoDF.show(5);
		
		println("=========== 7 Read file 3 as csv with header true and inferschema and Show ===========");
		println;
		
		val seamlessRead = spark.read.format("csv").option("header","true").option("inferschema","true").load("file:///E:/Hadoop/Hadoop_Data/file3.txt").select(columns_list.map(col):_*);
		//val seamlessRead = spark.read.format("csv").option("header","true").option("inferschema","true").load(file3).select(columns_list.map(col):_*);
		seamlessRead.show(5);
		seamlessRead.printSchema();
		
		println("=========== 8 Read file 4 as json and file 5 as parquet and show both the dataframe? ===========");
		println;
		
		println("=========== 8.1 Read file 4 as json ===========");
		println;
		
		val file4DF = spark.read.format("json").load("file:///E:/Hadoop/Hadoop_Data/file4.json").select(columns_list.map(col):_*);
		//val file4DF = spark.read.format("json").load(file4).select(columns_list.map(col):_*);
		file4DF.show(4);
		
		println("=========== 8.2 Read file 5 as parquet  ===========");
		println;
		
		val file5DF = spark.read.load("file:///E:/Hadoop/Hadoop_Data/file5.parquet").select(columns_list.map(col):_*);
		//val file5DF = spark.read.load(file5).select(columns_list.map(col):_*);
		file5DF.show(4);
		
		println("=========== 9 Read file 6 as xml with row tag as txndata  ===========");
		println;
		
		val xmlDF = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load("file:///E:/Hadoop/Hadoop_Data/file6").select(columns_list.map(col):_*);
		//val xmlDF = spark.read.format("com.databricks.spark.xml").option("rowTag","txndata").load(file6).select(columns_list.map(col):_*);
		xmlDF.show(4);
		println;
		
		println("=========== 10 Union all the dataframes  ===========");
		println;
		
		val unionDf = schRddtoDF.union(rowRddtoDF).union(file4DF).union(file5DF).union(xmlDF);
		unionDf.show(5);
		println;
		
		println("=========== 11 Get year from txn date and add one column at the end as status 1 for cash and 0 for credit in spendby and filter txnno<50000  ===========");
		println;
		
		val yearDf = unionDf.withColumn("txndate", expr("split(txndate,'-')[2]"));
		val caseDf = yearDf.withColumn("status", expr("case when spendby='cash' then 0 else 1 end"));
		val finalDf = caseDf.filter(col("txnno") > 5000);
		finalDf.show(5);
		println;
		
		println("=========== 12 Write as an avro in local with mode Append and partition the category column  ===========");
		println;
		
		finalDf.write.mode("append").format("com.databricks.spark.avro").partitionBy("category").save("file:///E:/Hadoop/Hadoop_Data/output/revision");
		//finalDf.write.mode("append").format("com.databricks.spark.avro").partitionBy("category").save(output);
		
		println("=========== File Written ========");
		println;
		
		println("=========== 13 Parameterize all the file paths and export it as a jar and write the data to hdfs.  ===========");
		
}
}