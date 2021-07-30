package CassendraSparkPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions._;

object CassendraSpark {
  
    def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("JOINS").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder()
					.config("spark.cassandra.connection.host","localhost")
					.config("spark.cassandra.connection.port","9042")
					.getOrCreate()
					import spark.implicits._

					val cassandradf = 
					spark
					.read
					.format("org.apache.spark.sql.cassandra")
					.options(Map(
					    "keyspace"->"zeyobatch28",
					    "table"->"emp_data"))
					.load()
					
					println("===== Basic Read from Cassendra =======");
					cassandradf.show()
    
					println("===== txns file read from Cassendra =======");
					val txnDf = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/txns_head.csv");
					txnDf.show();
					
					println("===== txns file write to Cassendra =======");
					txnDf.write.format("org.apache.spark.sql.cassandra").options(Map(
					    "keyspace"->"zeyobatch28",
					    "table"->"txn_data")).save();
					
					println("===== read txn_data from cassendra =======");
					val txn_dataDf = 
					spark
					.read
					.format("org.apache.spark.sql.cassandra")
					.options(Map(
					    "keyspace"->"zeyobatch28",
					    "table"->"txn_data"))
					.load()
					txn_dataDf.show();
					
    }
}