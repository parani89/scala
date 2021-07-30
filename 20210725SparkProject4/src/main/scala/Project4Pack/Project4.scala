package Project4Pack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer;
import org.apache.hadoop.hbase._;
import org.apache.spark.sql.execution.datasources.hbase._;
import org.apache.spark.sql.hive.HiveContext

object Project4 {

	def main(args: Array[String]): Unit = {

			val conf = new SparkConf().setAppName("Project_4").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().enableHiveSupport()
					.config("hive.exec.dynamic.partition.mode","nonstrict").getOrCreate();
			import spark.implicits._;

			// STEP 1
			println("======== Read the Meta Data ========");
			//val allData = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/project4_data.csv");
			val allData = spark.read.format("csv").option("header","true").load("file:///home/cloudera/data/project4_data.csv");
			allData.show();

			println("=== DF to List Prefix ======");
			val dfListPrefix = allData.select("scol").map(r => (r.getString(0)).split('_')(0)).collect.toList.distinct
					println(dfListPrefix);

			println("=== DF to List All ======");
			val dfListAll = allData.select("scol").map(r => (r.getString(0))).collect.toList.distinct
					println(dfListAll);

			//val df2 = metaDataDf.select(split(col("scol"),"_").getItem(0))
			//df2.show();
    
      // STEP 2 Creating Dynamic catalog String
      println("====== Dataframe to List ========");
      val dfList = allData.select("hcol","scol").map(r => (r.getString(0), r.getString(1))).collect.toList 
      //val dfList1 = allData.select("hname").rdd.map(r => r(0).toString()).collect().toList

      println(dfList);
      println;
    
			var string1 = """{
					"table":{"namespace":"default", "name":"hbase_tract101"},
					"rowkey":"masterid",
					"columns":{
					"masterid":{"cf":"rowkey", "col":"masterid", "type":"string"},"""

			var string2 ="";
			    dfList.foreach {
			      case(hcol,scol) => {
				    string2 += s"""
						  "${scol}":{"cf":"cf", "col":"${hcol}", "type":"string"},"""
			      }
			    }

			string2 = string2.dropRight(1)+"}"+"\n";

			var string3="}"
			
			var finalString = string1+string2+string3;
			
			println("======== final catalog ==========");
			println(finalString);
			
			println("========= HBASE READ ==========");
			val dfHbase=spark.read.options(Map(HBaseTableCatalog.tableCatalog->finalString)).format("org.apache.spark.sql.execution.datasources.hbase").load()
			dfHbase.show();
			
			println("======= Took prexied columns =======");
			var createCommands = new ListBuffer[String]()
					for (prefix <- dfListPrefix ) {
						var createTable = s"create table proj4.${prefix} (master_id string,";
						for( col <- dfListAll.filter(x=>x.contains(prefix))) {
							//
							createTable += s"${col}"+" string,";
						}
						createTable = createTable.dropRight(1)+")";
						createCommands += createTable;
						createTable = "";
					}

			println("======== Create commands =========");
			for (createCommand <- createCommands) {
				println(createCommand)
			}

			val hc = new HiveContext(sc);
			import hc.implicits._;
			
			println("======== Running the command in hive =========");
			for (createCommand <- createCommands) {
				hc.sql(createCommand)
			}



	}

}