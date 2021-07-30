package StringManipulationPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase._;
import org.apache.hadoop.hbase._;

object HbaseCatalogue {

	def main(args:Array[String]) : Unit ={

			val conf = new SparkConf().setAppName("String_MANI").setMaster("local[*]");
			val sc = new SparkContext(conf);
			sc.setLogLevel("ERROR");

			val spark = SparkSession.builder().getOrCreate();
			import spark.implicits._;

			val first_string = """{
					"table":{"namespace":"default", "name":"hbase_tract101"},  
					"rowkey":"masterid",
					"columns":{
					"masterid":{"cf":"rowkey", "col":"masterid", "type":"string"}, """ + System.lineSeparator()	

					//========================Second string=======================

					//val read_names= spark.read.format("csv").load("file:///E:/Hadoop/Hadoop_Data/string_creation.csv")
					val read_names= spark.read.format("csv").load("file:///home/cloudera/data/string_creation.csv")
					val list_data = read_names.rdd.collect

					var loop_string = ""							 // second variable
					for(row <- list_data) 
					{  var hive_col:String = "\""+row.getString(1)+"\":{\"cf\":\"cf\""+", \"col\":\""+
							row.getString(0)+"\""+", \"type\":\"string\"}," + System.lineSeparator()
							loop_string = loop_string + hive_col
					}			
			loop_string = loop_string.trim().dropRight(1)

					//======================================Third String =========================

					val last_string = System.lineSeparator()+"}"+System.lineSeparator()+"}"

					println("======Catalog String ======");
			val catalog_string = first_string + loop_string + last_string 
					println(catalog_string)

					val dfHbase=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog_string)).format("org.apache.spark.sql.execution.datasources.hbase").load()
					dfHbase.show();

	}
}

			/* val catalog =
      s"""{
|"table":{"namespace":"default", "name":"hbase_tract101"},
|"rowkey":"masterid",
|"columns":{
|"masterid":{"cf":"rowkey", "col":"masterid", "type":"string"},
|"BXCBLL_333":{"cf":"cf", "col":"QWER", "type":"string"},
|"VVGVHGGH_t3t":{"cf":"cf", "col":"WERT", "type":"string"},
|"POPPPIP_3r3g":{"cf":"cf", "col":"ERTY", "type":"string"},
|"BNVVVBN_3453":{"cf":"cf", "col":"RTYU", "type":"string"}
|}
|}""".stripMargin
		

			
			scala> val catalog_string = s"""{
     |                                         "table":{"namespace":"default", "name":"hbase_tract101"},
     |                                         "rowkey":"masterid",
     |                                         "columns":{
     |                                         "masterid":{"cf":"rowkey", "col":"masterid", "type":"string"},
     | "sname":{"cf":"cf", "col":"hname", "type":"string"},
     | "BXCBLL_333":{"cf":"cf", "col":"QWER", "type":"string"},
     | "VVGVHGGH_t3t":{"cf":"cf", "col":"WERT", "type":"string"},
     | "POPPPIP_3r3g":{"cf":"cf", "col":"ERTY", "type":"string"},
     | "BNVVVBN_3453":{"cf":"cf", "col":"RTYU", "type":"string"}
     | }
     | }"""
catalog_string: String =
{
                                        "table":{"namespace":"default", "name":"hbase_tract101"},
                                        "rowkey":"masterid",
                                        "columns":{
                                        "masterid":{"cf":"rowkey", "col":"masterid", "type":"string"},
"sname":{"cf":"cf", "col":"hname", "type":"string"},
"BXCBLL_333":{"cf":"cf", "col":"QWER", "type":"string"},
"VVGVHGGH_t3t":{"cf":"cf", "col":"WERT", "type":"string"},
"POPPPIP_3r3g":{"cf":"cf", "col":"ERTY", "type":"string"},
"BNVVVBN_3453":{"cf":"cf", "col":"RTYU", "type":"string"}
}
}

scala> val dfHbase=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog_string)).format("org.apache.spark.sql.execution.datasources.hbase").load()
<console>:25: error: not found: value HBaseTableCatalog
       val dfHbase=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog_string)).format("org.apache.spark.sql.execution.datasources.hbase").load()
                                          ^

scala> import org.apache.spark.sql.execution.datasources.hbase._;
import org.apache.spark.sql.execution.datasources.hbase._

scala> val dfHbase=spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog_string)).format("org.apache.spark.sql.execution.datasources.hbase").load()

*/