package StringManipulationPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.datasources.hbase._;
import org.apache.hadoop.hbase._;

object StringManipulation {
  
  def main(args:Array[String]) : Unit ={
    
    val conf = new SparkConf().setAppName("String_MANI").setMaster("local[*]");
    val sc = new SparkContext(conf);
    sc.setLogLevel("ERROR");
    
    val spark = SparkSession.builder().getOrCreate();
    import spark.implicits._;
    
    println("======= Raw Dataframe ======="); 
    val allData = spark.read.format("csv").option("header","true").load("file:///E:/Hadoop/Hadoop_Data/string_creation.csv");
    //val allData = spark.read.format("csv").option("header","true").load("file:///home/cloudera/data/string_creation.csv");
    allData.show();
    
    println("====== Dataframe to List ========");
    val dfList = allData.select("hname","sname").map(r => (r.getString(0), r.getString(1))).collect.toList 
    //val dfList1 = allData.select("hname").rdd.map(r => r(0).toString()).collect().toList

    println(dfList);
    println;
    
    var string1 = "s"+"\"\"\""+s"""{
    "table":{"namespace":"default", "name":"hbase_tract101"},
    "rowkey":"masterid",
    "columns":{
    "masterid":{"cf":"rowkey", "col":"masterid", "type":"string"},"""
    
    var string2 ="";
    dfList.foreach{
      case(hname,sname) => {
       string2 += s"""
                      "${sname}":{"cf":"cf", "col":"${hname}", "type":"string"},""".stripMargin; 
      }
    }
    
    string2 = string2.dropRight(1)+"|}"+"\n";
    
    var string3="|}\"\"\".stripMargin"
    var finalString = string1+string2+string3;
    
    println("=========== Final String ==========");
    
    print(finalString);
    
/*    val dfHbase=spark.read.options(Map(HBaseTableCatalog.tableCatalog->finalString)).format("org.apache.spark.sql.execution.datasources.hbase").load()
    dfHbase.show();*/
    
    			/*val catalog =
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
			 */
    
  /*  {
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
}*/
    
  }
}