package SparkDSLPack

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions._;

object SparkDsl {

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


			def main(args:Array[String]):Unit ={

					val conf = new SparkConf().setAppName("DSL").setMaster("local[*]");
					val sc = new SparkContext(conf);
					sc.setLogLevel("ERROR");

					val spark = SparkSession.builder().getOrCreate();
					import spark.implicits._;

					val src_path = args(0);
					val dst_path = args(1);
					
					println("====== Raw data usdata =====");
					val usdataDf = spark.read.format("csv").option("header","true").load(src_path);
					usdataDf.show();

					println("====== LA Filtered data =====");
					val lsDataDf = usdataDf.filter(col("state") === "LA")
							lsDataDf.show();

					println("====== Pick only domain on web =====");
					//val nameEmailDf = usdataDf.withColumn("NameEmail",split(col("web"),"[.]").getItem(1))
					val nameEmailDf = usdataDf.withColumn("NameEmail", expr("split(web,'[.]')[1] as NameEmail"));
					nameEmailDf.show(false);

					println("====== Raw data txns =====");
					val txndataDf = spark.read.format("csv").schema(schema_struct).option("header","true").load(src_path);
					txndataDf.show();

					println("====== First Filtered txns =====");
					val filterdData = txndataDf.filter(!col("category").isin("Gymnastics","Team Sports"));
					//val filterdData = txndataDf.filter(!col("category").isin("Gymnastics","Team Sports")).withColumn("IN_OR_OUT", expr("case when category in ('Exercise & Fitness','Outdoor Recreation') then 'Outdoor' else 'Indoor' end"));
					filterdData.show();

					println("====== Second Filtered txns =====");
					val caseData = filterdData.withColumn("IN_OR_OUT", expr("case when category in ('Exercise & Fitness','Outdoor Recreation') then 'Outdoor' else 'Indoor' end"));
					caseData.show();

					println("====== Before write to file =====");

					caseData.write.format("com.databricks.spark.avro").mode("ignore").save(dst_path);

					println("====== After write to file =====");

					println("============= Before write to AWS ===========");

					caseData.write.format("jdbc")
					.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
					.option("driver","com.mysql.jdbc.Driver")
					.option("dbtable","parani_task_tab1")
					.option("user","root")
					.option("password","Aditya908")
					.save();

					println("============= After write to AWS ===========");

					println("============= Before second Read from AWS ===========");

					val secondRead=spark.read.format("jdbc")
							.option("url","jdbc:mysql://mysql56.cki8jgd5zszv.ap-south-1.rds.amazonaws.com:3306/batch28")
							.option("driver","com.mysql.jdbc.Driver")
							.option("dbtable","parani_task_tab1")
							.option("user","root")
							.option("password","Aditya908")
							.load();

					println("============= After second Read from AWS ===========");
					
					secondRead.show();

	}
}