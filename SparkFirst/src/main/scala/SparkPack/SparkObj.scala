package SparkPack

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkObj {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val data = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns")

					data.take(10).foreach(println);
			
					val  basketdata = data.filter(x=>x.contains("Basketball"))

					println("==== Basket data ===");

					basketdata.take(10).foreach(println);
				//	"hadoop fs -rmr /user/cloudera/basketball_dir_dply" !

					basketdata.saveAsTextFile("file:///E:/Hadoop/Hadoop_Data/output/Ashok")
					
					println("==== File Written ===");

	}
}