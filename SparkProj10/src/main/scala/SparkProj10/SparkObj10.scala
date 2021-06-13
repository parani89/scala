package SparkPack10

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkObj10 {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val data = sc.textFile("file:///home/cloudera/data/txns")


					val basketdata = data.filter(x=>x.contains("Basketball"))


					"hadoop fs -rmr /user/cloudera/basketball_dir_dply" !


					basketdata.saveAsTextFile("hdfs:/user/cloudera/basketball_dir_dply")

	}
}