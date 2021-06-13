package SparkPack02

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import sys.process._

object SparkObj02 {

	def main(args:Array[String]) : Unit = {

			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val alldata = sc.textFile("file:///E:/Hadoop/Hadoop_Data/txns")

					val basketData = alldata.filter(x=>x.contains("Basketball"))

					basketData.take(10).foreach(println)

					basketData.saveAsTextFile("file:///E:/Hadoop/Hadoop_Data/output/Basketball")

					val allcsvdata = sc.textFile("file:///E:Hadoop/Hadoop_Data/usdata.csv")

					val flattenData = allcsvdata.flatMap(x=>x.split(","))

					flattenData.take(10).foreach(println)

					flattenData.saveAsTextFile("file:///E:Hadoop/Hadoop_Data/output/commaseperated")

					val list1 = List(1,2,3,4)
					val list2 = List(7,8,9,10)
					
					val list3 = List.concat(list1,list2)
					
					println(list3)
					
					list3.foreach(println);
	}

}