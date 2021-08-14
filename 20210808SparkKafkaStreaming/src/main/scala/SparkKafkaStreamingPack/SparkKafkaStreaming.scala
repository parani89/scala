package SparkKafkaStreamingPack

import org.apache.spark.streaming._
import org.apache.spark.sql.functions
import org.apache.kafka.clients.consumer._
import java.lang.IllegalArgumentException
import org.apache.spark.SparkDriverExecutionException
import org.apache.spark.sql.functions.explode
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc._
import com.mysql.jdbc.Driver
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import scala.util.Try
import org.apache.spark.sql.types.DecimalType._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.StructType
import org.apache.spark._


object SparkKafkaStreaming {

	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					.set("spark.driver.allowMultipleContexts","true")


					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")

					val spark = SparkSession
					.builder()

					.getOrCreate()


					import spark.implicits._


					val ssc = new StreamingContext(conf,Seconds(4))

					val kafkaParams = Map[String, Object](
							"bootstrap.servers" -> "localhost:9092",
							"key.deserializer" -> classOf[StringDeserializer],
							"value.deserializer" -> classOf[StringDeserializer],
							"group.id" -> "c2",
							"auto.offset.reset" -> "latest",
							"enable.auto.commit" -> "true")

					val topics = Array("zeyob28")


					val stream1 = KafkaUtils
					.createDirectStream[String, String](ssc,PreferConsistent,
							Subscribe[String, String]
									(topics,
											kafkaParams)
							)


					val stream= stream1.map(x=>x.value())

					stream.print()

					stream.foreachRDD(x =>
					if(!x.isEmpty())
					{



						val df = spark.read.json(x)


						// df.show();
						
/*								val username= df
								.withColumn("results",explode(col("results")))
								.selectExpr("results.user.username as name")

								username.write.format("org.elasticsearch.spark.sql")
								.option("es.nodes.wan.only","true")
								.option("spark.es	.net.http.auth.user","root")
								.option("spark.es.net.http.auth.pass","Aditya@usa908")
								.option("es.net.http.auth.user","root")
								.option("es.net.http.auth.pass","Aditya@usa908")
								.option("es.port","443")
								.option("es.nodes", "https://search-zeyoes-jx7md3pckkpodpjoysbdd33lym.ap-south-1.es.amazonaws.com/")
								.mode("append")
								.save("paraniindex/paranitab")


								println("data written to elastic search")*/




					})

					ssc.start()
					ssc.awaitTermination()
	}

}