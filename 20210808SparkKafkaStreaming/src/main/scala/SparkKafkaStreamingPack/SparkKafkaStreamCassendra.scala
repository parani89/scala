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
import org.apache.spark.sql.cassandra
import com.datastax.spark.connector._

object SparkKafkaStreamCassendra {
	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")
					.set("spark.driver.allowMultipleContexts","true")
					
					conf.set("spark.cassandra.connection.host", "localhost")
					conf.set("spark.cassandra.connection.port", "9042")
					
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()
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

					//stream.print()
					
					//Writing username data from kafka random data read to cassandra table

					stream.foreachRDD(x =>
					if(!x.isEmpty())
					{

						val df = spark.read.json(x)

								val username= df
								.withColumn("results",explode(col("results")))
								.select("results.user.username")

								username.write.format("org.apache.spark.sql.cassandra")
								.options(Map(
										"keyspace"->"zeyobatch28",
										"table"->"kafka_username"))
								.mode("append")
								.save()

								println("data written to cassandra table - zeyobatch28.kafka_username")

					})
					
					ssc.start()
					ssc.awaitTermination()

	}


}
