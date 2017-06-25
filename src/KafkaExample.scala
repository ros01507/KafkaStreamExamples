import kafka.serializer.StringDecoder

import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Working example of listening for log data from Kafka's testLogs topic on port 9092. */
object KafkaExample {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[2]", "KafkaExample", Seconds(1))

    // hostname:port for Kafka brokers, not Zookeeper
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")

    // List of topics you want to listen for from Kafka
    val topics = List("amarillo").toSet

    // lines of data.
    val lines = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topics).map(_._2)

    lines.print()

    // Extract the request field from each log line
    val requests = lines.flatMap(_.split(","))

    requests.print()

    // Kick it off

    ssc.start()
    ssc.awaitTermination()
  }
}

