package com.mapr.audit

import com.mapr.audit.AuditProcessingMain.appName
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}

class AuditConsumer extends Serializable {
  def execute(config: ProcessorConfig): Unit = {

    //Configure spark
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Milliseconds(config.batchInterval.toInt))

    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> config.broker,
      ConsumerConfig.GROUP_ID_CONFIG -> config.group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> config.keyDeserializer,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> config.valueDeserializer,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> config.offsetReset,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.streaming.kafka.consumer.poll.ms" -> config.pollTimeout
    )

    val consumerStrategy = ConsumerStrategies.SubscribePattern[String, String](java.util.regex.Pattern.compile(config.stream + ":" + config.cluster + ".*"), kafkaParams)
    val msgDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )

    val parsed = msgDStream.map(record => record.value())
    val writeCounts: DStream[Int] = new OpenTSDBPublisher(config.tsdbHost, config.tsdbPort).publish(parsed)
    writeCounts.reduce(_ + _).print

    //Start the computation
    println("Start Consuming")
    ssc.start()
    //Wait for the computation to terminate
    ssc.awaitTermination()

  }
}
