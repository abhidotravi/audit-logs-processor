package com.mapr.audit

import com.typesafe.config._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * The entry point class for the Spark Streaming Application.
  *
  * Usage:
  * /opt/mapr/spark/spark-2.2.1/bin/spark-submit --master local[2] \
  * --class com.mapr.audit.AuditProcessingMain target/auditlogs-processor-1.0-SNAPSHOT-jar-with-dependencies.jar \
  * -cluster sirius -stream /var/mapr/auditstream/auditlogstream -group grp
  */
object AuditProcessingMain {
  val appName  = "Audit-Stream-Processor"
  val defaultGroup = "audit-group"
  def main(args: Array[String]): Unit = {
    if (args.length < 2) throw new IllegalArgumentException("Insufficient arguments, run with -h / -help for usage")

    val config = parseArgs(args)
    new AuditConsumer().execute(config)
  }

  def parseArgs(args: Array[String]): ProcessorConfig = {
    var cluster: Option[String] = None
    var stream: Option[String] = None
    var group: Option[String] = None
    args foreach {
      case (value) => value match {
        case "-cluster" => {
          cluster = getVal(args, value)
          if (cluster.isEmpty)
            println("[ERROR]: Value for -cluster is missing")
        }
        case "-stream" => {
          stream = getVal(args, value)
          if (stream.isEmpty)
            println("[ERROR]: Value for -stream is missing")
        }
        case "-group" => group = getVal(args, value)

        case "-h" | "-help" => usage(usageString)
        case _ => if (value.startsWith("-")) {
          println(s"[ERROR] - Unrecognized argument: $value")
          usage(usageString)
        }
      }
    }

    if (cluster.isEmpty || stream.isEmpty)
      usage(usageString)

    val conf = ConfigFactory.load("application")

    ProcessorConfig(
      cluster.get,
      stream.get,
      "localhost:9092",
      group.getOrElse(defaultGroup),
      conf.getString("audit-conf.keyDeserializer"),
      conf.getString("audit-conf.valueDeserializer"),
      conf.getString("audit-conf.offsetReset"),
      conf.getString("audit-conf.pollTimeout"),
      conf.getString("audit-conf.batchInterval"),
      conf.getString("audit-conf.tsdbHost"),
      conf.getInt("audit-conf.tsdbPort")
    )
  }

  def usage(s: => String): Unit = {
    println(s.format(sparkRunCmd))
    System.exit(1)
  }

  def usageString = "Usage: %s -cluster <cluster_name> -stream </path/to/stream> [Options]\n" +
    "Options:\n" + "-h or -help <for usage>\n" + "-group <consumer groupname> [default group: audit-group]\n"

  def sparkRunCmd = "spark-2.2.1/bin/spark-submit --master local[2] --class com.mapr.audit.AuditProcessingMain" +
    " path/to/application.jar "

  def getVal (kvs: Array[String], key: String): Option[String] = {
    if(kvs.isDefinedAt(kvs.indexOf(key)+1))
      Some(kvs(kvs.indexOf(key)+1))
    else
      None
  }
}
