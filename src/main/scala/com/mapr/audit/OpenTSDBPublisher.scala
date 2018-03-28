package com.mapr.audit


import java.io.PrintWriter
import java.net.Socket

import com.mapr.db.spark._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

class OpenTSDBPublisher(val host: String, val port: Int) extends Serializable {
  def publish(stream: DStream[ConsumerRecord[String, String]]) = stream.mapPartitions(partition => {
    var count = 0
    //TSDB parameters
    val sock = new Socket(host, port)
    val writer = new PrintWriter(sock.getOutputStream, true)
    val helper = new FidToPathHelper()

    partition.foreach(rowData => {
      val doc = MapRDBSpark.newDocument(rowData.value())

      val timestamp = doc.getTimestamp("timestamp").getMillis
      var uid: String = "none"
      try {
        uid = doc.getInt("uid").toString
      } catch {
        case _: Throwable => //do nothing
      }

      /**
        * Build OpenTSDB String -> "put metric timestamp value tag1=val1 tag2=val2... "
        */
      val tsdStr = new StringBuilder()
        .append("put ")
        .append("audit.count.metric ")
        .append(timestamp).append(" ")
        .append(1).append(" ")

      //parse objectType
      rowData.topic().split("_")(1) match {
        case "fs" => tsdStr.append("object=").append("fs").append(" ")
        case "db" => tsdStr.append("object=").append("db").append(" ")
        case "cldb" => tsdStr.append("object=").append("cldb").append(" ")
        case "auth" => tsdStr.append("object=").append("auth").append(" ")
        case _ => //Do nothing
      }


      //Add rest as tags
      val docMap: Map[String, AnyRef] = doc.asMap()
      for((key, value) <- docMap.filterKeys(!_.equals("timestamp")).filterKeys(!_.equals("uid"))) {

        //Convert the fids in messages to paths
        if(key.toLowerCase.contains("fid")) {
          key match {
            case "tableFid" => tsdStr.append("tablePath=").append(helper.convertFidToPath(value.toString)).append(" ")
            case "srcFid" => tsdStr.append("srcPath=").append(helper.convertFidToPath(value.toString)).append(" ")
            case "childFid" => tsdStr.append("childPath=").append(helper.convertFidToPath(value.toString)).append(" ")
            case "parentFid" => tsdStr.append("parentPath=").append(helper.convertFidToPath(value.toString)).append(" ")
            case _ => //Do nothing
          }
        }
        tsdStr.append(key).append("=").append(value.toString).append(" ")
      }

      if(!uid.equals("none"))
        tsdStr.append("uid=").append(uid)
      writer.println(tsdStr.mkString)
      writer.flush()
      count += 1
    })
    writer.flush()
    writer.close()
    sock.close()
    Iterator[Int](count)
  })
}
