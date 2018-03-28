package com.mapr.audit


import java.io.PrintWriter
import java.net.Socket

import com.mapr.db.spark._
import org.apache.spark.streaming.dstream.DStream

class OpenTSDBPublisher(val host: String, val port: Int) extends Serializable {
  def publish(stream: DStream[String]) = stream.mapPartitions(partition => {
    var count = 0
    //TSDB parameters
    val sock = new Socket(host, port)
    val writer = new PrintWriter(sock.getOutputStream, true)
    val helper = new FidToPathHelper()

    partition.foreach(rowData => {
      val doc = MapRDBSpark.newDocument(rowData)

      val timestamp = doc.getTimestamp("timestamp").getMillis
      val operation = doc.getString("operation")
      val status = doc.getInt("status")

      var uid: String = "none"
      try {
        uid = doc.getInt("uid").toString
      } catch {
        case _: Throwable => //do nothing
      }

      val tsdStr = new StringBuilder()
        .append("put ")
        .append("audit.count.metric ")
        .append(timestamp).append(" ")
        .append(1).append(" ")

      val docMap: Map[String, AnyRef] = doc.asMap()

      for((key, value) <- docMap.filterKeys(!_.equals("timestamp")).filterKeys(!_.equals("uid"))) {
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
