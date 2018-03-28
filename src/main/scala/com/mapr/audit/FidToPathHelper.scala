package com.mapr.audit

import com.mapr.fs.MapRFileSystem
import org.apache.hadoop.conf.Configuration
import java.net.URI


class FidToPathHelper extends Serializable {
  val MAPR_URI = "maprfs:///mapr/"
  val FS_DEFAULT_NAME = "fs.default.name"
  val maprFS: MapRFileSystem = {
    var conf = new Configuration
    conf.set(FS_DEFAULT_NAME, MAPR_URI)
    var fs = new MapRFileSystem()
    fs.initialize(URI.create(MAPR_URI), conf, true)

    fs
  }

  def convertFidToPath(fid: String) = maprFS.getMountPathFid(fid)
  def close() = maprFS.close()
}
