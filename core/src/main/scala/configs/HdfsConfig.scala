package ir.mci.dwbi.bigdata.spark_job.core.configs

import java.io.InputStreamReader
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

object HdfsConfig {

def getConfig(path:String) : Config = {
    val fs: FileSystem = org.apache.hadoop.fs.FileSystem.get(new Configuration())
    val file: FSDataInputStream = fs.open(new Path(path))
    val reader = new InputStreamReader(file)
    val config = try {
      ConfigFactory.parseReader(reader)
    } finally {
      reader.close()
    }
    config
}
}
