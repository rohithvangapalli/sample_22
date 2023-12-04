import org.apache.hadoop.util.VersionInfo

try {
  val hadoopVersion = VersionInfo.getVersion
  println(s"Hadoop version: $hadoopVersion")
  sys.exit(0) // Exit with success status
} catch {
  case e: Throwable =>
    println(s"Error getting Hadoop version: ${e.getMessage}")
    sys.exit(1) // Exit with error status
}
