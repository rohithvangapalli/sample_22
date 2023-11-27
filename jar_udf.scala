import org.apache.spark.sql.SparkSession
import DataFrameProcessor.processDataFrame
 
val spark: SparkSession = SparkSession.builder().appName("AlmarenEMR").enableHiveSupport().getOrCreate()
 
var df = spark.read
  .format("csv")
  .option("header", "true")
  .option("inferSchema", "false")
  .option("delimiter", "\t")
  .option("comment", "#")
  .load("s3://gratis-bucket-test/mt4002_testing/sample.gz")
 
df = DataFrameProcessor.processDataFrame(df)
 
df.show(false)
