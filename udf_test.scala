import org.apache.spark.sql.DataFrame
import java.net.URLDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.builder().appName("AlmarenEMR").enableHiveSupport().getOrCreate()
val args = spark.sparkContext.getConf.get("spark.driver.args").split(",")
val environment = "dev"
val src_path = "sample" // "data/unstructured/rsa/test/2023-11-22/EMR_test/"

var df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").option("delimiter", "\t").option("comment", "#").load("s3://gratis-bucket-test/mt4002_testing/sample.csv.gz") // reads .gz files by excluding lines starting with #

def phenotype_to_icd10(phenotype: String): String = {
var icd10 = ""
if (phenotype.length() <= 3) {
icd10 = phenotype
} else if (phenotype.length() > 3) {
icd10 = phenotype.substring(0, 3) + '.' + phenotype.substring(3)
}
return icd10
}

spark.udf.register("get_icd10", (path: String) => {
val phenotype = URLDecoder.decode(path, "utf-8").split("/").last.split("\\.").head.replaceAll("_pval", "")
phenotype_to_icd10(phenotype)
}
)

spark.udf.register("get_file_name", (path: String) => URLDecoder.decode(path, "utf-8").split("/").last.split("\\.").head.replaceAll("_pval", ""))

// df = df.withColumn("phenotype", callUDF("get_file_name", input_file_name()))

// df = df.withColumn("icd10", callUDF("get_icd10", input_file_name()))

 df.show(false)
