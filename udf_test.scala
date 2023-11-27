import org.apache.spark.sql.DataFrame
import com.github.music.of.the.ainur.quenya.QuenyaDSL
import java.net.URLDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark: SparkSession = SparkSession.builder().appName("AlmarenEMR").enableHiveSupport().getOrCreate()
val args = spark.sparkContext.getConf.get("spark.driver.args").split(",")
val environment = args(0)
val src_path = args(1) // "data/unstructured/rsa/test/2023-11-22/EMR_test/"

var df = spark.read.format("csv").option("header", "true").option("inferSchema", "false").option("delimiter", "\t").option("comment", "#").load("s3a://gratis-bucket-test/mt4002_testing/sample.gz") // reads .gz files by excluding lines starting with #

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

df = df.withColumn("phenotype", callUDF("get_file_name", input_file_name()))

df = df.withColumn("icd10", callUDF("get_icd10", input_file_name()))

val quenyaDsl = QuenyaDSL
val dsl = quenyaDsl.compile("""
|icd10$icd10:StringType
|phenotype$phenotype:StringType
|chrom$chrom:StringType
|pos$pos:LongType
|ref$ref:StringType
|alt$alt:StringType
|pval$pval:FloatType
|maf$maf:FloatType
|af$af:FloatType
|ac$ac:FloatType
|beta$beta:FloatType
|sebeta$sebeta:FloatType
|or$o_r:FloatType
|num_samples$num_samples:IntegerType
|num_controls$num_controls:IntegerType
|num_cases$num_cases:IntegerType""".stripMargin) // generate a schema

val finalDf:DataFrame = quenyaDsl.execute(dsl,df)
finalDf.printSchema()
finalDf.show(false)
println(finalDf.count())
