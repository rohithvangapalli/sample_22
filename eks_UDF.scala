import java.net.URLDecoder
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().appName("SparkUDF").getOrCreate()

def phenotype_to_icd10(phenotype: String): String = {
var icd10 = ""
if (phenotype.length() <= 3) {
icd10 = phenotype
} else if (phenotype.length() > 3) {
icd10 = phenotype.substring(0, 3) + '.' + phenotype.substring(3)
}
return icd10
}

// Assuming you have the UDF registered as described
spark.udf.register("get_icd10", (path: String) => {
val phenotype = URLDecoder.decode(path, "utf-8").split("/").last.split("\\.").head.replaceAll("_pval", "")
phenotype_to_icd10(phenotype)
})
// Sample data
val data = Seq("/some/path/phenotype_pval.txt", "/another/path/another_phenotype_pval.txt")
// Create a DataFrame
import spark.implicits._
val df = data.toDF("path")
// Use the registered UDF in a DataFrame transformation
val resultDF = df.selectExpr("path", "get_icd10(path) as icd10")
// Show the result
resultDF.show()
