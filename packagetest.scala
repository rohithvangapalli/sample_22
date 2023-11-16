import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

val data = Array(1.0, 2.0, 3.0, 4.0, 5.0)

val stats = new DescriptiveStatistics()
data.foreach(stats.addValue)

println(s"Mean: ${stats.getMean}")
println(s"Standard Deviation: ${stats.getStandardDeviation}")
