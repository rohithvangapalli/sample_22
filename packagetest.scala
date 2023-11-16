# Inside the Spark shell

# Import the necessary classes from the external package
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics

# Create some sample data
val data = Array(1.0, 2.0, 3.0, 4.0, 5.0)

# Use the external package to calculate basic statistics
val stats = new DescriptiveStatistics()
data.foreach(stats.addValue)

# Print the calculated statistics
println(s"Mean: ${stats.getMean}")
println(s"Standard Deviation: ${stats.getStandardDeviation}")
