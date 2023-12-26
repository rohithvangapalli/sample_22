val driverClass = "oracle.jdbc.OracleDriver"
val driver = Class.forName(driverClass)
val version = driver.getPackage.getImplementationVersion
println(s"Oracle JDBC Driver Version: $version")
