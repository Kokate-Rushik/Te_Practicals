import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WeatherAnalysis {
  def main(args: Array[String]): Unit = {
    
    // 1. Initialize the Spark Booster
    val spark = SparkSession.builder()
      .appName("Weather Analysis")
      .getOrCreate()

    // To suppress unnecessary log subtasks
    spark.sparkContext.setLogLevel("ERROR")

    // 2. Load the dataset (Assuming CSV format with headers)
    // Path is passed via command line args[0]
    val weatherDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    println("Original Weather Data:")
    weatherDF.show(5)

    // 3. Data Processing Grind
    // We calculate Max, Min, and Avg temperature per year/station
    // Assuming columns: 'year', 'temperature', 'humidity'
    val resultDF = weatherDF.groupBy("year")
      .agg(
        max("temperature").alias("Max_Temp"),
        min("temperature").alias("Min_Temp"),
        avg("temperature").alias("Avg_Temp"),
        avg("humidity").alias("Avg_Humidity")
      )
      .orderBy("year")

    // 4. Output results
    println("Processed Weather Statistics:")
    resultDF.show()

    // 5. Save the result to output folder (args(1))
    resultDF.write.mode("overwrite").csv(args(1))

    spark.stop()
  }
}