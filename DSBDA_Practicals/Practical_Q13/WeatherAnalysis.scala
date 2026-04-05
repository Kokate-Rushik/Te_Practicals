import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WeatherAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Global Weather Analysis")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val weatherDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(args(0))

    println("--- Global Weather Statistics (All Years Combined) ---")

    // Remove groupBy to aggregate the entire dataset
    val globalStatsDF = weatherDF.select(
        max("temperature").alias("Global_Max_Temp"),
        min("temperature").alias("Global_Min_Temp"),
        avg("temperature").alias("Global_Avg_Temp"),
        avg("humidity").alias("Global_Avg_Humidity")
      )

    globalStatsDF.show()

    // Save as a single file
    globalStatsDF.coalesce(1).write.mode("overwrite").csv(args(1))

    spark.stop()
  }
}