import org.apache.spark.sql.SparkSession

object AverageCalculation {
  def main(args: Array[String]): Unit = {
    // Step 1: Create SparkSession
    val spark = SparkSession.builder()
      .appName("AverageCalculation")
      .master("local[*]") // Change to appropriate cluster URL in production
      .getOrCreate()

    // Step 2: Read CSV file into an RDD
    val textFilePath = "path/to/your/input.csv" // Replace with your actual CSV file path

    // Read CSV file into RDD
    val rdd = spark.sparkContext.textFile(textFilePath)
      .map(_.trim.toInt)  // Convert each line to integer

    // Step 3: Calculate average using Spark aggregation functions
    val count = rdd.count()
    val sum = rdd.reduce(_ + _)
    val average = sum.toDouble / count

    // Step 4: Display the result
    println(s"Average of the items: $average")

    // Step 5: Stop SparkSession
    spark.stop()
  }
}
