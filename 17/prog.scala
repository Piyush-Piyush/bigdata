import org.apache.spark.sql.SparkSession

object WordCountPairRDD {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("WordCountPairRDD")
      .master("local[*]")  // Run Spark locally with all available cores
      .getOrCreate()

    // Read text file into RDD of lines
    val lines = spark.sparkContext.textFile("path/to/text.txt")

    // Split lines into words and create Pair RDD (word, 1)
    val wordCounts = lines
      .flatMap(_.toLowerCase.split("\\W+"))  // Split by non-word characters and convert to lowercase
      .filter(_.nonEmpty)  // Filter out empty words
      .map(word => (word, 1))  // Map each word to (word, 1)

    // Perform reduceByKey to get word counts
    val wordCountPairs = wordCounts
      .reduceByKey(_ + _)  // Sum up the counts for each word

    // Collect results and print
    val result = wordCountPairs.collect()
    result.foreach { case (word, count) =>
      println(s"$word: $count")
    }

    // Stop SparkSession
    spark.stop()
  }
}
