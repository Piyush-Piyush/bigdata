import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("WordCount")
      .master("local[*]")  // Run Spark locally with all available cores
      .getOrCreate()

    // Read text file into RDD
    val lines = spark.sparkContext.textFile("path/to/words.txt")

    // Part (i): Count occurrences of each word
    val wordCounts = lines
      .flatMap(_.toLowerCase.split("\\W+"))  // Split by non-word characters and convert to lowercase
      .filter(_.nonEmpty)  // Filter out empty words
      .map((_, 1))  // Map each word to (word, 1)
      .reduceByKey(_ + _)  // Reduce by key to get word counts

    // Part (ii): Arrange word count in ascending order based on word
    val wordCountsSorted = wordCounts
      .sortByKey(ascending = true)
    
    // Part (iii): Display words that begin with 's'
    val wordsStartingWithS = wordCounts
      .filter { case (word, count) => word.startsWith("s") }

    // Print results
    println("Word counts:")
    wordCountsSorted.collect().foreach { case (word, count) =>
      println(s"$word: $count")
    }

    println("\nWords starting with 's':")
    wordsStartingWithS.collect().foreach { case (word, count) =>
      println(s"$word: $count")
    }

    // Stop SparkSession
    spark.stop()
  }
}
