import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    // Spark Configuration
    val conf = new SparkConf()
      .setAppName("Word Count")
      .setMaster("local[*]")  // Using all available cores locally
    val sc = new SparkContext(conf)

    // Path to the input text file
    val inputPath = "path/to/text.txt"

    // Read the text file
    val textFile = sc.textFile(inputPath)

    // Count the occurrences of each word
    val wordCounts = rdd
      .flatMap(line => line.split(" "))  // Split each line into words using a regex that matches any non-word character
      .filter(word => word.nonEmpty)        // Filter out empty words
      .map(word => word.toLowerCase)        // Convert words to lowercase for case-insensitive counting
      .map(word => (word, 1))               // Create a tuple (word, 1)
      .reduceByKey(_ + _)                   // Sum up the counts for each word

    // Save the word counts to an output file
    val outputPath = "path/to/output"
    wordCounts.saveAsTextFile(outputPath)

    // Display words that appear more than 4 times
    val frequentWords = wordCounts.filter { case (word, count) => count > 4 }
    frequentWords.collect().foreach { case (word, count) => println(s"$word: $count") }

    // Stop the SparkContext
    sc.stop()
  }
}
