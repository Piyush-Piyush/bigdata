import org.apache.spark.{SparkConf, SparkContext}

object Prog {
  def main(args: Array[String]): Unit = {
    // Spark Configuration
    val conf = new SparkConf()
      .setAppName("ItemPartitions")
      .setMaster("local[*]") // Run Spark locally with all available cores
    val sc = new SparkContext(conf)

    // Define the Item collection as a Map
    val Item = Map("Ball" -> 10, "Ribbon" -> 50, "Box" -> 20, "Pen" -> 5, "Book" -> 8, "Dairy" -> 4, "Pin" -> 20)

    // Convert the Map to an RDD
    val rdd = sc.parallelize(Item.toSeq) // parallelize the Map

    // Get number of partitions
    val numPartitions = rdd.partitions.length

    // Print number of partitions
    println(s"Number of partitions for RDD: $numPartitions")

    // Stop Spark Context
    sc.stop()
  }
}
