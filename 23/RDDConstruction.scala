import org.apache.spark.{SparkConf, SparkContext}

object RDDConstruction {
  def main(args: Array[String]): Unit = {
    // Spark Configuration
    val conf = new SparkConf()
      .setAppName("RDDConstruction")
      .setMaster("local[*]") // Run Spark locally with all available cores
    val sc = new SparkContext(conf)

    // Collection of data
    val data = Array(11, 34, 45, 67, 3, 4, 90)

    // Number of partitions
    val numPartitions = 3

    // Create RDD with specified number of partitions
    val rdd = sc.parallelize(data, numPartitions)

    // Print RDD partitions
    rdd.glom().foreachPartition(partition => {
      println("Partition contents: " + partition.mkString(", "))
    })

    // Stop Spark Context
    sc.stop()
  }
}
