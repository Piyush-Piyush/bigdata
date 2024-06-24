import org.apache.spark.sql.SparkSession

object CombineByKeyExample {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("CombineByKeyExample")
      .master("local[*]") // Run Spark locally with all available cores
      .getOrCreate()

    // Sample input data
    val data = Seq(
      ("coffee", 2),
      ("cappuccino", 5),
      ("tea", 3),
      ("coffee", 10),
      ("cappuccino", 15)
    )

    // Convert data to RDD
    val rdd = spark.sparkContext.parallelize(data)

    // Apply combineByKey
    val combinedRDD = rdd.combineByKey(
      (quantity: Int) => quantity,                        // createCombiner: initialize with quantity
      (agg: Int, quantity: Int) => agg + quantity,        // mergeValue: sum up quantities
      (agg1: Int, agg2: Int) => agg1 + agg2              // mergeCombiners: combine sums from different partitions
    )

    // Collect results and print
    val result = combinedRDD.collect()
    result.foreach { case (beverage, totalQuantity) =>
      println(s"$beverage: $totalQuantity")
    }

    // Stop SparkSession
    spark.stop()
  }
}
