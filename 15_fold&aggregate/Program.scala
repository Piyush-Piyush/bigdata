import org.apache.spark.{SparkContext, SparkConf}

object Program {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("User mining")
      .setMaster("local[*]")  // Using all available cores locally
    val sc = new SparkContext(conf)
    
    // Example RDD creation
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5))
    
    // Example using aggregate function to compute sum
    val sumAggregate = rdd.aggregate(0)(_ + _, _ + _)
    println(s"Sum using aggregate(): $sumAggregate")
    
    // Example using fold function to compute sum
    val sumFold = rdd.fold(0)(_ + _)
    println(s"Sum using fold(): $sumFold")
    
    // Stop SparkContext
    sc.stop()
  }
}
