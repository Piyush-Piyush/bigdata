import org.apache.spark.sql.SparkSession
import org.apache.spark.HashPartitioner

// Step 1: Define Employee case class
case class Employee(EmpID: Int, Dept: String, EmpDesg: String)

object PartitionExample {
  def main(args: Array[String]): Unit = {
    // Step 2: Create SparkSession
    val spark = SparkSession.builder()
      .appName("PartitionExample")
      .master("local[*]") // Change to appropriate cluster URL in production
      .getOrCreate()
    
    // Step 3: Read data into RDD or DataFrame
    // For demonstration, creating RDD from a local collection
    val data = Seq(
      Employee(1, "IT", "Developer"),
      Employee(2, "HR", "Manager"),
      Employee(3, "IT", "Analyst"),
      Employee(4, "Admin", "Clerk"),
      Employee(5, "HR", "Assistant"),
      Employee(6, "IT", "Tester"),
      Employee(7, "Admin", "Manager")
    )
    
    import spark.implicits._
    
    // Create RDD from the sequence
    val rdd = spark.sparkContext.parallelize(data)
    
    // Step 4: Hash partition by Dept into 4 partitions
    val partitionedRDD = rdd
      .map(emp => (emp.Dept, emp))
      .partitionBy(new HashPartitioner(4))
      .map(_._2)
    
    // Optionally, you can perform further operations on partitionedRDD
    partitionedRDD.foreach(println) // For demonstration, printing each record
    
    // Step 5: Write partitioned data to a destination
    // Example: partitionedRDD.toDF().write.mode("overwrite").csv("output_path")
    
    // Stop SparkSession
    spark.stop()
  }
}
