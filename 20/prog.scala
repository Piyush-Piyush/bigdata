import org.apache.spark.sql.SparkSession

object AverageMarksCombineByKey {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("AverageMarksCombineByKey")
      .master("local[*]")  // Run Spark locally with all available cores
      .getOrCreate()

    // Sample input data
    val data = Array(
      ("Joe", "Maths", 83),
      ("Joe", "Physics", 74),
      ("Joe", "Chemistry", 91),
      ("Joe", "Biology", 82),
      ("Nik", "Maths", 69),
      ("Nik", "Physics", 62),
      ("Nik", "Chemistry", 97),
      ("Nik", "Biology", 80)
    )

    // Convert data to RDD
    val rdd = spark.sparkContext.parallelize(data)

    // Use combineByKey to calculate sum and count for each student
    val sumCount = rdd.combineByKey(
      // createCombiner: (marks) => (marks, 1)
      (marks: Int) => (marks, 1),
      // mergeValue: (acc, marks) => (acc._1 + marks, acc._2 + 1)
      (acc: (Int, Int), marks: Int) => (acc._1 + marks, acc._2 + 1),
      // mergeCombiners: (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    )

    // Calculate average marks
    val averageMarks = sumCount.mapValues { case (sum, count) =>
      sum.toDouble / count
    }

    // Print results
    println("Average marks per student:")
    averageMarks.collect().foreach { case (student, average) =>
      println(s"Student: $student, Average Marks: $average")
    }

    // Stop SparkSession
    spark.stop()
  }
}
