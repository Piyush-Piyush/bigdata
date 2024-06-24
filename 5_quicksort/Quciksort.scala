object Quicksort {
  def main(args: Array[String]): Unit = {
    val array = Array(64, 25, 12, 22, 11)
    println("Original Array:")
    array.foreach(x => print(s"$x "))

    quickSort(array, 0, array.length - 1)

    println("\nSorted Array:")
    array.foreach(x => print(s"$x "))
  }

  def quickSort(arr: Array[Int], low: Int, high: Int): Unit = {
    if (low < high) {
      val pivotIndex = partition(arr, low, high)

      quickSort(arr, low, pivotIndex - 1)
      quickSort(arr, pivotIndex + 1, high)
    }
  }

  def partition(arr: Array[Int], low: Int, high: Int): Int = {
    val pivot = arr(high)
    var i = low - 1

    for (j <- low until high) {
      if (arr(j) < pivot) {
        i += 1
        swap(arr, i, j)
      }
    }

    swap(arr, i + 1, high)
    i + 1
  }

  def swap(arr: Array[Int], i: Int, j: Int): Unit = {
    val temp = arr(i)
    arr(i) = arr(j)
    arr(j) = temp
  }
}
