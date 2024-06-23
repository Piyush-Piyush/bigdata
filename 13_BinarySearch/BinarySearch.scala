object BinarySearch {
  
  def main(args: Array[String]): Unit = {
    val sortedList = List(1, 3, 5, 7, 9, 11, 13, 15, 17, 19)
    
    // Testing the binarySearch function
    println(binarySearch(sortedList, 11))  // true
    println(binarySearch(sortedList, 8))   // false
  }
  
  def binarySearch(sortedList: List[Int], target: Int): Boolean = {
    def search(low: Int, high: Int): Boolean = {
      if (low > high) {
        false
      } else {
        val mid = low + (high - low) / 2
        val midValue = sortedList(mid)
        
        if (midValue == target) {
          true
        } else if (midValue > target) {
          search(low, mid - 1)  // Search in the left half
        } else {
          search(mid + 1, high) // Search in the right half
        }
      }
    }
    
    search(0, sortedList.length - 1)
  }
}
