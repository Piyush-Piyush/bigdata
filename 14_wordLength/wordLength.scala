object wordLength {
  def main(args: Array[String]): Unit = {
    println("Enter words separated by spaces:")
    val input = scala.io.StdIn.readLine().trim  // Read input from keyboard
    
    val words = input.split("\\s+").toList  // Split input into words and convert to list
    
    // Calculate lengths and find word with maximum length
    val result = findLongestWord(words)
    
    // Print the result
    println(s"The word with the highest length is: ${result._1}, length ${result._2}")
  }
  
  def findLongestWord(words: List[String]): (String, Int) = {
    // Calculate lengths of each word and find the word with maximum length
    words.map(word => (word, word.length)).maxBy(_._2)
  }
}
