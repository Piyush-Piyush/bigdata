object WordLength {
    def findLength(arr : Array[String]) : Unit = {
        for(i <- 0 until arr.length){
            print(arr(i).length)
            print(" ")
        }
    }

    def mapwordlength(arr : Array[String]) : Array[Int] = {
        arr.map((word) => word.length)
    }

    def main(args : Array[String]) : Unit = {
        val words = Array("Scala", "is", "awesome", "and", "fun")
        findLength(words)
        println()
        val ans = mapwordlength(words).mkString(", ")
        println(ans)
    }
}