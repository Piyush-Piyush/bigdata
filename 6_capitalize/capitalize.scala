object  capitalize {
    def capitalizeword(sentence : String) = {
        val words = sentence.split(" ")
        val capitalwords = words.map((word) => word.toUpperCase())
        println(capitalwords.mkString(" "))
    }

    def main(args : Array[String]): Unit = {
        val sentence = "hello how are you."
        capitalizeword(sentence)
    }
}