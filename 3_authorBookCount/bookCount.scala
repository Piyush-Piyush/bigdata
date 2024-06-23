import  scala.collection.mutable
object bookCount {
    def bookcount(arr : List[(String, String)]) : Map[String, Int] = {
        val mp = mutable.Map[String, Int]().withDefaultValue(0);
        for((author, book) <- arr){
            mp(author) += 1
        }
        mp.toMap()
    }

    def main(args : Array[String]):Unit ={
        val books = List(
        ("Dr. Seuss", "How the Grinch Stole Christmas!"),
        ("Jon Stone", "Monsters at the End of This Book"),
        ("Dr. Seuss", "The Lorax"),
        ("Jon Stone", "Big Bird in China"),
        ("Dr. Seuss", "One Fish, Two Fish, Red Fish, Blue Fish")
        )
        val count =  bookcount(books)
        for((author, num) <- count){
            print(author)
            print(" -> ")
            print(num)
            println("")
        }
    }
}