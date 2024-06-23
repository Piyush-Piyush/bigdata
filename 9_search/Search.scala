object Search {
    def search(arr : Array[Int], key : Int) : String = {
        for(x <- arr){
            if(x == key){
                 return "True"
            }
        }
        "False"
    }

    def main(args : Array[String]) : Unit = {
        val arr = Array(1,2,3,4,5)
        println(search(arr, 4))
        println(search(arr, 6))
    }

}