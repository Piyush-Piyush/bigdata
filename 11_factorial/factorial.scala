object factorial {
    def findfactorial(num : Int) : Int = {
        if(num ==  0){
            return 1
        }
        else{
            num*findfactorial(num-1)
        }
    }
    def findfactforall(collection : Iterable[Int]) = {
        collection.map((num) => findfactorial(num) )
    }

    def main(args : Array[String]):Unit = {
        val nums = Array(0,1,2,3,4,5)
        val fact = findfactforall(nums)
        fact.foreach(println)
    }

}