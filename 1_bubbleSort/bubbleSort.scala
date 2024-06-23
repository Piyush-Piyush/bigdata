object bubbleSort {
    def bubblesort(arr : Array[Int]):Array[Int] = {
        val len = arr.length
        for(i <- 0 until len-1){
            for(j <- 0 until len - i - 1){
                if(arr(j) > arr(j+1)){
                    val temp = arr(j)
                    arr(j) = arr(j+1)
                    arr(j+1) = temp
                }
            }
        }
        arr
    }

    def main(args : Array[String]) : Unit = {
        val arr = Array(23,45,55,22,0)
        val sortedArr = bubblesort(arr)
        println(sortedArr.mkString(", "))
    }
}