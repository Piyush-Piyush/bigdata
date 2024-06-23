object Countdown {
    def main(args: Array[String]) : Unit = {
        for(i <- 1 to 10){
            println(i)
            Thread.sleep(1000)
        }
    }
}