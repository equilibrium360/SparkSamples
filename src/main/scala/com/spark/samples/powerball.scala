package com.spark.samples

/**
  * Hello world!
  *
  */


import org.apache.spark._

//import sqlContext.implicits._

object PowerBall {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("JoinGroupByTest")
    val sc = new SparkContext(sparkConf)

    val filepath = new java.io.File(".").getCanonicalPath

    //println(getClass.getPackagepower)

    val numberFile = sc.textFile(filepath + "/inputdata")

    println("#############")

    println(filepath + "/inputdata")

    def processInput(x: String): Int = {
      val ar = x.split(" ")
      println(ar.mkString(" "))
      println(ar(4))

      val fourAr = Array(ar(2).toInt, ar(4).toInt, ar(6).toInt, ar(8).toInt, ar(10).toInt)

      scala.util.Sorting.quickSort(fourAr)
      fourAr(4)

      //println(fourAr new comment .mkString(" "))

      //ar(4)
    }



    val counts = numberFile.map(processInput)
      .map(word => (word, 1))
      .reduceByKey((a, b) => a + b)

    val swap = counts.map(x => (x._2, x._1))

    val sortedCount = swap.sortByKey(ascending = false, 3)

    sortedCount.take(10).foreach(println)


  }
}