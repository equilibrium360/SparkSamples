package com.spark.samples

/**
 * Hello world!
 *
 */


import org.apache.spark._

//import sqlContext.implicits._

object JoinGroupByPrep1 {

  case class School(sc_name: String, sc_id: String, st_id1: String)
  case class Student(st_name: String, st_id: String, st_age: String)





  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("JoinGroupByTest")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._




    val schoolRow1 = List("sc_name1|sc_id1|st_id1", "sc_name2|sc_id2|st_id2")


    val schoolRDD = sc.parallelize( schoolRow1 )


    val schoolDF = schoolRDD.map(rd => rd.split("|")).map( sch => School(sch(0), sch(1), sch(2))).toDF()



    val studentRow1 = List("st_name1|st_d1|st_age1", "st_name2|st_d2|st_age2")


    val studentRDD = sc.parallelize( studentRow1 )

    println( studentRDD.count() + "Hello")

    println( studentRDD.take(2).toSeq.toString() + "take1")

    //val studentDF = studentRDD.map(_.split("|")).map( st => Student(st(0), st(1), st(2)))
    val studentDF = studentRDD.map((rf: java.lang.String) => rf.split("|").length).collect.foreach(println)
    //studentDF.map(t => "Name: " + t.toString     ).collect().foreach(println)



    sc.stop()
  }
}
// scalastyle:on println
