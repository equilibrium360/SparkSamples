package com.spark.samples

/**
  * Hello world!
  *
  */


import org.apache.spark._
import org.apache.spark.sql._

//import sqlContext.implicits._

object JoinGroupByTest {

  /** Usage: HdfsTest [file] */
  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setMaster("local").setAppName("JoinGroupByTest")
    val sc = new SparkContext(sparkConf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    // this is used to implicitly convert an RDD to a DataFrame.
    import sqlContext.implicits._


    val schoolRow1 = List("same_name|sc_id1|st_id1", "same_name|sc_id2|st_id2")


    val schoolRDD = sc.parallelize(schoolRow1)


    val schoolDF = schoolRDD.map(_.split('|')).map(sch => School(sch(0), sch(1), sch(2))).toDF()



    val studentRow1 = List("same_name|st_id1|st_age1", "same_name|st_id2|st_age2")


    val studentRDD = sc.parallelize(studentRow1)
    val studentDF = studentRDD.map(_.split('|')).map(st => Student(st(0), st(1), st(2))).toDF()



    val joinedSchoolDF = schoolDF.join(studentDF, $"st_id" === $"sh_st_id")

    def genkey(a: Row): CustomKey = {
      CustomKey(a.getAs[String]("sc_name"))


    }

    def createCombinerGroup(eachRow: Row): Row = {

      val row_level2 = Row(eachRow.getAs[String]("sc_id"),
        eachRow.getAs[String]("sh_st_id"),
        eachRow.getAs[String]("st_id"),
        eachRow.getAs[String]("st_age"))

      val row_level2Array = Vector(row_level2)
      return Row(eachRow.getAs[String]("sc_name"), row_level2Array)
    }



    def mergeValueGroup(updatedRow: Row, eachRow: Row): Row = {

      val eachrow_level2 = Row(eachRow.getAs[String]("sc_id"),
        eachRow.getAs[String]("sh_st_id"),
        eachRow.getAs[String]("st_id"),
        eachRow.getAs[String]("st_age"))

      val aa = updatedRow.getSeq(1)

      val res = aa :+ eachrow_level2




      return Row(eachRow.getAs[String]("sc_name"), res)


    }

    def mergeCombinerGroup(updatedRow1: Row, updatedRow2: Row): Row = {

      val updatedRowArray1 = updatedRow1.getSeq(1)
      val updatedRowArray2 = updatedRow2.getSeq(1)

      val updatedRowArray = updatedRowArray1 ++ updatedRowArray2

      return Row(updatedRow1.getString(0), updatedRowArray)


    }



    joinedSchoolDF.map(t => "Name1: " + t.toSeq.toString()).collect().foreach(println)
    val pairRDDToGroup = joinedSchoolDF.rdd.map(hg => (genkey(hg), hg))

    pairRDDToGroup.values.collect().foreach(println)
    pairRDDToGroup.keys.collect().foreach(println)

    val groupedData = pairRDDToGroup.combineByKey(createCombinerGroup, mergeValueGroup, mergeCombinerGroup)



    println("LAST")

    //groupedData.mapValues(t => (t.getString(0).toString() + t.getSeq(1).toString())).collect().foreach(println)
    //groupedData.mapValues(t => (t.get(0).toString() )).collect().foreach(println)
    //
    groupedData.values.collect.foreach(println)

    //GROUPBY  functions returns key and iterator of values so not efficient.
    //Please use combine by key
    // needToGroup.map(t => "Name: " + t.toString()).collect().foreach(println)
    print(new java.io.File(".").getCanonicalPath)
    //print(Object.getClass.getResource("").getPath)

    sc.stop()
  }

  case class School(sc_name: String, sc_id: String, sh_st_id: String)

  case class Student(st_name: String, st_id: String, st_age: String)

  case class CustomKey(cust_key: String)

}

// scalastyle:on println
