package com.errisy.spark1

import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

object FirstApp extends App {

  def arrayDemo(sparkConf: SparkConf) ={
    val sc = new SparkContext(sparkConf)

    val rdd1 = sc.makeRDD(Array(1,2,3,4,5,6))

    rdd1.collect().foreach(println)
  }

  def sparkSQLExample() = {
    var sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic")
      .config("spark.config.option", "value")
      .getOrCreate()

    // var dfCsv = sparkSession.read.csv("D:\\BigData\\sample data\\faithful.csv")
    var csvFaith = sparkSession.read.format("csv")
      .option("header", true)
      .option("ignoreLeadingWhiteSpace",true)
      .load(".\\sample data\\faithful.csv")

    var csvPrice = sparkSession.read.format("csv").option("header", true).load(".\\sample data\\pricedata.csv")

    //csvFaith.foreach(line => println(line))
    //csvPrice.foreach(line => println(line))

    val toInt    = udf[Int, String]( _.toInt)
    val toDouble = udf[Double, String]( _.toDouble)
    val toFloat  = udf[Float, String](_.toFloat)
    val toString = udf[String, String](_.toString)

    val dfFaith = csvFaith
      .withColumn("fIndex", toInt(csvFaith("Index")))
      .withColumn("Eruption length (mins)", toDouble(csvFaith("Eruption length (mins)")))
      .withColumn("Eruption wait (mins)", toInt(csvFaith("Eruption wait (mins)")))

    println("dfFaith:")
    dfFaith.show(10)

    val dfPrice = csvPrice
      .withColumn("priceIndex", toInt(csvPrice("index")))
      .withColumn("class", toString(csvPrice("class")))
      .withColumn("price", toDouble(csvPrice("price"))) // take care that to int and to double are very sensitive to the string format

    println("dfPrice:")
    dfPrice.show(10)

    val joinedDF = dfFaith.join(dfPrice, dfFaith("fIndex") <=> dfPrice("priceIndex"), "inner")

    println("Joined Data is here:")
    joinedDF.show()

    joinedDF.select((Seq("fIndex", "class").map(c => col(c))): _*).show()

    // use filter first and then run select to create a new dataframe
    val selectedDF = joinedDF.filter(joinedDF("price") >= 20).select(joinedDF("fIndex"), joinedDF("price") + 1)

    // this the string formatter
    println(s"The filtered dataset has ${selectedDF.count()} entries")
  }

  val conf = new SparkConf()
  conf.setMaster("local")
  conf.setAppName("first application")

  arrayDemo(conf)

  sparkSQLExample()

}
