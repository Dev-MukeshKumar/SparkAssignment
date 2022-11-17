package com.credits

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger

import java.util.Properties
import scala.io.Source

object Credits extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  case class SalesManSchema(salesman_id:Int,name:String,city:String,commission:Double)
  case class CustomerSchema(customer_id:Int,customer_name:String,city:String,grade:String,salesman_id:Int)

  def main(args: Array[String]): Unit = {

    logger.info("Starting credits app.")

    val spark = SparkSession.builder().config(getSparkConf()).getOrCreate()

    val salesManData = List(
      (5001,"James Hoog","New york",0.15),
      (5002,"Nail Knite","Paris",0.13),
      (5005,"Pit Alex","London",0.11),
      (5006,"Mc Lyon","Paris",0.14),
      (5007,"Paul Adam","Rome",0.16),
      (5009,"Lauson Hen","San Jose",0.12)
    )

    val customerData = List(
      (3002, "Nick Rimando", "New york", 100, 5001),
      (3007, "Brad Davis", "New york", 200, 5001),
      (3005, "Graham Zusi", "California", 200, 5002),
      (3008, "Julian Green", "London", 300, 5002),
      (3009, "Geoff Cameron", "Berlin", 100, 5003),
      (3003, "Jozy Altidor", "Moscow", 200, 5007),
      (3004, "Fabian Johnson", "Paris", 300, 5006)
    )


    //join 2 RDDs
    val salesManRdd = spark.sparkContext.parallelize(salesManData).map(data => (data._3,data))
    val customerRdd = spark.sparkContext.parallelize(customerData).map(data => (data._3,data))

    logger.info("Inner join of 2 RDD:")
    val innerJoin = salesManRdd.join(customerRdd)
    innerJoin.foreach(logger.info(_))
    logger.info("--------------------------------------------------------")

    logger.info("Left outer join of 2 RDD:")
    val leftOuterJoin = salesManRdd.leftOuterJoin(customerRdd)
    leftOuterJoin.foreach(logger.info(_))
    logger.info("--------------------------------------------------------")

    //join 2 DF
    val salesManDf = spark.sqlContext.createDataFrame(salesManData).toDF("salesman_id","name","city","commission")
    val customerDf = spark.sqlContext.createDataFrame(customerData).toDF("customer_id","customer_name","city","grade","salesman_id")

    val innerJoinDf = salesManDf.join(customerDf,salesManDf.col("city")===customerDf.col("city"))
    logger.info("inner join 2DF")
    innerJoinDf.select("name","customer_name").foreach(logger.info(_))
    logger.info("--------------------------------------------------------")

    //join 2 DS
    import spark.implicits._
    val salesManDs = salesManDf.as[SalesManSchema]
    val customerDs = customerDf.as[CustomerSchema]
    val innerJoinDs = salesManDs.join(customerDs,salesManDs.col("city")===customerDs.col("city"))
    val rightJoinDs = salesManDs.join(customerDs,salesManDs.col("city")===customerDs.col("city"),"rightOuter")
    logger.info("inner join 2DF")
    innerJoinDs.select("name","customer_name").foreach(logger.info(_))
    logger.info("--------------------------------------------------------")

    logger.info("right join 2DF")
    rightJoinDs.select("name","customer_name").foreach(logger.info(_))
    logger.info("--------------------------------------------------------")

    spark.stop()
    logger.info("Shutting down credits app.")
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").reader())
    properties.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }
}
