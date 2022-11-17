package com.credits

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import data._
import data.GenerateData.getData
import org.apache.spark.{SparkConf, sql}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import java.util.Properties
import scala.io.Source

object Credits extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Starting credits app.")

    val spark = SparkSession.builder().config(getSparkConf()).getOrCreate()

    //DF to DS
    val creditsDf = getCreditsDf(spark)
    import spark.implicits._
    logger.info("Converting DF to DS")
    val dfToDs = creditsDf.as[RecordCaseClassSchema]
    dfToDs.show()
    logger.info("DF to DS converted")

    //DS to RDD
    logger.info("Converting DS to RDD")
    val dsToRdd = dfToDs.rdd
    dsToRdd.foreach(logger.info(_))
    logger.info("DS to RDD converted")

    //RDD to DS
    logger.info("Converting RDD to DS")
    val rddToDs = dsToRdd.toDS()
    rddToDs.show()
    logger.info("Converted RDD to DS")

    //RDD to DF
    logger.info("Converting RDD to DF")
    val rddToDf = dsToRdd.toDF()
    rddToDf.show()
    logger.info("Converted RDD to DF")

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

  //data parallelize and readers
  def getCreditsRdd(spark: SparkSession): RDD[RecordCaseClassSchema] = spark.sparkContext.parallelize(getData(10)).map(row => RecordCaseClassSchema(row._1, row._2, row._3, row._4, row._5, row._6))

  def getCreditsDf(session: SparkSession): Dataset[Row] = session.sqlContext.createDataFrame(getData(10)).toDF("stateCode", "bankId", "areaName", "accountId", "creditScore", "hasCreditCard")

  def getCreditsDs(session: SparkSession): Dataset[RecordCaseClassSchema] = {
    import session.implicits._
    session.sqlContext.createDataFrame(getData(10)).toDF("stateCode", "bankId", "areaName", "accountId", "creditScore", "hasCreditCard").as[RecordCaseClassSchema]
  }
}
