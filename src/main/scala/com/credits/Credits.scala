package com.credits

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import data._
import org.apache.spark.{SparkConf, sql}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import java.util.Properties
import scala.io.Source

object Credits extends Serializable{

  @transient lazy val logger: Logger =Logger.getLogger(getClass.getName)

  def main(args: Array[String]):Unit = {

    logger.info("Starting credits app.")

    val spark = SparkSession.builder().config(getSparkConf()).getOrCreate()

    //a RDD of case class
    val creditsRddOfCaseClass = getCreditsRddCaseClassSchema(spark)
    logger.info("RDD of case class Schema:")
    creditsRddOfCaseClass.foreach(logger.info(_))
    logger.info("----------------------------------------------------------------")

    //a RDD of case class
    val creditsRddOfClass = getCreditsRddClassSchema(spark)
    logger.info("RDD of class Schema:")
    creditsRddOfClass.foreach(data => logger.info(display(data)))
    logger.info("----------------------------------------------------------------")

    //a DataFrame
    val creditsDf = getCreditsDf(spark)
    logger.info("DF of credits showed in console:")
    creditsDf.filter("creditScore > 130").show()
    logger.info("----------------------------------------------------------------")

    //a DataSet of case class
    val creditsDs = getCreditsDs(spark)
    logger.info("Ds of credits showed in console:")
    creditsDs.filter(row => row.creditScore>130).show()
    logger.info("----------------------------------------------------------------")

    spark.stop()
    logger.info("Shutting down credits app.")
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf()
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").reader())
    properties.forEach((k,v) => sparkConf.set(k.toString,v.toString))
    sparkConf
  }

  //data parallelize and readers
  def getCreditsRddCaseClassSchema(spark: SparkSession): RDD[RecordCaseClassSchema] = spark.sparkContext.parallelize(SeqOfCaseClassData.generateData(10))

  def getCreditsRddClassSchema(spark: SparkSession): RDD[RecordClassSchema] = spark.sparkContext.parallelize(SeqOfClassData.generateData(10))

  def getCreditsDf(session: SparkSession): DataFrame = session.sqlContext.createDataFrame(SeqOfCaseClassData.generateData(10)).toDF

  def getCreditsDs(session: SparkSession): Dataset[RecordCaseClassSchema] = {
    import session.implicits._
    session.sqlContext.createDataFrame(SeqOfCaseClassData.generateData(10)).toDF.as[RecordCaseClassSchema]
  }

  //utility functions
  def display(record: RecordClassSchema) = s"RecordClassSchema(${record.stateCode},${record.bankId},${record.areaName},${record.accountId},${record.creditScore},${record.hasCreditCard})"
}
