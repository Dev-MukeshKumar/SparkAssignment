package com.credits

import org.apache.spark.sql.SparkSession
import data.generate.GenerateData._
import data.generate.RecordSchema
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import java.util.Properties
import scala.io.Source

object Credits extends Serializable{

  @transient lazy val logger: Logger =Logger.getLogger(getClass.getName)

  def main(args: Array[String]):Unit = {

    logger.info("Starting credits app.")

    val spark = SparkSession.builder().config(getSparkConf()).getOrCreate()

    //dataset creation
    val creditsRdd = getCreditsRdd(spark)

    logger.info(getCountOfRecords(creditsRdd))

    getMaxCreditsScoreInEachState(creditsRdd)

    logger.info(countOfCreditScoreGreaterThan100(creditsRdd))

    logger.info("samples of 2x credit scores:")
    increaseCreditsScoreBy2x(creditsRdd).take(10).foreach(logger.info(_))

    creditsRdd.unpersist()
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

  def getCreditsRdd(spark: SparkSession): RDD[RecordSchema] = spark.sparkContext.parallelize(getData(1000))

  def getCountOfRecords(rdd:RDD[RecordSchema]): Long = rdd.count()

  def getMaxCreditsScoreInEachState(rdd:RDD[RecordSchema]):Unit = rdd.map(data => (data.stateCode,data.creditScore)).reduceByKey((x,y) => x max y).collect().foreach(data => logger.info(data))

  def countOfCreditScoreGreaterThan100(rdd:RDD[RecordSchema]):Long = rdd.filter(x => x.creditScore > 100).count()

  def increaseCreditsScoreBy2x(rdd: RDD[RecordSchema]):RDD[RecordSchema] = rdd.map(data => RecordSchema(data.stateCode, data.bankId, data.areaName,data.accountId,data.creditScore * 2,data.hasCreditCard))

}
