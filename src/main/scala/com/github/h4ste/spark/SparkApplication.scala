package com.github.h4ste.spark

import java.io.File

import com.univocity.parsers.csv.{CsvWriter, CsvWriterSettings}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

trait SparkApplication extends AutoCloseable {

  protected[this] val spark: SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
//    .config("spark.sql.warehouse.dir", "sql-warehouse")
    .config("spark.sql.broadcastTimeout", "600")
    .enableHiveSupport()
    .getOrCreate()

  protected[this] val sc: SparkContext = spark.sparkContext

  protected[this] val sql: SQLContext = spark.sqlContext

  private[this] def init(): Unit = {
    LogManager.getRootLogger.setLevel(Level.WARN)
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    Logger.getLogger("com.github.h4ste").setLevel(Level.DEBUG)
  }

  protected[this] def saveToCsv(df: DataFrame, filename: String): Unit = {
    val writer = new CsvWriter(new File(filename), new CsvWriterSettings())
    try {
      writer.writeHeaders(df.columns: _*)
      for (row <- df.collect()) {
        writer.writeRow(row.toSeq.map(_.toString): _*)
      }
    } finally {
      writer.close()
    }
  }


  init()

  def close(): Unit = {
    spark.close()
  }
}
