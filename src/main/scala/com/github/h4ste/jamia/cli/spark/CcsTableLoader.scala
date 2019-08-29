package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.SparkApplication
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import picocli.CommandLine
import picocli.CommandLine.{Option, Parameters}

import scala.language.{implicitConversions, postfixOps}


class CcsTableLoader extends Runnable with SparkApplication {

  @Parameters(index = "0",
    description = Array("path to CCS CSV files"),
    paramLabel = "PATH")
  var mimicDataPath: String = _

  @Option(names = Array("-g", "--gzipped"),
    description = Array("indicate whether the CCS csv files are gzipped"))
  var gzipped: Boolean = false

  @Option(names = Array("-d", "--database"),
    description = Array("name of database to create (default: ccs_dx)"))
  var databaseName: String = "ccs_dx"

  @Option(names = Array("-m", "--write-mode"),
    description = Array("write mode when creating spark tables. valid options are \"error\", \"append\", \"overwrite\", and \"ignore\"."))
  var writeMode: String = "overwrite"

  private[this] object MimicTables extends Enumeration {

    protected case class CcsTable(fileName: String, schema: StructType, writeOptions: DataFrameWriter[Row] => DataFrameWriter[Row] = identity) extends super.Val {
      def tableName: String = fileName.substring(0, fileName.indexOf('.')).toLowerCase

      def csv: DataFrame = spark.read
        .schema(schema)
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("escape", "\"")
        .option("multiLine", "true")
        .csv(s"$mimicDataPath/$fileName${if (gzipped) ".gz" else ""}")


      def writeAsTable(): Unit = {
        val writer = csv
          .select(csv.columns.map(x => col(x).as(x.toLowerCase)): _*)
          .write
          .mode(writeMode)
        writeOptions(writer).saveAsTable(s"$databaseName.$tableName")
      }
    }

    implicit def valueToMimicTable(x: Value): CcsTable = x.asInstanceOf[CcsTable]

    val ccsSingle = CcsTable("ccs_single_level.csv", StructType(Array(
      StructField("ccs_id", IntegerType, nullable = false),
      StructField("ccs_name", StringType, nullable = false),
      StructField("icd9_code", StringType, nullable = false)
    )))

    val ccsMulti = CcsTable("ccs_multi_level.csv", StructType(Array(
      StructField("ccs_mid", StringType, nullable = false),
      StructField("ccs_name", StringType, nullable = false),
      StructField("ccs_group1", StringType, nullable = true),
      StructField("ccs_group2", StringType, nullable = true),
      StructField("ccs_group3", StringType, nullable = true),
      StructField("icd9_code", StringType, nullable = false)
    )))
  }

  @Override
  def run(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

    for (table <- MimicTables.values) {
      sc.setJobGroup(s"Writing ${table.tableName}", s"Converting ${table.fileName} to ${table.tableName}")
      table.writeAsTable()
    }
  }
}

object CcsTableLoader {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new CcsTableLoader(), System.err, args: _*)
  }
}


