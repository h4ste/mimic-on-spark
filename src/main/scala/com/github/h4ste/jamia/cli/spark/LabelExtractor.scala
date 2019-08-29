package com.github.h4ste.jamia.cli.spark

import java.nio.file.Paths
import java.sql.Timestamp
import java.time.LocalDateTime

import com.github.h4ste.spark.SparkApplication
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import picocli.CommandLine.{Option, Parameters}

trait LabelExtractor extends Runnable with SparkApplication {

  @Parameters(index = "1",
    description = Array("Path to note-based labels"),
    paramLabel = "NOTE-LABELS")
  var notePath: String = _

  @Parameters(index = "0",
    description = Array("directory to save label CSV files"),
    paramLabel = "OUTPUT-DIRECTORY")
  var outputDir: String = _

  @Option(names = Array("-f", "--force-recreate"),
    description = Array("recreate all intermediate tables rather than re-using them (if they have already been defined)"))
  var recreate: Boolean = false


  val parseTimestamp: UserDefinedFunction =
    udf((timestamp_str: String) => {
      val timestamp = Timestamp.valueOf(LocalDateTime.parse(timestamp_str))
      assert(timestamp != null)
      timestamp
    })

  // Read Note DX
  def readNoteLabels(path: String, viewName: String = null): DataFrame = {
    val df = spark.read
      .schema(StructType(Array(
        StructField("subject_id", IntegerType, nullable = false),
        StructField("hadm_id", IntegerType, nullable = true),
        StructField("timestamp_str", StringType, nullable = false),
        StructField("label", BooleanType, nullable = false)
      )))
      .option("header", "true")
      .option("mode", "FAILFAST")
      .csv(path)
      .select(col("subject_id"), col("hadm_id"), parseTimestamp(col("timestamp_str")).as("timestamp"), col("label"))
    if (viewName != null) {
      df.createOrReplaceTempView(viewName)
    }
    df
  }

  def write_csv(tableOrView: String, filename: String): Unit = {
    val df = spark.sql(s"SELECT * FROM $tableOrView")
    saveToCsv(df, Paths.get(outputDir, filename).toAbsolutePath.toString)
  }

  def write_csv(df: DataFrame, filename: String): Unit = {
    assert(df != null, "attempted to save null dataframe")
    saveToCsv(df, Paths.get(outputDir, filename).toAbsolutePath.toString)
  }


  def make_labels(note_label_query: String,
                  table_label_query: String,
                  icd9_label_query: String): DataFrame = {
    spark.sql(
      s"""  WITH note_labels AS (
         |$note_label_query
         |       ),
         |
         |       icd_labels AS (
         |$icd9_label_query
         |       ),
         |
         |       table_labels AS (
         |$table_label_query
         |       )
         |
         |SELECT a.subject_id,
         |       a.hadm_id,
         |       MAX(COALESCE(note.label, false)) AS note_dx,
         |       MAX(COALESCE(table.label, false)) AS table_dx,
         |       MAX(COALESCE(icd.label, false)) AS icd_dx
         |  FROM jamia.adults AS a
         |       LEFT OUTER JOIN note_labels AS note
         |       ON a.subject_id = note.subject_id
         |       AND a.hadm_id = note.hadm_id
         |
         |       LEFT OUTER JOIN table_labels AS table
         |       ON a.subject_id = table.subject_id
         |       AND a.hadm_id = table.hadm_id
         |
         |       LEFT OUTER JOIN icd_labels AS icd
         |       ON a.subject_id = icd.subject_id
         |       AND a.hadm_id = icd.hadm_id
         | WHERE a.hadm_id IS NOT NULL
         | GROUP BY a.subject_id, a.hadm_id""".stripMargin
    )
  }

  def make_label_stats(labels: DataFrame): DataFrame = {
    labels.createOrReplaceTempView("_labels")
    spark.sql(
      """SELECT note_dx,
        |       table_dx,
        |       icd_dx,
        |       COUNT(DISTINCT hadm_id) AS num_hadms,
        |       CAST(COUNT(DISTINCT hadm_id) AS DOUBLE) / (SELECT CAST(COUNT(1) AS DOUBLE) FROM _labels) AS percent
        |  FROM _labels
        | GROUP BY icd_dx, table_dx, note_dx
        | ORDER BY icd_dx DESC, table_dx DESC, note_dx DESC""".stripMargin
    )
  }
}