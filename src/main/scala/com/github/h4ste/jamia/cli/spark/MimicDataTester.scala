package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.SparkApplication
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import picocli.CommandLine
import picocli.CommandLine.{Option, Parameters}

class MimicDataTester extends Runnable with SparkApplication {
  @Override
  def run(): Unit = {
    spark.sql("USE DATABASE mimic")

    spark.sql(
      """  WITH expected AS (
        |       SELECT 'admissions' AS tbl, 58976 AS row_count UNION ALL
        |       SELECT 'callout' AS tbl, 34499 AS row_count UNION ALL
        |       SELECT 'caregivers' AS tbl, 7567 AS row_count UNION ALL
        |       SELECT 'chartevents' AS tbl, 330712483 AS row_count UNION ALL
        |       SELECT 'cptevents' AS tbl, 573146 AS row_count UNION ALL
        |       SELECT 'd_cpt' AS tbl, 134 AS row_count UNION ALL
        |       SELECT 'd_icd_diagnoses' AS tbl, 14567 AS row_count UNION ALL
        |       SELECT 'd_icd_procedures' AS tbl, 3882 AS row_count UNION ALL
        |       SELECT 'd_items' AS tbl, 12487 AS row_count UNION ALL
        |       SELECT 'd_labitems' AS tbl, 753 AS row_count UNION ALL
        |       SELECT 'datetimeevents' AS tbl, 4485937 AS row_count UNION ALL
        |       SELECT 'diagnoses_icd' AS tbl, 651047 AS row_count UNION ALL
        |       SELECT 'drgcodes' AS tbl, 125557 AS row_count UNION ALL
        |       SELECT 'icustays' AS tbl, 61532 AS row_count UNION ALL
        |       SELECT 'inputevents_cv' AS tbl, 17527935 AS row_count UNION ALL
        |       SELECT 'inputevents_mv' AS tbl, 3618991 AS row_count UNION ALL
        |       SELECT 'labevents' AS tbl, 27854055 AS row_count UNION ALL
        |       SELECT 'microbiologyevents' AS tbl, 631726 AS row_count UNION ALL
        |       SELECT 'noteevents' AS tbl, 2083180 AS row_count UNION ALL
        |       SELECT 'outputevents' AS tbl, 4349218 AS row_count UNION ALL
        |       SELECT 'patients' AS tbl, 46520 AS row_count UNION ALL
        |       SELECT 'prescriptions' AS tbl, 4156450 AS row_count UNION ALL
        |       SELECT 'procedureevents_mv' AS tbl, 258066 AS row_count UNION ALL
        |       SELECT 'procedures_icd' AS tbl, 240095 AS row_count UNION ALL
        |       SELECT 'services' AS tbl, 73343 AS row_count UNION ALL
        |       SELECT 'transfers' AS tbl, 261897 AS row_count
        |       ),
        |       observed AS (
        |       SELECT 'admissions' AS tbl, count(*) AS row_count from admissions UNION ALL
        |       SELECT 'callout' AS tbl, count(*) AS row_count from callout UNION ALL
        |       SELECT 'caregivers' AS tbl, count(*) AS row_count from caregivers UNION ALL
        |       SELECT 'chartevents' AS tbl, count(*) AS row_count from chartevents UNION ALL
        |       SELECT 'cptevents' AS tbl, count(*) AS row_count from cptevents UNION ALL
        |       SELECT 'd_cpt' AS tbl, count(*) AS row_count from d_cpt UNION ALL
        |       SELECT 'd_icd_diagnoses' AS tbl, count(*) AS row_count from d_icd_diagnoses UNION ALL
        |       SELECT 'd_icd_procedures' AS tbl, count(*) AS row_count from d_icd_procedures UNION ALL
        |       SELECT 'd_items' AS tbl, count(*) AS row_count from d_items UNION ALL
        |       SELECT 'd_labitems' AS tbl, count(*) AS row_count from d_labitems UNION ALL
        |       SELECT 'datetimeevents' AS tbl, count(*) AS row_count from datetimeevents UNION ALL
        |       SELECT 'diagnoses_icd' AS tbl, count(*) AS row_count from diagnoses_icd UNION ALL
        |       SELECT 'drgcodes' AS tbl, count(*) AS row_count from drgcodes UNION ALL
        |       SELECT 'icustays' AS tbl, count(*) AS row_count from icustays UNION ALL
        |       SELECT 'inputevents_cv' AS tbl, count(*) AS row_count from inputevents_cv UNION ALL
        |       SELECT 'inputevents_mv' AS tbl, count(*) AS row_count from inputevents_mv UNION ALL
        |       SELECT 'labevents' AS tbl, count(*) AS row_count from labevents UNION ALL
        |       SELECT 'microbiologyevents' AS tbl, count(*) AS row_count from microbiologyevents UNION ALL
        |       SELECT 'noteevents' AS tbl, count(*) AS row_count from noteevents UNION ALL
        |       SELECT 'outputevents' AS tbl, count(*) AS row_count from outputevents UNION ALL
        |       SELECT 'patients' AS tbl, count(*) AS row_count from patients UNION ALL
        |       SELECT 'prescriptions' AS tbl, count(*) AS row_count from prescriptions UNION ALL
        |       SELECT 'procedureevents_mv' AS tbl, count(*) AS row_count from procedureevents_mv UNION ALL
        |       SELECT 'procedures_icd' AS tbl, count(*) AS row_count from procedures_icd UNION ALL
        |       SELECT 'services' AS tbl, count(*) AS row_count from services UNION ALL
        |       SELECT 'transfers' AS tbl, count(*) AS row_count from transfers
        |       )
        |SELECT exp.tbl,
        |       exp.row_count AS expected_count,
        |       obs.row_count AS observed_count,
        |       CASE
        |       WHEN exp.row_count = obs.row_count THEN 'PASSED'
        |       ELSE 'FAILED'
        |       END AS ROW_COUNT_CHECK
        |  FROM expected AS exp
        |       INNER JOIN observed AS obs
        |       ON exp.tbl = obs.tbl
        | ORDER BY exp.tbl;""".stripMargin)
  }
}

object MimicDataTester {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new MimicDataTester(), System.err, args: _*)
  }
}

