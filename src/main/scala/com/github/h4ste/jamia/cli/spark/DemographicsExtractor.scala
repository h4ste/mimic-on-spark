package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.SparkApplication
import picocli.CommandLine
import picocli.CommandLine.Parameters

class DemographicsExtractor extends Runnable with SparkApplication {

  @Parameters(index = "0",
    description = Array("directory to save demographics CSV"),
    paramLabel = "PATH")
  var outputPath: String = _

  spark.sql("DROP TABLE IF EXISTS jamia.hadm_demographics")

  @Override
  def run(): Unit = {
    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.hadm_info AS
        |  WITH icu_stats AS (
        |SELECT subject_id,
        |       hadm_id,
        |       intime AS icutime,
        |       first_careunit,
        |       oasis,
        |       oasis_prob,
        |       ROW_NUMBER() OVER (PARTITION BY subject_id, hadm_id ORDER BY intime ASC, row_id ASC) AS rank
        |  FROM mimic.icustays
        |       INNER JOIN concepts.oasis
        |       USING (subject_id, hadm_id, icustay_id)
        |       )
        |
        |SELECT subject_id,
        |       hadm_id,
        |       DATEDIFF(admittime, dob) / 365.25 AS age,
        |       gender,
        |       admittime,
        |       icutime,
        |       first_careunit AS icu,
        |       oasis,
        |       CASE WHEN ethnicity LIKE 'ASIAN%' THEN 'ASIAN'
        |            WHEN ethnicity LIKE 'BLACK%' THEN 'BLACK'
        |            WHEN ethnicity LIKE 'HISPANIC%' OR ethnicity = 'SOUTH AMERICAN' THEN 'HISPANIC/LATINO'
        |            WHEN ethnicity LIKE 'WHITE%' THEN 'WHITE'
        |            WHEN ethnicity IN ('UNABLE TO OBTAIN', 'UNKNOWN/NOT SPECIFIED', 'PATIENT DECLINED TO ANSWER') THEN 'UNKNOWN'
        |            ELSE 'OTHER'
        |       END as ethnicity,
        |       insurance,
        |       admission_type,
        |       CASE WHEN admission_location = 'CLINIC REFERRAL/PREMATURE' THEN 'CLINIC REFERRAL'
        |            WHEN admission_location = 'PHYS REFERRAL/NORMAL DELI' THEN 'PHYS REFERRAL'
        |            WHEN admission_location = 'TRANSFER FROM HOSP/EXTRAM' THEN 'TRANSFER FROM HOSP'
        |            ELSE 'OTHER'
        |       END AS admission_location
        |
        |  FROM mimic.patients
        |       INNER JOIN mimic.admissions
        |       USING (subject_id)
        |
        |       LEFT OUTER JOIN icu_stats
        |       USING (subject_id, hadm_id)
        | WHERE hadm_id IS NOT NULL
        |   AND rank == 1""".stripMargin
    )

    saveToCsv(spark.sql("SELECT * FROM jamia.hadm_info"), outputPath)
  }
}

object DemographicsExtractor {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new DemographicsExtractor(), System.err, args: _*)
  }
}

