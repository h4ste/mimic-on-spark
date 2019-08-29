package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.{JamiaTables, MedianAggregateFunction}
import org.apache.logging.log4j.{LogManager, Logger}
import picocli.CommandLine
import picocli.CommandLine.Option

class HaAkiExtractor extends LabelExtractor {

  @Option(names = Array("-b", "--baseline"),
    description = Array("type of creatinine baseline to use when detecting AKIs"))
  var akiBaseline: AkiBaseline = AkiBaseline.MEDIAN

  val logger: Logger = HaAkiExtractor.logger

  @Override
  def run(): Unit = {
    spark.udf.register("convert_weight",
      (itemId: Int, valueNum: Double) => itemId match {
        case 3581 | 226531 => valueNum * 0.45359237
        case 3582 => valueNum * 0.0283495231
        case _ => valueNum
      })

    spark.udf.register("median", new MedianAggregateFunction)

    logger.debug("Reading note labels")
    readNoteLabels(notePath,"note_dx")

    spark.sql("CREATE DATABASE IF NOT EXISTS jamia")

    val creatinineBaseline = akiBaseline.name().toLowerCase()

    if (recreate) {
      logger.debug("Dropping previous tables")
      spark.sql("DROP TABLE IF EXISTS jamia.urine_stats")
      spark.sql("DROP TABLE IF EXISTS jamia.akis")
      spark.sql(s"DROP TABLE IF EXISTS jamia.running_aki_stages_$creatinineBaseline")
    }

    // Create temporary view for patient weights
    logger.debug("Creating patients weight view")
    spark.sql(
      s"""CREATE TEMPORARY VIEW patient_weights AS
         |SELECT row_id,
         |       subject_id,
         |       hadm_id,
         |       charttime,
         |       weight,
         |       median(weight) OVER (PARTITION BY subject_id, hadm_id ORDER BY charttime ASC, row_id ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS median3
         |  FROM (SELECT *,
         |               convert_weight(itemid, valuenum) AS weight
         |          FROM mimic.chartevents
         |         WHERE itemid IN (
         |               -- weight measurements from CareVue
         |               762, -- "Admit Wt"
         |               763, -- "Daily Weight"
         |               3580, -- "Present Weight (kg)"
         |               3581, -- "Present Weight (lb)"
         |               3582, -- "Present Weight (oz)",
         |               3693, -- "Weight Kg"
         |               -- weight measurements from MetaVision
         |               224639, -- "Daily Weight" (kg)
         |               226512, -- "Admission Weight (kg)"
         |               226531 -- "Admission Weight (lbs.)"
         |               )
         |           AND valuenum IS NOT NULL
         |           AND valueuom NOT IN ("cm", "cmH20")
         |           AND (error IS NULL or error <> 1)
         |       ) AS vkg""".stripMargin
    )

    // Create view for urine outputs over thresholds
    logger.debug("Creating urine outputs view")
    spark.sql(
      s"""CREATE TEMPORARY VIEW urine_volume AS
         |SELECT oe.subject_id,
         |       oe.hadm_id,
         |       oe.charttime,
         |       SUM(oe.value) AS urine
         |  FROM mimic.outputevents AS oe
         | WHERE oe.itemid IN (
         |       -- most frequently occurring urine output observations in CareVue
         |       40055, -- "Urine Out Foley"
         |       43175, -- "Urine ."
         |       40069, -- "Urine Out Void"
         |       40094, -- "Urine Out Condom Cath"
         |       40715, -- "Urine Out Suprapubic"
         |       40473, -- "Urine Out IleoConduit"
         |       40085, -- "Urine Out Incontinent"
         |       40057, -- "Urine Out Rt Nephrostomy"
         |       40056, -- "Urine Out Lt Nephrostomy"
         |       40405, -- "Urine Out Other"
         |       40428, -- "Urine Out Straight Cath"
         |       40086, -- "Urine Out Incontinent"
         |       40096, -- "Urine Out Ureteral Stent #1"
         |       40651, -- "Urine Out Ureteral Stent #2"
         |       -- most frequently occurring urine output observations in MetaVision
         |       226559, -- "Foley"
         |       226560, -- "Void"
         |       226561, -- "Condom Cath"
         |       226584, -- "Ileoconduit"
         |       226563, -- "Suprapubic"
         |       226564, -- "R Nephrostomy"
         |       226565, -- "L Nephrostomy"
         |       226567, -- "Straight Cath"
         |       226557, -- "R Ureteral Stent"
         |       226558 -- "L Ureteral Stent"
         |       )
         |   AND oe.hadm_id IS NOT NULL
         |   AND (oe.iserror IS NULL or oe.iserror <> 1)
         | GROUP BY oe.subject_id, oe.hadm_id, oe.charttime
         | ORDER BY oe.subject_id ASC, oe.hadm_id ASC, oe.charttime ASC""".stripMargin
    )

    logger.info("Creating persistent table: jamia.urine_stats")
    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.urine_stats AS
        |SELECT pu.subject_id,
        |       pu.hadm_id,
        |       pu.charttime,
        |       FIRST(pu.charttime) OVER  6h AS first_6h,
        |       FIRST(pu.charttime) OVER 12h AS first_12h,
        |       FIRST(pu.charttime) OVER 24h AS first_24h,
        |       MAX(unix_timestamp(pu.charttime) - unix_timestamp(pu.prev)) OVER  6h AS max_lag_6h,
        |       MAX(unix_timestamp(pu.charttime) - unix_timestamp(pu.prev)) OVER 12h AS max_lag_12h,
        |       MAX(unix_timestamp(pu.charttime) - unix_timestamp(pu.prev)) OVER 24h AS max_lag_24h,
        |       MAX(pu.tot_1h / pw1.median3) OVER  6h AS max_wnrate_6h,
        |       MAX(pu.tot_1h / pw1.median3) OVER 12h AS max_wnrate_12h,
        |       MAX(pu.tot_1h / pw1.median3) OVER 24h AS max_wnrate_24h
        |  FROM (SELECT u.*,
        |               SUM(u.urine) OVER (PARTITION BY u.subject_id, u.hadm_id ORDER BY u.charttime ASC RANGE BETWEEN INTERVAL 1 HOURS PRECEDING AND CURRENT ROW) AS tot_1h,
        |               LAG(u.charttime) OVER (PARTITION BY u.subject_id, u.hadm_id ORDER BY u.charttime ASC) AS prev
        |          FROM urine_volume AS u
        |       ) AS pu
        |       INNER JOIN patient_weights AS pw1
        |            ON pu.subject_id = pw1.subject_id
        |            AND pu.hadm_id = pw1.hadm_id
        |            AND pu.charttime >= pw1.charttime
        |       LEFT OUTER JOIN patient_weights AS pw2
        |            ON pu.subject_id = pw2.hadm_id
        |            AND pu.hadm_id = pw2.hadm_id
        |            AND pu.charttime >= pw1.charttime
        |            AND (pw1.charttime < pw2.charttime
        |                 OR (pw1.charttime < pw2.charttime
        |                     AND pw1.row_id < pw2.row_id))
        | WHERE (pw2.subject_id IS NULL OR pw2.hadm_id IS NULL)
        |WINDOW  6h AS (PARTITION BY pu.subject_id, pu.hadm_id ORDER BY pu.charttime ASC RANGE BETWEEN INTERVAL  6 HOURS PRECEDING AND CURRENT ROW),
        |       12h AS (PARTITION BY pu.subject_id, pu.hadm_id ORDER BY pu.charttime ASC RANGE BETWEEN INTERVAL 12 HOURS PRECEDING AND CURRENT ROW),
        |       24h AS (PARTITION BY pu.subject_id, pu.hadm_id ORDER BY pu.charttime ASC RANGE BETWEEN INTERVAL 24 HOURS PRECEDING AND CURRENT ROW)""".stripMargin
    )

    logger.debug("Creating creatinines view")
    // Creatinine lab values
    spark.sql(
      """CREATE TEMPORARY VIEW creatinines AS
        |SELECT subject_id,
        |        hadm_id,
        |       charttime,
        |       valuenum AS creatinine
        |  FROM mimic.labevents AS labs
        | WHERE itemid = 50912 -- Creatinine
        |   AND valuenum IS NOT NULL
        |   AND hadm_id IS NOT NULL""".stripMargin
    )
    sql.cacheTable("creatinines")

    logger.debug("Creating creatinine baseline: view {}", akiBaseline)
    akiBaseline match {
      case AkiBaseline.NADIR =>
        // Creatinine 48h_nadirs
        spark.sql(
          """CREATE TEMPORARY VIEW creatinine_nadir_48h AS
            |SELECT labs.subject_id,
            |       labs.hadm_id,
            |   	  MIN(labs.creatinine) AS baseline
            |  FROM creatinines AS labs
            |  	    INNER JOIN mimic.admissions AS hadms
            |       ON labs.subject_id = hadms.subject_id
            |  	    AND labs.hadm_id = hadms.hadm_id
            | WHERE labs.charttime BETWEEN hadms.admittime - INTERVAL 24 HOURS AND hadms.admittime + INTERVAL 24 HOURS
            | GROUP BY labs.subject_id, labs.hadm_id""".stripMargin
        )

      case AkiBaseline.MAXIMUM =>
        // Creatinine 48h_maximum
        spark.sql(
          """CREATE TEMPORARY VIEW creatinine_maximum_48h AS
            |SELECT labs.subject_id,
            |       labs.hadm_id,
            |   	  MAX(labs.creatinine) AS baseline
            |  FROM creatinines AS labs
            |  	    INNER JOIN mimic.admissions AS hadms
            |       ON labs.subject_id = hadms.subject_id
            |  	    AND labs.hadm_id = hadms.hadm_id
            | WHERE labs.charttime BETWEEN hadms.admittime - INTERVAL 24 HOURS AND hadms.admittime + INTERVAL 24 HOURS
            | GROUP BY labs.subject_id, labs.hadm_id""".stripMargin
        )

      case AkiBaseline.MEDIAN =>
        // Creatinine 48h_median
        spark.sql(
          """CREATE TEMPORARY VIEW creatinine_median_48h AS
            |SELECT labs.subject_id,
            |       labs.hadm_id,
            |   	  median(labs.creatinine) AS baseline
            |  FROM creatinines AS labs
            |  	    INNER JOIN mimic.admissions AS hadms
            |       ON labs.subject_id = hadms.subject_id
            |  	    AND labs.hadm_id = hadms.hadm_id
            | WHERE labs.charttime BETWEEN hadms.admittime - INTERVAL 24 HOURS AND hadms.admittime + INTERVAL 24 HOURS
            | GROUP BY labs.subject_id, labs.hadm_id""".stripMargin
        )
      case AkiBaseline.FIRST =>
        // Creatinine 48h_first
        spark.sql(
          """CREATE TEMPORARY VIEW creatinine_first_48h AS
            |  WITH ordered_creatinines AS (
            |SELECT labs.subject_id,
            |       labs.hadm_id,
            |       labs.creatinine,
            |   	  ROW_NUMBER() OVER (PARTITION BY labs.subject_id, labs.hadm_id ORDER BY labs.charttime ASC) AS rank
            |  FROM creatinines AS labs
            |  	    INNER JOIN mimic.admissions AS hadms
            |       ON labs.subject_id = hadms.subject_id
            |  	    AND labs.hadm_id = hadms.hadm_id
            | WHERE labs.charttime BETWEEN hadms.admittime - INTERVAL 24 HOURS AND hadms.admittime + INTERVAL 24 HOURS
            |       )
            |
            |SELECT subject_id,
            |       hadm_id,
            |       creatinine AS baseline
            |  FROM ordered_creatinines
            | WHERE rank = 1""".stripMargin
        )
      case x => throw new IllegalStateException("Encountered unexpected AkiBaseline value " + x)
    }
    val creatinineBaselineTable = s"creatinine_${creatinineBaseline}_48h"

    // Creatinine increases
    logger.debug("Creating creatinine increase view")
    spark.sql(
      """CREATE TEMPORARY VIEW creatinine_increases AS
        |SELECT subject_id,
        |       hadm_id,
        |       charttime,
        |       creatinine - MIN(creatinine) OVER (PARTITION BY subject_id, hadm_id ORDER BY charttime ASC RANGE BETWEEN INTERVAL 48 HOURS PRECEDING AND CURRENT ROW) AS increase
        |  FROM creatinines""".stripMargin
    )

    // Create permanent table to store AKI labels
    logger.info(s"Creating persistent table: jamia.akis_$creatinineBaseline")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS jamia.akis_$creatinineBaseline AS
         |  WITH aki_details AS (
         |SELECT subject_id,
         |       hadm_id,
         |       charttime,
         |       creatinine >= 3.0 * baseline AS stage3_cr,
         |       (max_wnrate_24h < 0.3 AND charttime > first_24h + INTERVAL 23 HOURS AND max_lag_24h < 5400)
         |       OR (max_wnrate_12h == 0.0 AND charttime > first_12h + INTERVAL 11 HOURS AND max_lag_12h < 5400) AS stage3_wnur,
         |       creatinine >= 4.0 AS stage3_cr_thresh,
         |       increase >= 0.5 AS stage3_cr_inc,
         |       creatinine >= 2.0 * baseline AS stage2_cr,
         |       (max_wnrate_12h < 0.5 AND charttime > first_12h + INTERVAL 11 HOURS AND max_lag_12h < 5400) AS stage2_wnur,
         |       creatinine >= 1.5 * baseline AS stage1_cr,
         |       (max_wnrate_6h  < 0.5 AND charttime > first_6h  + INTERVAL  5 HOURS AND max_lag_6h  < 5400) AS stage1_wnur,
         |       increase >= 0.3 AS stage1_cr_inc
         |  FROM creatinines AS c
         |       LEFT JOIN $creatinineBaselineTable
         |       USING (subject_id, hadm_id)
         |       LEFT OUTER JOIN creatinine_increases AS ci
         |       USING (subject_id, hadm_id, charttime)
         |       FULL OUTER JOIN jamia.urine_stats AS uo
         |       USING (subject_id, hadm_id, charttime)
         |       )
         |
         |SELECT aki_details.*,
         |       CASE
         |       WHEN stage3_cr OR stage3_wnur OR (stage3_cr_thresh AND stage3_cr_inc) THEN 3
         |       WHEN stage2_cr OR stage2_wnur THEN 2
         |       WHEN stage1_cr OR stage1_wnur OR stage1_cr_inc THEN 1
         |       ELSE 0
         |       END AS aki_risk
         |  FROM aki_details""".stripMargin
    )

    logger.info(s"Creating persistent table: jamia.running_aki_stages_$creatinineBaseline")
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS jamia.running_aki_stages_$creatinineBaseline AS
         |SELECT subject_id,
         |       hadm_id,
         |       charttime,
         |       MAX(aki_risk) OVER (PARTITION BY subject_id, hadm_id ORDER BY charttime ASC) AS aki_stage
         |  FROM jamia.akis_$creatinineBaseline
         | WHERE charttime IS NOT NULL""".stripMargin
    )

    // Temporary view for patients with chronic renal failure
    logger.debug("Creating CRF patient view")
    spark.sql(
      """CREATE TEMPORARY VIEW crf_patients AS
        |SELECT subject_id,
        |       hadm_id
        |  FROM mimic.diagnoses_icd
        | WHERE icd9_code IN (
        |       '585', '5851', '5852', '5853', '5854', '5855', '5856', '5859', '7925', 'V420', 'V451', 'V4511', 'V4512', 'V560', 'V561', 'V562', 'V5631', 'V5632', 'V568'
        |       )
      """.stripMargin
    )

    logger.info("Creating persistent table: jamia.adults")
    spark.sql(s"CREATE TABLE IF NOT EXISTS jamia.adults AS\n${JamiaTables.adultQuery}")

    logger.debug("Creating HAAKI label view")
    val haAkiLabels = make_labels(
      """SELECT n.subject_id,
        |       n.hadm_id,
        |       MAX(n.label) AS label
        |  FROM note_dx AS n
        |       LEFT OUTER JOIN
        |       (SELECT subject_id,
        |               hadm_id
        |          FROM note_dx
        |               INNER JOIN mimic.admissions
        |               USING (subject_id, hadm_id)
        |         WHERE label
        |           AND timestamp <= admittime + INTERVAL 48 HOURS
        |       ) AS t
        |       ON n.subject_id = t.subject_id
        |       AND n.hadm_id = t.hadm_id
        | WHERE t.hadm_id IS NULL
        |    OR t.subject_id IS NULL
        | GROUP BY n.subject_id, n.hadm_id""".stripMargin,
      s"""SELECT r.subject_id,
         |       r.hadm_id,
         |       MAX(r.aki_stage) >= 1 AS label
         |  FROM jamia.running_aki_stages_$creatinineBaseline AS r
         |       LEFT OUTER JOIN
         |       (SELECT subject_id,
         |               hadm_id
         |          FROM jamia.running_aki_stages_$creatinineBaseline
         |               INNER JOIN mimic.admissions
         |               USING (subject_id, hadm_id)
         |         WHERE aki_stage >= 1
         |           AND charttime <= admittime + INTERVAL 48 HOURS
         |       ) AS t
         |       ON r.subject_id = t.subject_id
         |       AND r.hadm_id = t.hadm_id
         | WHERE t.hadm_id IS NULL
         |    OR t.subject_id IS NULL
         | GROUP BY r.subject_id, r.hadm_id""".stripMargin,
      """SELECT a.subject_id,
        |       a.hadm_id,
        |       MAX(icd9_code IN ('5845', '5846', '5847', '5848', '5849')) AS label
        |  FROM mimic.diagnoses_icd AS a
        |       LEFT OUTER JOIN crf_patients AS c
        |       ON a.subject_id = c.subject_id
        |       AND a.hadm_id = c.hadm_id
        | WHERE (c.subject_id IS NULL OR c.hadm_id IS NULL)
        | GROUP BY a.subject_id, a.hadm_id""".stripMargin
    )
    haAkiLabels.createOrReplaceTempView("haaki_labels")
    haAkiLabels.cache()

    logger.info("Writing haaki_label_details.csv")
    write_csv(
      haAkiLabels,
      "haaki_label_details.csv"
    )

    logger.info("Writing haaki_label_dist.csv")
    write_csv(
      make_label_stats(haAkiLabels),
      "haaki_label_dist.csv"
    )

    logger.info("Writing aki_stages.csv")
    write_csv(
      spark.sql(
        s"""SELECT subject_id,
           |       hadm_id,
           |       MIN(charttime) AS timestamp,
           |       aki_stage AS label
           |  FROM jamia.running_aki_stages_$creatinineBaseline
           | WHERE aki_stage >= 1
           | GROUP BY subject_id, hadm_id, aki_stage
           | ORDER BY subject_id ASC, hadm_id ASC, timestamp ASC""".stripMargin
      ),
      "aki_stages.csv"
    )

    logger.info("Writing haaki_stages.csv")
    write_csv(
      spark.sql(
        s"""SELECT r.subject_id,
           |       r.hadm_id,
           |       MIN(r.charttime) AS timestamp,
           |       r.aki_stage AS label
           |  FROM jamia.running_aki_stages_$creatinineBaseline AS r
           |       LEFT OUTER JOIN
           |       (SELECT subject_id,
           |               hadm_id
           |          FROM jamia.running_aki_stages_$creatinineBaseline
           |               INNER JOIN mimic.admissions
           |               USING (subject_id, hadm_id)
           |         WHERE aki_stage >= 1
           |           AND charttime <= admittime + INTERVAL 48 HOURS
           |       ) AS t
           |       ON r.subject_id = t.subject_id
           |       AND r.hadm_id = t.hadm_id
           | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
           |   AND aki_stage >= 1
           | GROUP BY r.subject_id, r.hadm_id, r.aki_stage
           | ORDER BY r.subject_id ASC, r.hadm_id ASC, timestamp ASC""".stripMargin
      ),
      "haaki_stages.csv"
    )

    val noteLabels = spark.sql(
      """SELECT n.subject_id,
        |       n.hadm_id,
        |       MIN(n.timestamp) AS timestamp
        |  FROM note_dx AS n
        |       LEFT OUTER JOIN
        |       (SELECT subject_id,
        |               hadm_id
        |          FROM note_dx
        |               INNER JOIN mimic.admissions
        |               USING (subject_id, hadm_id)
        |         WHERE label
        |           AND timestamp <= admittime + INTERVAL 48 HOURS
        |       ) AS t
        |       ON n.subject_id = t.subject_id
        |       AND n.hadm_id = t.hadm_id
        | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
        |   AND n.label
        |   AND n.hadm_id IS NOT NULL
        | GROUP BY n.subject_id, n.hadm_id""".stripMargin
    )
    noteLabels.createOrReplaceTempView("note_labels")
    noteLabels.cache()
    write_csv(noteLabels, "haaki_positive_note_labels.csv")

    val tableLabels = spark.sql(
      s"""SELECT r.subject_id,
         |       r.hadm_id,
         |       MIN(r.charttime) AS timestamp
         |  FROM jamia.running_aki_stages_$creatinineBaseline AS r
         |       LEFT OUTER JOIN
         |       (SELECT subject_id,
         |               hadm_id
         |          FROM jamia.running_aki_stages_$creatinineBaseline
         |               INNER JOIN mimic.admissions
         |               USING (subject_id, hadm_id)
         |         WHERE aki_stage >= 1
         |           AND charttime <= admittime + INTERVAL 48 HOURS
         |       ) AS t
         |       ON r.subject_id = t.subject_id
         |       AND r.hadm_id = t.hadm_id
         | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
         |   AND r.hadm_id IS NOT NULL
         |   AND aki_stage >= 1
         | GROUP BY r.subject_id, r.hadm_id""".stripMargin
    )
    tableLabels.cache()
    tableLabels.createOrReplaceTempView("table_labels")
    write_csv(tableLabels, "haaki_positive_table_labels.csv")

    logger.info("Writing haaki_positive_labels.csv")
    write_csv(
      spark.sql(
        """  WITH label_union AS (
          |SELECT * FROM note_labels
          | UNION ALL
          |SELECT * FROM table_labels
          |       )
          |
          |SELECT subject_id,
          |       hadm_id,
          |       MIN(timestamp) AS timestamp,
          |       1 AS label
          |  FROM jamia.adults AS a
          |       INNER JOIN label_union AS u
          |       USING (subject_id, hadm_id)
          | WHERE a.hadm_id IS NOT NULL
          |   AND timestamp IS NOT NULL
          | GROUP BY a.subject_id, a.hadm_id""".stripMargin
      ),
      "haaki_positive_labels.csv"
    )

    logger.info("Writing aki_label_dist.csv")
    val akiLabels = make_labels(
      """SELECT subject_id,
        |       hadm_id,
        |       MAX(label) AS label
        |  FROM note_dx
        | GROUP BY subject_id, hadm_id""".stripMargin,
      s"""SELECT subject_id,
         |       hadm_id,
         |       MAX(aki_stage) >= 1 AS label
         |  FROM jamia.running_aki_stages_$creatinineBaseline
         | GROUP BY subject_id, hadm_id""".stripMargin,
      """SELECT a.subject_id,
        |       a.hadm_id,
        |       MAX(icd9_code IN ('5845', '5846', '5847', '5848', '5849')) AS label
        |  FROM mimic.diagnoses_icd AS a
        |       LEFT OUTER JOIN crf_patients AS c
        |       ON a.subject_id = c.subject_id
        |       AND a.hadm_id = c.hadm_id
        | WHERE (c.subject_id IS NULL OR c.hadm_id IS NULL)
        | GROUP BY a.subject_id, a.hadm_id""".stripMargin
    )
    akiLabels.createOrReplaceTempView("aki_labels")
    akiLabels.cache()

    write_csv(
      akiLabels,
      "aki_label_dist.csv"
    )

    logger.info("Writing haaki_hadms.csv")
    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id
          |  FROM haaki_labels
          | WHERE (note_dx OR table_dx)
          |   AND hadm_id IS NOT NULL""".stripMargin
      ),
      "haaki_hadms.csv"
    )

    logger.info("Writing noaki_hadms.csv")
    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id
          |  FROM aki_labels
          | WHERE NOT note_dx
          |   AND NOT table_dx
          |   AND NOT icd_dx
          |   AND hadm_id IS NOT NULL""".stripMargin
      ),
      "noaki_hadms.csv"
    )
  }
}

object HaAkiExtractor {
  protected val logger: Logger = LogManager.getLogger(classOf[HaAkiExtractor])

  def main(args: Array[String]): Unit = {
    CommandLine.run(new HaAkiExtractor(), System.err, args: _*)
  }
}
