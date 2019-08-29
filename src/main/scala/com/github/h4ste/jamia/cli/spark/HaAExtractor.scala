package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.JamiaTables
import picocli.CommandLine
import picocli.CommandLine.Option

class HaAExtractor extends LabelExtractor {

  @Option(names = Array("-c", "--allow-chart-hgb"),
    description = Array("include hemoglobin measurements from the chart as well as from the labs"))
  var chartHemoglobin: Boolean = false

  @Override
  def run(): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS jamia")

    if (recreate) {
      spark.sql("DROP TABLE IF EXISTS jamia.ages")
      spark.sql("DROP TABLE IF EXISTS jamia.adults")
      spark.sql("DROP TABLE IF EXISTS jamia.pregnancies")
      spark.sql("DROP TABLE IF EXISTS jamia.anemia_severity")
      spark.sql("DROP TABLE IF EXISTS jamia.running_anemia_severity")
      spark.sql("DROP TABLE IF EXISTS jamia.haa_patients")
      spark.sql("DROP TABLE IF EXISTS jamia.noa_patients")
    }

    if (chartHemoglobin) {
      spark.sql(
        """CREATE TEMPORARY VIEW hgb AS
          |SELECT *
          |  FROM (SELECT subject_id,
          |               hadm_id,
          |               charttime,
          |               ROUND(valuenum, 1) as hgb
          |          FROM mimic.chartevents
          |         WHERE itemid IN (814, 220228)
          |           AND (error IS NULL OR error <> 1)
          |           AND valuenum IS NOT NULL
          |           AND valuenum > 0
          |       )
          |       UNION
          |       (SELECT subject_id,
          |               hadm_id,
          |               charttime,
          |               ROUND(valuenum, 1) as hgb
          |          FROM mimic.labevents
          |         WHERE itemid IN (50811, 51222)
          |           AND valuenum IS NOT NULL
          |           AND valuenum > 0
          |       )""".stripMargin
      )
    } else {
      spark.sql(
        """CREATE TEMPORARY VIEW hgb AS
          |SELECT subject_id,
          |       hadm_id,
          |       charttime,
          |       ROUND(valuenum, 1) as hgb
          |  FROM mimic.labevents
          | WHERE itemid IN (50811, 51222)
          |   AND valuenum IS NOT NULL
          |   AND valuenum > 0""".stripMargin
      )
    }

    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.pregnancies AS
        |SELECT subject_id,
        |       hadm_id,
        |       ccs_id IN (
        |            180, -- Ectopic pregnancy
        |            181, -- Other complications of pregnancy
        |            182, -- Hemorrhage during pregnancy; abruptio placenta; placenta previa
        |            183, -- Hypertension complicating pregnancy; childbirth and the puerperium
        |            184, -- Early or threatened labor
        |            185, -- Prolonged pregnancy
        |            186, --  Diabetes or abnormal glucose tolerance complicating pregnancy; childbirth; or the puerperium
        |            187, -- Malposition; malpresentation
        |            188, --  Fetopelvic disproportion; obstruction
        |            190, --  Fetal distress and abnormal forces of labor
        |            191, --  Polyhydramnios and other problems of amniotic cavity
        |            192, --  Umbilical cord complication
        |            193, --  OB-related trauma to perineum and vulva
        |            194, --  Forceps delivery
        |            195, --  Other complications of birth; puerperium affecting management of mother
        |            196  -- Other pregnancy and delivery including normal
        |        ) AS is_pregnant
        |  FROM ccs_dx.ccs_single_level
        |       NATURAL JOIN mimic.diagnoses_icd""".stripMargin
    )

    spark.sql(s"CREATE TABLE IF NOT EXISTS jamia.ages AS ${JamiaTables.ageQuery}")

    // Taken from https://www.who.int/vmnis/indicators/haemoglobin.pdf
    spark.udf.register("calc_anemia_severity",
      (age: Int, gender: String, pregnant: Boolean, hgb: Double) => {
        assert(age >= 0)
        assert(gender == "F" || gender == "M")
        assert(hgb > 0.0)
        if (age <= 4) {
          assert(!pregnant)
          // Children 6-49 months of age
          if (hgb < 7) 3
          else if (hgb < 9.9) 2
          else if (hgb < 10.9) 1
          else 0
        } else if (age <= 11) {
          assert(!pregnant)
          // Children 5 - 11 years of age
          if (hgb < 8) 3
          else if (hgb < 10.9) 2
          else if (hgb < 11.4) 1
          else 0
        } else if (age <= 14 || (!pregnant && gender == "F")) {
          // Children 12 - 14 years of age, or non-pregnant women (15 years of age and above)
          if (hgb < 8) 3
          else if (hgb < 10.9) 2
          else if (hgb < 11.9) 1
          else 0
        } else if (pregnant) {
          // Pregnant women
          assert(gender == "F")
          if (hgb < 7) 3
          else if (hgb < 9.9) 2
          else if (hgb < 10.9) 1
          else 0
        } else {
          // Men (15 years of age and above)
          assert(gender == "M")
          assert(age >= 15)
          assert(!pregnant)
          if (hgb < 8) 3
          else if (hgb < 10.9) 2
          else if (hgb < 12.9) 1
          else 0
        }
      }
    )

    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.anemia_severity AS
        |SELECT subject_id,
        |       hadm_id,
        |       charttime,
        |       age,
        |       gender,
        |       is_pregnant,
        |       hgb,
        |       calc_anemia_severity(age, gender, is_pregnant, hgb) AS anemia_severity
        |  FROM hgb
        |       NATURAL JOIN jamia.ages
        |       NATURAL JOIN jamia.pregnancies
        |       INNER JOIN mimic.patients
        |       USING (subject_id)""".stripMargin
    )


    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.running_anemia_severity AS
        |SELECT subject_id,
        |       hadm_id,
        |       charttime,
        |       age,
        |       gender,
        |       is_pregnant,
        |       hgb,
        |       MAX(anemia_severity) OVER (PARTITION BY subject_id, hadm_id ORDER BY charttime ASC) AS anemia_severity
        |  FROM jamia.anemia_severity
        | WHERE charttime IS NOT NULL""".stripMargin
    )

    spark.sql(s"CREATE TABLE IF NOT EXISTS jamia.adults AS\n${JamiaTables.adultQuery}")

    readNoteLabels(
      notePath,
      "note_dx"
    )

    val labels = make_labels(
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
        | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
        | GROUP BY n.subject_id, n.hadm_id""".stripMargin,
      """SELECT r.subject_id,
        |       r.hadm_id,
        |       MAX(r.anemia_severity) >= 1 AS label
        |  FROM jamia.running_anemia_severity AS r
        |       LEFT OUTER JOIN
        |       (SELECT subject_id,
        |               hadm_id
        |          FROM jamia.running_anemia_severity
        |               INNER JOIN mimic.admissions
        |               USING (subject_id, hadm_id)
        |         WHERE anemia_severity >= 1
        |           AND charttime <= admittime + INTERVAL 48 HOURS
        |       ) AS t
        |       ON r.subject_id = t.subject_id
        |       AND r.hadm_id = t.hadm_id
        | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
        | GROUP BY r.subject_id, r.hadm_id""".stripMargin,
      """SELECT d.subject_id,
        |       d.hadm_id,
        |       MAX(c.ccs_id IN (59, 60)) AS label
        |  FROM mimic.diagnoses_icd AS d
        |       NATURAL JOIN ccs_dx.ccs_single_level AS c
        | GROUP BY d.subject_id, d.hadm_id""".stripMargin
    )
    labels.createOrReplaceTempView("haa_labels")

    write_csv(
      labels,
      "haa_label_details.csv"
    )

    write_csv(
      make_label_stats(labels),
      "haa_label_dist.csv"
    )

    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id,
          |       MIN(charttime) AS timestamp,
          |       anemia_severity AS label
          |  FROM jamia.running_anemia_severity
          | WHERE anemia_severity >= 1
          | GROUP BY subject_id, hadm_id, anemia_severity
          | ORDER BY subject_id ASC, hadm_id ASC, timestamp ASC""".stripMargin
      ),
      "anemia_severities.csv"
    )

    write_csv(
      spark.sql(
        """SELECT r.subject_id,
          |       r.hadm_id,
          |       MIN(r.charttime) AS timestamp,
          |       r.anemia_severity AS label
          |  FROM jamia.running_anemia_severity AS r
          |       LEFT OUTER JOIN
          |       (SELECT subject_id,
          |               hadm_id
          |          FROM jamia.running_anemia_severity
          |               INNER JOIN mimic.admissions
          |               USING (subject_id, hadm_id)
          |         WHERE anemia_severity >= 1
          |           AND charttime <= admittime + INTERVAL 48 HOURS
          |       ) AS t
          |       ON r.subject_id = t.subject_id
          |       AND r.hadm_id = t.hadm_id
          | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
          |   AND anemia_severity >= 1
          | GROUP BY r.subject_id, r.hadm_id, r.anemia_severity
          | ORDER BY r.subject_id ASC, r.hadm_id ASC, timestamp ASC""".stripMargin
      ),
      "haa_severities.csv"
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
    write_csv(noteLabels, "haa_positive_note_labels.csv")

    val tableLabels = spark.sql(
      s"""SELECT r.subject_id,
         |       r.hadm_id,
         |       MIN(r.charttime) AS timestamp
         |  FROM jamia.running_anemia_severity AS r
         |       LEFT OUTER JOIN
         |       (SELECT subject_id,
         |               hadm_id
         |          FROM jamia.running_anemia_severity
         |               INNER JOIN mimic.admissions
         |               USING (subject_id, hadm_id)
         |         WHERE anemia_severity >= 1
         |           AND charttime <= admittime + INTERVAL 48 HOURS
         |       ) AS t
         |       ON r.subject_id = t.subject_id
         |       AND r.hadm_id = t.hadm_id
         | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
         |   AND anemia_severity >= 1
         |   AND r.hadm_id IS NOT NULL
         | GROUP BY r.subject_id, r.hadm_id""".stripMargin
    )
    tableLabels.cache()
    tableLabels.createOrReplaceTempView("table_labels")
    write_csv(tableLabels, "haa_positive_table_labels.csv")

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
      "haa_positive_labels.csv"
    )

    write_csv(
      make_label_stats(
        make_labels(
          """SELECT subject_id,
            |       hadm_id,
            |       MAX(label) AS label
            |  FROM note_dx
            | GROUP BY subject_id, hadm_id""".stripMargin,
          """SELECT subject_id,
            |       hadm_id,
            |       MAX(anemia_severity) >= 1 AS label
            |  FROM jamia.running_anemia_severity
            | GROUP BY subject_id, hadm_id""".stripMargin,
          """SELECT d.subject_id,
            |       d.hadm_id,
            |       MAX(c.ccs_id IN (59, 60)) AS label
            |  FROM mimic.diagnoses_icd AS d
            |       NATURAL JOIN ccs_dx.ccs_single_level AS c
            | GROUP BY d.subject_id, d.hadm_id""".stripMargin
        )
      ),
      "anemia_label_dist.csv"
    )

    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id
          |  FROM haa_labels
          | WHERE (note_dx OR table_dx)
          |   AND hadm_id IS NOT NULL""".stripMargin),
      "haa_hadms.csv"
    )

    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id
          |  FROM haa_labels
          | WHERE NOT note_dx
          |   AND NOT table_dx
          |   AND NOT icd_dx
          |   AND hadm_id IS NOT NULL""".stripMargin),
      "noa_hadms.csv"
    )
  }
}

object HaAExtractor {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new HaAExtractor(), System.err, args: _*)
  }
}
