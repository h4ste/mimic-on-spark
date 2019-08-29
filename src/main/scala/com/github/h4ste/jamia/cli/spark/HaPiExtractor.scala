package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.JamiaTables
import picocli.CommandLine

class HaPiExtractor extends LabelExtractor {

  @Override
  def run(): Unit = {

    spark.sql("CREATE DATABASE IF NOT EXISTS jamia")

    if (recreate) {
      spark.sql("DROP TABLE IF EXISTS jamia.adults")
      spark.sql("DROP TABLE IF EXISTS jamia.npuap_pi_stages")
      spark.sql("DROP TABLE IF EXISTS jamia.running_pi_stages")
      spark.sql("DROP TABLE IF EXISTS jamia.hapi_patients")
      spark.sql("DROP TABLE IF EXISTS jamia.nopi_patients")
    }


    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.npuap_pi_stages AS
        |SELECT subject_id,
        |       hadm_id,
        |       charttime,
        |       c.value,
        |
        |       CASE
        |       WHEN value LIKE 'Unable%' OR value = "Other/Remarks" THEN 1
        |       WHEN value IN ("Deep Tiss Injury", "Deep tissue injury") THEN 1
        |       WHEN value LIKE 'Red%' OR value = "Intact,Color Chg" THEN 1
        |
        |       WHEN value LIKE 'Part%' OR value = "Through Dermis" THEN 2
        |
        |       WHEN value IN (
        |               "Full thickness skin loss that may extend down to underlying fascia; ulcer may have tunneling or undermining",
        |               "Full Thickness"
        |       ) THEN 3
        |
        |       WHEN value IN (
        |               "Full thickness skin loss with damage to muscle, bone, or supporting structures; tunneling or undermining may be present",
        |               "Through Fascia",
        |               "To Bone"
        |       ) THEN 4
        |
        |       ELSE 0
        |       END AS pi_stage
        |  FROM mimic.chartevents AS c
        | WHERE itemid IN (
        |            -- MetaVision
        |          224631, -- Pressure Ulcer Stage #1
        |          224965, -- Pressure Ulcer Stage #2
        |          224966, -- Pressure Ulcer Stage #3
        |          224967, -- Pressure Ulcer Stage #4
        |          224968, -- Pressure Ulcer Stage #5
        |          224969, -- Pressure Ulcer Stage #6
        |          224970, -- Pressure Ulcer Stage #7
        |          224971, -- Pressure Ulcer Stage #8
        |          227618, -- Pressure Ulcer Stage #9
        |          227619,  -- Pressure Ulcer Stage #10
        |          -- CareVue
        |          551, -- Pressure Sore #1 [Stage]
        |          552, -- Pressure Sore #3 [Stage]
        |          553  -- Pressure Sore #3 [Stage]
        |       )
        |   AND (ERROR IS NULL OR ERROR <> 1)""".stripMargin)


    spark.sql(
      """CREATE TABLE IF NOT EXISTS jamia.running_pi_stages AS
        |SELECT subject_id,
        |       hadm_id,
        |       charttime,
        |       MAX(pi_stage) OVER (PARTITION BY subject_id, hadm_id ORDER BY charttime ASC) AS pi_stage
        |  FROM jamia.npuap_pi_stages
        | WHERE charttime IS NOT NULL""".stripMargin
    )

    spark.sql(s"CREATE TABLE IF NOT EXISTS jamia.adults AS\n${JamiaTables.adultQuery}")

    spark.sql(
      """CREATE TEMPORARY VIEW pi_patients AS
        |SELECT subject_id,
        |       hadm_id
        |  FROM jamia.running_pi_stages
        | WHERE pi_stage >= 1""".stripMargin
    )


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
        | WHERE t.hadm_id IS NULL
        |    OR t.subject_id IS NULL
        | GROUP BY n.subject_id, n.hadm_id""".stripMargin,
      """SELECT r.subject_id,
        |       r.hadm_id,
        |       MAX(r.pi_stage) >= 1 AS label
        |  FROM jamia.running_pi_stages AS r
        |       LEFT OUTER JOIN
        |       (SELECT subject_id,
        |               hadm_id
        |          FROM jamia.running_pi_stages
        |               INNER JOIN mimic.admissions
        |               USING (subject_id, hadm_id)
        |         WHERE pi_stage >= 1
        |           AND charttime <= admittime + INTERVAL 48 HOURS
        |       ) AS t
        |       ON r.subject_id = t.subject_id
        |       AND r.hadm_id = t.hadm_id
        | WHERE t.hadm_id IS NULL
        |    OR t.subject_id IS NULL
        | GROUP BY r.subject_id, r.hadm_id""".stripMargin,
      """SELECT d.subject_id,
        |       d.hadm_id,
        |       MAX(c.ccs_id == 199) AS label
        |  FROM mimic.diagnoses_icd AS d
        |       NATURAL JOIN ccs_dx.ccs_single_level AS c
        | GROUP BY d.subject_id, d.hadm_id""".stripMargin
    )
    labels.createOrReplaceTempView("hapi_labels")

    write_csv(
      labels,
      "hapi_label_details.csv"
    )

    write_csv(
      make_label_stats(labels),
      "hapi_label_dist.csv"
    )

    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id,
          |       MIN(charttime) AS timestamp,
          |       pi_stage AS label
          |  FROM jamia.running_pi_stages
          | WHERE pi_stage >= 1
          | GROUP BY subject_id, hadm_id, pi_stage
          | ORDER BY subject_id ASC, hadm_id ASC, timestamp ASC""".stripMargin
      ),
      "pi_stages.csv"
    )

    write_csv(
      spark.sql(
        """SELECT r.subject_id,
          |       r.hadm_id,
          |       MIN(r.charttime) AS timestamp,
          |       r.pi_stage AS label
          |  FROM jamia.running_pi_stages AS r
          |       LEFT OUTER JOIN
          |       (SELECT subject_id,
          |               hadm_id
          |          FROM jamia.running_pi_stages
          |               INNER JOIN mimic.admissions
          |               USING (subject_id, hadm_id)
          |         WHERE pi_stage >= 1
          |           AND charttime <= admittime + INTERVAL 48 HOURS
          |       ) AS t
          |       ON r.subject_id = t.subject_id
          |       AND r.hadm_id = t.hadm_id
          | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
          |   AND pi_stage >= 1
          | GROUP BY r.subject_id, r.hadm_id, r.pi_stage
          | ORDER BY r.subject_id ASC, r.hadm_id ASC, timestamp ASC""".stripMargin
      ),
      "hapi_stages.csv"
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
    write_csv(noteLabels, "hapi_positive_note_labels.csv")

    val tableLabels = spark.sql(
      s"""SELECT r.subject_id,
         |       r.hadm_id,
         |       MIN(r.charttime) AS timestamp
         |  FROM jamia.running_pi_stages AS r
         |       LEFT OUTER JOIN
         |       (SELECT subject_id,
         |               hadm_id
         |          FROM jamia.running_pi_stages
         |               INNER JOIN mimic.admissions
         |               USING (subject_id, hadm_id)
         |         WHERE pi_stage >= 1
         |           AND charttime <= admittime + INTERVAL 48 HOURS
         |       ) AS t
         |       ON r.subject_id = t.subject_id
         |       AND r.hadm_id = t.hadm_id
         | WHERE (t.hadm_id IS NULL OR t.subject_id IS NULL)
         |   AND pi_stage >= 1
         |   AND r.hadm_id IS NOT NULL
         | GROUP BY r.subject_id, r.hadm_id""".stripMargin
    )
    tableLabels.cache()
    tableLabels.createOrReplaceTempView("table_labels")
    write_csv(tableLabels.select("*"), "hapi_positive_table_labels.csv")

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
      "hapi_positive_labels.csv"
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
            |       MAX(pi_stage) >= 1 AS label
            |  FROM jamia.running_pi_stages
            | GROUP BY subject_id, hadm_id""".stripMargin,
          """SELECT d.subject_id,
            |       d.hadm_id,
            |       MAX(c.ccs_id == 199) AS label
            |  FROM mimic.diagnoses_icd AS d
            |       NATURAL JOIN ccs_dx.ccs_single_level AS c
            | GROUP BY d.subject_id, d.hadm_id""".stripMargin
        )
      ),
      "pi_label_dist.csv"
    )

    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id
          |  FROM hapi_labels
          | WHERE (note_dx OR table_dx)
          |   AND hadm_id IS NOT NULL""".stripMargin),
      "hapi_hadms.csv"
    )

    write_csv(
      spark.sql(
        """SELECT subject_id,
          |       hadm_id
          |  FROM hapi_labels
          | WHERE NOT note_dx
          |   AND NOT table_dx
          |   AND NOT icd_dx
          |   AND hadm_id IS NOT NULL""".stripMargin),
      "nopi_hadms.csv"
    )
  }
}

object HaPiExtractor {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new HaPiExtractor(), System.err, args: _*)
  }
}
