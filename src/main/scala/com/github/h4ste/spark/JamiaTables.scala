package com.github.h4ste.spark

object JamiaTables {
  // Spark DATEDIFF is of the form (enddate, startdate)
  val ageQuery = """SELECT subject_id,
                   |        hadm_id,
                   |        DATEDIFF(admittime, dob) / 365.2425 AS age
                   |  FROM mimic.admissions
                   |       INNER JOIN mimic.patients
                   |       USING (subject_id)""".stripMargin


  val adultQuery = """SELECT subject_id, hadm_id
                     |  FROM mimic.admissions
                     |       INNER JOIN mimic.patients
                     |       USING (subject_id)
                     | WHERE admittime >= dob + INTERVAL 15 YEARS""".stripMargin
}
