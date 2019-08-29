package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.{MedianAggregateFunction, SparkApplication}
import picocli.CommandLine.Option


class MimicCohortExtractor extends Runnable with SparkApplication {
  @Option(names = Array("-c", "--chartevents"),
    description = Array("name of spark table containing chart events"))
  var chartEvents: String = "mimic.chartevents"

  @Option(names = Array("-o", "--outputevents"),
    description = Array("name of spark table containing output events"))
  var outputEvents: String = "mimic.outputevents"


  @Override
  def run(): Unit = {
    // Get cohort

    // Compute distribution of CCS primary diagnoses
  }
}




