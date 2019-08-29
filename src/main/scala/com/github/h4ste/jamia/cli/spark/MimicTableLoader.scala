package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.SparkApplication
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import picocli.CommandLine
import picocli.CommandLine.{Option, Parameters}

import scala.language.{implicitConversions, postfixOps}


class MimicTableLoader extends Runnable with SparkApplication with Serializable {
  @Option(names = Array("-m", "--write-mode"),
    description = Array("write mode when creating spark tables. valid options are \"error\", \"append\", \"overwrite\", and \"ignore\"."))
  var writeMode: String = "overwrite"

  @Parameters(index = "0",
    description = Array("path to mimic CSV files"),
    paramLabel = "PATH")
  var mimicDataPath: String = _

  @Option(names = Array("-g", "--gzipped"),
    description = Array("indicate whether the mimic data csv files are gzipped"))
  var gzipped: Boolean = false

  @Option(names = Array("-d", "--database"),
    description = Array("name of database to create (default: mimic)"))
  var databaseName: String = "mimic"

  @Option(names = Array("-R", "--repartition"),
    description = Array("repartition files before saving"))
  var repartition: Int = 1

  @Option(names = Array("-f", "--format"),
    description = Array("File format to save tables as"))
  var format: String = "orc"

  private[this] object MimicTables extends Enumeration {

    protected case class MimicTable(fileName: String, schema: StructType, writeOptions: DataFrameWriter[Row] => DataFrameWriter[Row] = identity, format: String = "orc") extends super.Val {
      def tableName: String = fileName.substring(0, fileName.indexOf('.')).toLowerCase

      def csv(): DataFrame = {
        val df = spark
          .read
          .schema(schema)
          .option("header", "true")
          .option("mode", "FAILFAST")
          .option("escape", "\"")
          .option("multiLine", "true")
          .csv(s"$mimicDataPath/$fileName${if (gzipped) ".gz" else ""}")
        if (repartition > 1) {
          df.repartition(repartition)
        } else {
          df
        }
      }


      def writeAsTable(): Unit = {
        val csv = this.csv()

        var writer = csv
          .select(csv.columns.map(x => col(x).as(x.toLowerCase)): _*)
          .write
          .mode(writeMode)

        if (format != null) {
          writer = writer.format(format)
        }

        writer = writeOptions(writer)

        writer.saveAsTable(s"$databaseName.$tableName")
      }
    }

    implicit def valueToMimicTable(x: Value): MimicTable = x.asInstanceOf[MimicTable]

    val admissions = MimicTable(
      fileName = "ADMISSIONS.csv",
      schema = StructType(Array(
        StructField("ROW_ID", IntegerType, nullable = false),
        StructField("SUBJECT_ID", IntegerType, nullable = false),
        StructField("HADM_ID", IntegerType, nullable = false),
        StructField("ADMITTIME", TimestampType, nullable = false),
        StructField("DISCHTIME", TimestampType, nullable = false),
        StructField("DEATHTIME", TimestampType, nullable = true),
        StructField("ADMISSION_TYPE", StringType, nullable = false),
        StructField("ADMISSION_LOCATION", StringType, nullable = false),
        StructField("DISCHARGE_LOCATION", StringType, nullable = false),
        StructField("INSURANCE", StringType, nullable = false),
        StructField("LANGUAGE", StringType, nullable = true),
        StructField("RELIGION", StringType, nullable = true),
        StructField("MARITAL_STATUS", StringType, nullable = true),
        StructField("ETHNICITY", StringType, nullable = false),
        StructField("EDREGTIME", TimestampType, nullable = true),
        StructField("EDOUTTIME", TimestampType, nullable = true),
        StructField("DIAGNOSIS", StringType, nullable = true),
        StructField("HOSPITAL_EXPIRE_FLAG", ShortType, nullable = true),
        StructField("HAS_CHARTEVENTS_DATA", ShortType, nullable = false)
      ))
    )

    val callouts = MimicTable(
      fileName = "CALLOUT.csv",
      schema = StructType(Array(
        StructField("ROW_ID", IntegerType, nullable = false),
        StructField("SUBJECT_ID", IntegerType, nullable = false),
        StructField("HADM_ID", IntegerType, nullable = false),
        StructField("SUBMIT_WARDID", IntegerType, nullable = true),
        StructField("SUBMIT_CAREUNIT", StringType, nullable = true),
        StructField("CURR_WARDID", IntegerType, nullable = true),
        StructField("CURR_CAREUNIT", StringType, nullable = true),
        StructField("CALLOUT_WARDID", IntegerType, nullable = true),
        StructField("CALLOUT_SERVICE", StringType, nullable = false),
        StructField("REQUEST_TELE", ShortType, nullable = false),
        StructField("REQUEST_RESP", ShortType, nullable = false),
        StructField("REQUEST_CDIFF", ShortType, nullable = false),
        StructField("REQUEST_MRSA", ShortType, nullable = false),
        StructField("REQUEST_VRE", ShortType, nullable = false),
        StructField("CALLOUT_STATUS", StringType, nullable = false),
        StructField("CALLOUT_OUTCOME", StringType, nullable = false),
        StructField("DISCHARGE_WARDID", IntegerType, nullable = true),
        StructField("ACKNOWLEDGE_STATUS", StringType, nullable = false),
        StructField("CREATETIME", TimestampType, nullable = false),
        StructField("UPDATETIME", TimestampType, nullable = false),
        StructField("ACKNOWLEDGETIME", TimestampType, nullable = true),
        StructField("OUTCOMETIME", TimestampType, nullable = false),
        StructField("FIRSTRESERVATIONTIME", TimestampType, nullable = true),
        StructField("CURRENTRESERVATIONTIME", TimestampType, nullable = true)
      ))
    )

    val caregivers = MimicTable(
      fileName = "CAREGIVERS.csv",
      schema = StructType(Array(
        StructField("ROW_ID", IntegerType, nullable = false),
        StructField("CGID", IntegerType, nullable = false),
        StructField("LABEL", StringType, nullable = true),
        StructField("DESCRIPTION", StringType, nullable = true)
      ))
    )

    val chartEvents = MimicTable(
      fileName = "CHARTEVENTS.csv",
      schema = StructType(Array(
        StructField("ROW_ID", IntegerType, nullable = false),
        StructField("SUBJECT_ID", IntegerType, nullable = false),
        StructField("HADM_ID", IntegerType, nullable = true),
        StructField("ICUSTAY_ID", IntegerType, nullable = true),
        StructField("ITEMID", IntegerType, nullable = true),
        StructField("CHARTTIME", TimestampType, nullable = true),
        StructField("STORETIME", TimestampType, nullable = true),
        StructField("CGID", IntegerType, nullable = true),
        StructField("VALUE", StringType, nullable = true),
        StructField("VALUENUM", DoubleType, nullable = true),
        StructField("VALUEUOM", StringType, nullable = true),
        StructField("WARNING", IntegerType, nullable = true),
        StructField("ERROR", IntegerType, nullable = true),
        StructField("RESULTSTATUS", StringType, nullable = true),
        StructField("STOPPED", StringType, nullable = true)
      )),
      writeOptions =
        writer => writer.bucketBy(8, "subject_id")
          .bucketBy(8, "hadm_id")
          .sortBy("subject_id", "hadm_id", "charttime", "row_id")
    )

    val cptEvents = MimicTable(
      fileName = "CPTEVENTS.csv",
      schema = StructType(Array(
        StructField("ROW_ID", IntegerType, nullable = false),
        StructField("SUBJECT_ID", IntegerType, nullable = false),
        StructField("HADM_ID", IntegerType, nullable = false),
        StructField("COSTCENTER", StringType, nullable = false),
        StructField("CHARTDATE", TimestampType, nullable = true),
        StructField("CPT_CD", StringType, nullable = false),
        StructField("CPT_NUMBER", IntegerType, nullable = true),
        StructField("CPT_SUFFIX", StringType, nullable = true),
        StructField("TICKET_ID_SEQ", IntegerType, nullable = true),
        StructField("SECTIONHEADER", StringType, nullable = true),
        StructField("SUBSECTIONHEADER", StringType, nullable = true),
        StructField("DESCRIPTION", StringType, nullable = true)
      ))
    )

    val dateTimeEvents = MimicTable(fileName = "DATETIMEEVENTS.csv", schema = StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("ITEMID", IntegerType, nullable = false),
      StructField("CHARTTIME", TimestampType, nullable = false),
      StructField("STORETIME", TimestampType, nullable = false),
      StructField("CGID", IntegerType, nullable = false),
      StructField("VALUE", TimestampType, nullable = true),
      StructField("VALUEUOM", StringType, nullable = false),
      StructField("WARNING", ShortType, nullable = true),
      StructField("ERROR", ShortType, nullable = true),
      StructField("RESULTSTATUS", StringType, nullable = true),
      StructField("STOPPED", StringType, nullable = true)
    ))
    )

    val diagnosesIcd = MimicTable(
      fileName = "DIAGNOSES_ICD.csv",
      schema = StructType(Array(
        StructField("ROW_ID", IntegerType, nullable = false),
        StructField("SUBJECT_ID", IntegerType, nullable = false),
        StructField("HADM_ID", IntegerType, nullable = false),
        StructField("SEQ_NUM", IntegerType, nullable = true),
        StructField("ICD9_CODE", StringType, nullable = true)
      ))
    )

    val drgCodes = MimicTable(
      fileName = "DRGCODES.csv",
      schema = StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("DRG_TYPE", StringType, nullable = false),
      StructField("DRG_CODE", StringType, nullable = false),
      StructField("DESCRIPTION", StringType, nullable = true),
      StructField("DRG_SEVERITY", ShortType, nullable = true),
      StructField("DRG_MORTALITY", ShortType, nullable = true)
    ))
    )

    val cptDefinitions = MimicTable(
      fileName = "D_CPT.csv",
      schema = StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("CATEGORY", ShortType, nullable = false),
      StructField("SECTIONRANGE", StringType, nullable = false),
      StructField("SECTIONHEADER", StringType, nullable = false),
      StructField("SUBSECTIONRANGE", StringType, nullable = false),
      StructField("SUBSECTIONHEADER", StringType, nullable = false),
      StructField("CODESUFFIX", StringType, nullable = true),
      StructField("MINCODEINSUBSECTION", IntegerType, nullable = false),
      StructField("MAXCODEINSUBSECTION", IntegerType, nullable = false)
    )))

    val icdDiagnosisDefinitions = MimicTable("D_ICD_DIAGNOSES.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("ICD9_CODE", StringType, nullable = false),
      StructField("SHORT_TITLE", StringType, nullable = false),
      StructField("LONG_TITLE", StringType, nullable = false)
    ))
    )

    val icdProcedureDefinitions = MimicTable("D_ICD_PROCEDURES.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("ICD9_CODE", StringType, nullable = false),
      StructField("SHORT_TITLE", StringType, nullable = false),
      StructField("LONG_TITLE", StringType, nullable = false)
    )))

    val itemDefinitions = MimicTable("D_ITEMS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("ITEMID", IntegerType, nullable = false),
      StructField("LABEL", StringType, nullable = true),
      StructField("ABBREVIATION", StringType, nullable = true),
      StructField("DBSOURCE", StringType, nullable = true),
      StructField("LINKSTO", StringType, nullable = true),
      StructField("CATEGORY", StringType, nullable = true),
      StructField("UNITNAME", StringType, nullable = true),
      StructField("PARAM_TYPE", StringType, nullable = true),
      StructField("CONCEPTID", IntegerType, nullable = true)
    )))

    val labItemDefinitions = MimicTable("D_LABITEMS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("ITEMID", IntegerType, nullable = false),
      StructField("LABEL", StringType, nullable = false),
      StructField("FLUID", StringType, nullable = false),
      StructField("CATEGORY", StringType, nullable = false),
      StructField("LOINC_CODE", StringType, nullable = true)
    )))

    val icuStays = MimicTable("ICUSTAYS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("ICUSTAY_ID", IntegerType, nullable = false),
      StructField("DBSOURCE", StringType, nullable = false),
      StructField("FIRST_CAREUNIT", StringType, nullable = false),
      StructField("LAST_CAREUNIT", StringType, nullable = false),
      StructField("FIRST_WARDID", ShortType, nullable = false),
      StructField("LAST_WARDID", ShortType, nullable = false),
      StructField("INTIME", TimestampType, nullable = false),
      StructField("OUTTIME", TimestampType, nullable = true),
      StructField("LOS", DoubleType, nullable = true)
    )))

    val inputEventsCareVue = MimicTable(
      fileName = "INPUTEVENTS_CV.csv",
      schema = StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("CHARTTIME", TimestampType, nullable = true),
      StructField("ITEMID", IntegerType, nullable = true),
      StructField("AMOUNT", DoubleType, nullable = true),
      StructField("AMOUNTUOM", StringType, nullable = true),
      StructField("RATE", DoubleType, nullable = true),
      StructField("RATEUOM", StringType, nullable = true),
      StructField("STORETIME", TimestampType, nullable = true),
      StructField("CGID", IntegerType, nullable = true),
      StructField("ORDERID", IntegerType, nullable = true),
      StructField("LINKORDERID", IntegerType, nullable = true),
      StructField("STOPPED", StringType, nullable = true),
      StructField("NEWBOTTLE", IntegerType, nullable = true),
      StructField("ORIGINALAMOUNT", DoubleType, nullable = true),
      StructField("ORIGINALAMOUNTUOM", StringType, nullable = true),
      StructField("ORIGINALROUTE", StringType, nullable = true),
      StructField("ORIGINALRATE", DoubleType, nullable = true),
      StructField("ORIGINALRATEUOM", StringType, nullable = true),
      StructField("ORIGINALSITE", StringType, nullable = true)
      )),
      writeOptions =
        writer => writer.bucketBy(8, "subject_id")
          .bucketBy(8, "hadm_id")
          .sortBy("subject_id", "hadm_id", "charttime", "row_id")
    )

    val inputEventsMetaVision = MimicTable("INPUTEVENTS_MV.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("STARTTIME", TimestampType, nullable = true),
      StructField("ENDTIME", TimestampType, nullable = true),
      StructField("ITEMID", IntegerType, nullable = true),
      StructField("AMOUNT", DoubleType, nullable = true),
      StructField("AMOUNTUOM", StringType, nullable = true),
      StructField("RATE", DoubleType, nullable = true),
      StructField("RATEUOM", StringType, nullable = true),
      StructField("STORETIME", TimestampType, nullable = true),
      StructField("CGID", IntegerType, nullable = true),
      StructField("ORDERID", IntegerType, nullable = true),
      StructField("LINKORDERID", IntegerType, nullable = true),
      StructField("ORDERCATEGORYNAME", StringType, nullable = true),
      StructField("SECONDARYORDERCATEGORYNAME", StringType, nullable = true),
      StructField("ORDERCOMPONENTTYPEDESCRIPTION", StringType, nullable = true),
      StructField("ORDERCATEGORYDESCRIPTION", StringType, nullable = true),
      StructField("PATIENTWEIGHT", DoubleType, nullable = true),
      StructField("TOTALAMOUNT", DoubleType, nullable = true),
      StructField("TOTALAMOUNTUOM", StringType, nullable = true),
      StructField("ISOPENBAG", ShortType, nullable = true),
      StructField("CONTINUEINNEXTDEPT", ShortType, nullable = true),
      StructField("CANCELREASON", ShortType, nullable = true),
      StructField("STATUSDESCRIPTION", StringType, nullable = true),
      StructField("COMMENTS_EDITEDBY", StringType, nullable = true),
      StructField("COMMENTS_CANCELEDBY", StringType, nullable = true),
      StructField("COMMENTS_DATE", TimestampType, nullable = true),
      StructField("ORIGINALAMOUNT", DoubleType, nullable = true),
      StructField("ORIGINALRATE", DoubleType, nullable = true)
    )),
      writeOptions =
        writer => writer.bucketBy(8, "subject_id")
          .bucketBy(8, "hadm_id")
          .sortBy("subject_id", "hadm_id", "charttime", "row_id")
    )

    val labEvents = MimicTable("LABEVENTS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("ITEMID", IntegerType, nullable = false),
      StructField("CHARTTIME", TimestampType, nullable = true),
      StructField("VALUE", StringType, nullable = true),
      StructField("VALUENUM", DoubleType, nullable = true),
      StructField("VALUEUOM", StringType, nullable = true),
      StructField("FLAG", StringType, nullable = true)
    )),
      writeOptions =
        writer => writer.bucketBy(8, "subject_id")
          .bucketBy(8, "hadm_id")
          .sortBy("subject_id", "hadm_id", "charttime", "row_id")
    )

    val microbiologyEvents = MimicTable("MICROBIOLOGYEVENTS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("CHARTDATE", TimestampType, nullable = true),
      StructField("CHARTTIME", TimestampType, nullable = true),
      StructField("SPEC_ITEMID", IntegerType, nullable = true),
      StructField("SPEC_TYPE_DESC", StringType, nullable = true),
      StructField("ORG_ITEMID", IntegerType, nullable = true),
      StructField("ORG_NAME", StringType, nullable = true),
      StructField("ISOLATE_NUM", ShortType, nullable = true),
      StructField("AB_ITEMID", IntegerType, nullable = true),
      StructField("AB_NAME", StringType, nullable = true),
      StructField("DILUTION_TEXT", StringType, nullable = true),
      StructField("DILUTION_COMPARISON", StringType, nullable = true),
      StructField("DILUTION_VALUE", DoubleType, nullable = true),
      StructField("INTERPRETATION", StringType, nullable = true)
    )))

    val noteEvents = MimicTable("NOTEEVENTS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("CHARTDATE", TimestampType, nullable = true),
      StructField("CHARTTIME", TimestampType, nullable = true),
      StructField("STORETIME", TimestampType, nullable = true),
      StructField("CATEGORY", StringType, nullable = true),
      StructField("DESCRIPTION", StringType, nullable = true),
      StructField("CGID", IntegerType, nullable = true),
      StructField("ISERROR", StringType, nullable = true),
      StructField("TEXT", StringType, nullable = true)
    )),
      writeOptions =
        writer => writer.bucketBy(8, "subject_id")
          .bucketBy(8, "hadm_id")
          .sortBy("subject_id", "hadm_id", "charttime", "row_id")
    )

    val outputEvents = MimicTable(
      fileName = "OUTPUTEVENTS.csv",
      schema = StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = true),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("CHARTTIME", TimestampType, nullable = true),
      StructField("ITEMID", IntegerType, nullable = true),
      StructField("VALUE", DoubleType, nullable = true),
      StructField("VALUEUOM", StringType, nullable = true),
      StructField("STORETIME", TimestampType, nullable = true),
      StructField("CGID", IntegerType, nullable = true),
      StructField("STOPPED", StringType, nullable = true),
      StructField("NEWBOTTLE", StringType, nullable = true),
      StructField("ISERROR", IntegerType, nullable = true)
    )),
      writeOptions =
        writer => writer.bucketBy(8, "subject_id")
        .bucketBy(8, "hadm_id")
        .sortBy("subject_id", "hadm_id", "charttime", "row_id")
    )

    val patients = MimicTable("PATIENTS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("GENDER", StringType, nullable = false),
      StructField("DOB", TimestampType, nullable = false),
      StructField("DOD", TimestampType, nullable = true),
      StructField("DOD_HOSP", TimestampType, nullable = true),
      StructField("DOD_SSN", TimestampType, nullable = true),
      StructField("EXPIRE_FLAG", IntegerType, nullable = false)
    )))

    val prescriptions = MimicTable("PRESCRIPTIONS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("STARTDATE", TimestampType, nullable = true),
      StructField("ENDDATE", TimestampType, nullable = true),
      StructField("DRUG_TYPE", StringType, nullable = false),
      StructField("DRUG", StringType, nullable = false),
      StructField("DRUG_NAME_POE", StringType, nullable = true),
      StructField("DRUG_NAME_GENERIC", StringType, nullable = true),
      StructField("FORMULARY_DRUG_CD", StringType, nullable = true),
      StructField("GSN", StringType, nullable = true),
      StructField("NDC", StringType, nullable = true),
      StructField("PROD_STRENGTH", StringType, nullable = true),
      StructField("DOSE_VAL_RX", StringType, nullable = true),
      StructField("DOSE_UNIT_RX", StringType, nullable = true),
      StructField("FORM_VAL_DISP", StringType, nullable = true),
      StructField("FORM_UNIT_DISP", StringType, nullable = true),
      StructField("ROUTE", StringType, nullable = true)
    )))

    val procedureEvents = MimicTable("PROCEDUREEVENTS_MV.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("STARTTIME", TimestampType, nullable = true),
      StructField("ENDTIME", TimestampType, nullable = true),
      StructField("ITEMID", IntegerType, nullable = true),
      StructField("VALUE", DoubleType, nullable = true),
      StructField("VALUEUOM", StringType, nullable = true),
      StructField("LOCATION", StringType, nullable = true),
      StructField("LOCATIONCATEGORY", StringType, nullable = true),
      StructField("STORETIME", TimestampType, nullable = true),
      StructField("CGID", IntegerType, nullable = true),
      StructField("ORDERID", IntegerType, nullable = true),
      StructField("LINKORDERID", IntegerType, nullable = true),
      StructField("ORDERCATEGORYNAME", StringType, nullable = true),
      StructField("SECONDARYORDERCATEGORYNAME", StringType, nullable = true),
      StructField("ORDERCATEGORYDESCRIPTION", StringType, nullable = true),
      StructField("ISOPENBAG", ShortType, nullable = true),
      StructField("CONTINUEINNEXTDEPT", ShortType, nullable = true),
      StructField("CANCELREASON", ShortType, nullable = true),
      StructField("STATUSDESCRIPTION", StringType, nullable = true),
      StructField("COMMENTS_EDITEDBY", StringType, nullable = true),
      StructField("COMMENTS_CANCELEDBY", StringType, nullable = true),
      StructField("COMMENTS_DATE", TimestampType, nullable = true)
    )))

    val proceduresIcd = MimicTable("PROCEDURES_ICD.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("SEQ_NUM", IntegerType, nullable = false),
      StructField("ICD9_CODE", StringType, nullable = false)
    )))

    val services = MimicTable("SERVICES.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("TRANSFERTIME", TimestampType, nullable = false),
      StructField("PREV_SERVICE", StringType, nullable = true),
      StructField("CURR_SERVICE", StringType, nullable = true)
    )))

    val transfers = MimicTable("TRANSFERS.csv", StructType(Array(
      StructField("ROW_ID", IntegerType, nullable = false),
      StructField("SUBJECT_ID", IntegerType, nullable = false),
      StructField("HADM_ID", IntegerType, nullable = false),
      StructField("ICUSTAY_ID", IntegerType, nullable = true),
      StructField("DBSOURCE", StringType, nullable = true),
      StructField("EVENTTYPE", StringType, nullable = true),
      StructField("PREV_CAREUNIT", StringType, nullable = true),
      StructField("CURR_CAREUNIT", StringType, nullable = true),
      StructField("PREV_WARDID", ShortType, nullable = true),
      StructField("CURR_WARDID", ShortType, nullable = true),
      StructField("INTIME", TimestampType, nullable = true),
      StructField("OUTTIME", TimestampType, nullable = true),
      StructField("LOS", DoubleType, nullable = true)
    )))
  }

  @Override
  def run(): Unit = {
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName")

    MimicTables.values.par.foreach(t =>
      synchronized {
        sc.setJobGroup(s"Writing ${t.tableName}", s"Converting ${t.fileName} to ${t.tableName}")
        t.writeAsTable()
      }
    )
  }
}

object MimicTableLoader {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new MimicTableLoader(), System.err, args: _*)
  }
}
