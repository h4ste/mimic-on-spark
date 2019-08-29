package com.github.h4ste.jamia.cli.spark

import com.github.h4ste.spark.SparkApplication
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}
import picocli.CommandLine
import picocli.CommandLine.{Option, Parameters}

import scala.language.{implicitConversions, postfixOps}


class CommunityTableGenerator extends Runnable with SparkApplication {

  @Option(names = Array("-d", "--database"),
    description = Array("name of database to create (default: mimic)"))
  var databaseName: String = "concepts"


  @Override
  def run(): Unit = {
    spark.sql("CREATE DATABASE IF NOT EXISTS concepts")

    spark.sql("DROP TABLE IF EXISTS concepts.uofirstday")

    spark.sql("""CREATE TABLE concepts.uofirstday as
                |select
                |-- patient identifiers
                |ie.subject_id, ie.hadm_id, ie.icustay_id         |
                |-- volumes associated with urine output ITEMIDs
                |, sum(
                |-- we consider input of GU irrigant as a negative volume
                |case
                |when oe.itemid = 227488 and oe.value > 0 then -1*oe.value
                |else oe.value
                |end) as UrineOutput
                |from icustays ie
                |-- Join to the outputevents table to get urine output
                |left join outputevents oe
                |-- join on all patient identifiers
                |on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and ie.icustay_id = oe.icustay_id
                |-- and ensure the data occurs during the first day
                |and oe.charttime between ie.intime and (ie.intime + interval '1' day) -- first ICU day
                |where itemid in
                |(
                |-- these are the most frequently occurring urine output observations in CareVue
                |40055, -- "Urine Out Foley"
                |43175, -- "Urine ."
                |40069, -- "Urine Out Void"
                |40094, -- "Urine Out Condom Cath"
                |40715, -- "Urine Out Suprapubic"
                |40473, -- "Urine Out IleoConduit"
                |40085, -- "Urine Out Incontinent"
                |40057, -- "Urine Out Rt Nephrostomy"
                |40056, -- "Urine Out Lt Nephrostomy"
                |40405, -- "Urine Out Other"
                |40428, -- "Urine Out Straight Cath"
                |40086,--	Urine Out Incontinent
                |40096, -- "Urine Out Ureteral Stent #1"
                |40651, -- "Urine Out Ureteral Stent #2"         |
                |-- these are the most frequently occurring urine output observations in MetaVision
                |226559, -- "Foley"
                |226560, -- "Void"
                |226561, -- "Condom Cath"
                |226584, -- "Ileoconduit"
                |226563, -- "Suprapubic"
                |226564, -- "R Nephrostomy"
                |226565, -- "L Nephrostomy"
                |226567, --	Straight Cath
                |226557, -- R Ureteral Stent
                |226558, -- L Ureteral Stent
                |227488, -- GU Irrigant Volume In
                |227489  -- GU Irrigant/Urine Volume Out
                |)
                |group by ie.subject_id, ie.hadm_id, ie.icustay_id
                |order by ie.subject_id, ie.hadm_id, ie.icustay_id""".stripMargin)

    // First, create a temporary table to store relevant data from CHARTEVENTS.
    spark.sql("""DROP VIEW IF EXISTS ventsettings""")
    spark.sql("""CREATE TEMPORARY VIEW ventsettings AS
                |select
                |icustay_id, charttime
                |-- case statement determining whether it is an instance of mech vent
                |, max(
                |case
                |when itemid is null or value is null then 0 -- can't have null values
                |  when itemid = 720 and value != 'Other/Remarks' THEN 1  -- VentTypeRecorded
                |when itemid = 223848 and value != 'Other' THEN 1
                |when itemid = 223849 then 1 -- ventilator mode
                |  when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
                |when itemid in
                |(
                |  445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
                |  , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
                |  , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
                |  , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
                |  , 543 -- PlateauPressure
                |  , 5865,5866,224707,224709,224705,224706 -- APRV pressure
                |  , 60,437,505,506,686,220339,224700 -- PEEP
                |  , 3459 -- high pressure relief
                |  , 501,502,503,224702 -- PCV
                |  , 223,667,668,669,670,671,672 -- TCPCV
                |  , 224701 -- PSVlevel
                |)
                |THEN 1
                |else 0
                |end
                |) as MechVent
                |, max(
                |case
                |-- initiation of oxygen therapy indicates the ventilation has ended
                |  when itemid = 226732 and value in
                |  (
                |    'Nasal cannula', -- 153714 observations
                |  'Face tent', -- 24601 observations
                |  'Aerosol-cool', -- 24560 observations
                |  'Trach mask ', -- 16435 observations
                |  'High flow neb', -- 10785 observations
                |  'Non-rebreather', -- 5182 observations
                |  'Venti mask ', -- 1947 observations
                |  'Medium conc mask ', -- 1888 observations
                |  'T-piece', -- 1135 observations
                |  'High flow nasal cannula', -- 925 observations
                |  'Ultrasonic neb', -- 9 observations
                |  'Vapomist' -- 3 observations
                |) then 1
                |when itemid = 467 and value in
                |  (
                |    'Cannula', -- 278252 observations
                |  'Nasal Cannula', -- 248299 observations
                |  -- 'None', -- 95498 observations
                |  'Face Tent', -- 35766 observations
                |  'Aerosol-Cool', -- 33919 observations
                |  'Trach Mask', -- 32655 observations
                |  'Hi Flow Neb', -- 14070 observations
                |  'Non-Rebreather', -- 10856 observations
                |  'Venti Mask', -- 4279 observations
                |  'Medium Conc Mask', -- 2114 observations
                |  'Vapotherm', -- 1655 observations
                |  'T-Piece', -- 779 observations
                |  'Hood', -- 670 observations
                |  'Hut', -- 150 observations
                |  'TranstrachealCat', -- 78 observations
                |  'Heated Neb', -- 37 observations
                |  'Ultrasonic Neb' -- 2 observations
                |) then 1
                |else 0
                |end
                |) as OxygenTherapy
                |, max(
                |case when itemid is null or value is null then 0
                |-- extubated indicates ventilation event has ended
                |when itemid = 640 and value = 'Extubated' then 1
                |when itemid = 640 and value = 'Self Extubation' then 1
                |else 0
                |end
                |)
                |as Extubated
                |, max(
                |case when itemid is null or value is null then 0
                |when itemid = 640 and value = 'Self Extubation' then 1
                |else 0
                |end
                |)
                |as SelfExtubated
                |  from chartevents ce
                |where ce.value is not null
                |-- exclude rows marked as error
                |  and ce.error IS DISTINCT FROM 1
                |and itemid in
                |(
                |  -- the below are settings used to indicate ventilation
                |720, 223849 -- vent mode
                |, 223848 -- vent type
                |, 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
                |, 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
                |, 218,436,535,444,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean ("RespPressure")
                |, 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
                |, 543 -- PlateauPressure
                |, 5865,5866,224707,224709,224705,224706 -- APRV pressure
                |, 60,437,505,506,686,220339,224700 -- PEEP
                |, 3459 -- high pressure relief
                |, 501,502,503,224702 -- PCV
                |, 223,667,668,669,670,671,672 -- TCPCV
                |, 224701 -- PSVlevel
                |
                |-- the below are settings used to indicate extubation
                |, 640 -- extubated
                |
                |-- the below indicate oxygen/NIV, i.e. the end of a mechanical vent event
                |, 468 -- O2 Delivery Device#2
                |, 469 -- O2 Delivery Mode
                |, 470 -- O2 Flow (lpm)
                |, 471 -- O2 Flow (lpm) #2
                |, 227287 -- O2 Flow (additional cannula)
                |, 226732 -- O2 Delivery Device(s)
                |, 223834 -- O2 Flow
                |
                |-- used in both oxygen + vent calculation
                |, 467 -- O2 Delivery Device
                |)
                |group by icustay_id, charttime
                |UNION
                |-- add in the extubation flags from procedureevents_mv
                |  -- note that we only need the start time for the extubation
                |  -- (extubation is always charted as ending 1 minute after it started)
                |select
                |icustay_id, starttime as charttime
                |, 0 as MechVent
                |, 0 as OxygenTherapy
                |, 1 as Extubated
                |, case when itemid = 225468 then 1 else 0 end as SelfExtubated
                |  from procedureevents_mv
                |  where itemid in
                |(
                |  227194 -- "Extubation"
                |  , 225468 -- "Unplanned Extubation (patient-initiated)"
                |  , 225477 -- "Unplanned Extubation (non-patient initiated)"
                |)""".stripMargin)

    spark.sql("DROP TABLE IF EXISTS concepts.ventdurations")
    spark.sql("""CREATE TABLE concepts.ventdurations AS
                |with vd0 as
                |  (
                |    select
                |      icustay_id
                |      -- this carries over the previous charttime which had a mechanical ventilation event
                |, case
                |when MechVent=1 then
                |  LAG(CHARTTIME, 1) OVER (partition by icustay_id, MechVent order by charttime)
                |else null
                |end as charttime_lag
                |, charttime
                |, MechVent
                |, OxygenTherapy
                |, Extubated
                |, SelfExtubated
                |from ventsettings
                |)
                |, vd1 as
                |  (
                |    select
                |      icustay_id
                |    , charttime_lag
                |    , charttime
                |    , MechVent
                |    , OxygenTherapy
                |    , Extubated
                |    , SelfExtubated
                |
                |    -- if this is a mechanical ventilation event, we calculate the time since the last event
                |, case
                |-- if the current observation indicates mechanical ventilation is present
                |  -- calculate the time since the last vent event
                |when MechVent=1 then
                |  unix_timestamp(CHARTTIME) - unix_timestamp(charttime_lag)
                |else null
                |end as ventduration
                |
                |, LAG(Extubated,1)
                |OVER
                |(
                |  partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
                |  order by charttime
                |) as ExtubatedLag
                |
                |-- now we determine if the current mech vent event is a "new", i.e. they've just been intubated
                |, case
                |-- if there is an extubation flag, we mark any subsequent ventilation as a new ventilation event
                |  --when Extubated = 1 then 0 -- extubation is *not* a new ventilation event, the *subsequent* row is
                |  when
                |LAG(Extubated,1)
                |OVER
                |(
                |  partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
                |  order by charttime
                |)
                |= 1 then 1
                |-- if patient has initiated oxygen therapy, and is not currently vented, start a newvent
                |when MechVent = 0 and OxygenTherapy = 1 then 1
                |-- if there is less than 8 hours between vent settings, we do not treat this as a new ventilation event
                |  when CHARTTIME > charttime_lag + interval '8' hour
                |  then 1
                |else 0
                |end as newvent
                |-- use the staging table with only vent settings from chart events
                |  FROM vd0 ventsettings
                |)
                |, vd2 as
                |  (
                |    select vd1.*
                |-- create a cumulative sum of the instances of new ventilation
                |-- this results in a monotonic integer assigned to each instance of ventilation
                |, case when MechVent=1 or Extubated = 1 then
                |  SUM( newvent )
                |OVER ( partition by icustay_id order by charttime )
                |else null end
                |  as ventnum
                |  --- now we convert CHARTTIME of ventilator settings into durations
                |  from vd1
                |)
                |-- create the durations for each mechanical ventilation instance
                |  select icustay_id
                |  -- regenerate ventnum so it's sequential
                |, ROW_NUMBER() over (partition by icustay_id order by ventnum) as ventnum
                |, min(charttime) as starttime
                |, max(charttime) as endtime
                |, (unix_timestamp(max(charttime))-unix_timestamp(min(charttime)))/60/60 AS duration_hours
                |from vd2
                |  group by icustay_id, ventnum
                |having min(charttime) != max(charttime)
                |-- patient had to be mechanically ventilated at least once
                |  -- i.e. max(mechvent) should be 1
                |-- this excludes a frequent situation of NIV/oxygen before intub
                |-- in these cases, ventnum=0 and max(mechvent)=0, so they are ignored
                |  and max(mechvent) = 1
                |order by icustay_id, ventnum""".stripMargin)

    spark.sql("""DROP TABLE IF EXISTS concepts.ventfirstday""")
    spark.sql("""CREATE TABLE concepts.ventfirstday AS
                |select
                |  ie.subject_id, ie.hadm_id, ie.icustay_id
                |  -- if vd.icustay_id is not null, then they have a valid ventilation event
                |  -- in this case, we say they are ventilated
                |  -- otherwise, they are not
                |  , max(case
                |      when vd.icustay_id is not null then 1
                |    else 0 end) as vent
                |from icustays ie
                |left join concepts.ventdurations vd
                |  on ie.icustay_id = vd.icustay_id
                |  and
                |  (
                |    -- ventilation duration overlaps with ICU admission -> vented on admission
                |    (vd.starttime <= ie.intime and vd.endtime >= ie.intime)
                |    -- ventilation started during the first day
                |    OR (vd.starttime >= ie.intime and vd.starttime <= ie.intime + interval '1' day)
                |  )
                |group by ie.subject_id, ie.hadm_id, ie.icustay_id
                |order by ie.subject_id, ie.hadm_id, ie.icustay_id""")

    spark.sql("""DROP TABLE IF EXISTS concepts.vitalsfirstday""")
    spark.sql("""CREATE TABLE concepts.vitalsfirstday AS
                |SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id
                |
                |-- Easier names
                |, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
                |, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
                |, avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
                |, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
                |, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
                |, avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
                |, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
                |, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
                |, avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
                |, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
                |, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
                |, avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
                |, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
                |, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
                |, avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
                |, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
                |, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
                |, avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
                |, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
                |, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
                |, avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
                |, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
                |, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
                |, avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean
                |
                |FROM  (
                |  select ie.subject_id, ie.hadm_id, ie.icustay_id
                |  , case
                |    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
                |    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
                |    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
                |    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
                |    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
                |    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
                |    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
                |    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
                |    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose
                |
                |    else null end as VitalID
                |      -- convert F to C
                |  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum
                |
                |  from icustays ie
                |  left join chartevents ce
                |  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
                |  and ce.charttime between ie.intime and ie.intime + interval '1' day
                |  -- exclude rows marked as error
                |  and ce.error IS DISTINCT FROM 1
                |  where ce.itemid in
                |  (
                |  -- HEART RATE
                |  211, --"Heart Rate"
                |  220045, --"Heart Rate"
                |
                |  -- Systolic/diastolic
                |
                |  51, --	Arterial BP [Systolic]
                |  442, --	Manual BP [Systolic]
                |  455, --	NBP [Systolic]
                |  6701, --	Arterial BP #2 [Systolic]
                |  220179, --	Non Invasive Blood Pressure systolic
                |  220050, --	Arterial Blood Pressure systolic
                |
                |  8368, --	Arterial BP [Diastolic]
                |  8440, --	Manual BP [Diastolic]
                |  8441, --	NBP [Diastolic]
                |  8555, --	Arterial BP #2 [Diastolic]
                |  220180, --	Non Invasive Blood Pressure diastolic
                |  220051, --	Arterial Blood Pressure diastolic
                |
                |
                |  -- MEAN ARTERIAL PRESSURE
                |  456, --"NBP Mean"
                |  52, --"Arterial BP Mean"
                |  6702, --	Arterial BP Mean #2
                |  443, --	Manual BP Mean(calc)
                |  220052, --"Arterial Blood Pressure mean"
                |  220181, --"Non Invasive Blood Pressure mean"
                |  225312, --"ART BP mean"
                |
                |  -- RESPIRATORY RATE
                |  618,--	Respiratory Rate
                |  615,--	Resp Rate (Total)
                |  220210,--	Respiratory Rate
                |  224690, --	Respiratory Rate (Total)
                |
                |
                |  -- SPO2, peripheral
                |  646, 220277,
                |
                |  -- GLUCOSE, both lab and fingerstick
                |  807,--	Fingerstick Glucose
                |  811,--	Glucose (70-105)
                |  1529,--	Glucose
                |  3745,--	BloodGlucose
                |  3744,--	Blood Glucose
                |  225664,--	Glucose finger stick
                |  220621,--	Glucose (serum)
                |  226537,--	Glucose (whole blood)
                |
                |  -- TEMPERATURE
                |  223762, -- "Temperature Celsius"
                |  676,	-- "Temperature C"
                |  223761, -- "Temperature Fahrenheit"
                |  678 --	"Temperature F"
                |
                |  )
                |) pvt
                |group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
                |order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id""")

    spark.sql("DROP TABLE IF EXISTS concepts.gcsfirstday")
    spark.sql("""CREATE TABLE concepts.gcsfirstday as
                |with base as
                |(
                |  SELECT pvt.ICUSTAY_ID
                |  , pvt.charttime
                |
                |  -- Easier names - note we coalesced Metavision and CareVue IDs below
                |  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
                |  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
                |  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes
                |
                |  -- If verbal was set to 0 in the below select, then this is an intubated patient
                |  , case
                |      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0
                |    then 1
                |    else 0
                |    end as EndoTrachFlag
                |
                |  , ROW_NUMBER ()
                |          OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn
                |
                |  FROM  (
                |  select l.ICUSTAY_ID
                |  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
                |  , case
                |      when l.ITEMID in (723,223900) then 723
                |      when l.ITEMID in (454,223901) then 454
                |      when l.ITEMID in (184,220739) then 184
                |      else l.ITEMID end
                |    as ITEMID
                |
                |  -- convert the data into a number, reserving a value of 0 for ET/Trach
                |  , case
                |      -- endotrach/vent is assigned a value of 0, later parsed specially
                |      when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
                |      when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision
                |
                |      else VALUENUM
                |      end
                |    as VALUENUM
                |  , l.CHARTTIME
                |  from CHARTEVENTS l
                |
                |  -- get intime for charttime subselection
                |  inner join icustays b
                |    on l.icustay_id = b.icustay_id
                |
                |  -- Isolate the desired GCS variables
                |  where l.ITEMID in
                |  (
                |    -- 198 -- GCS
                |    -- GCS components, CareVue
                |    184, 454, 723
                |    -- GCS components, Metavision
                |    , 223900, 223901, 220739
                |  )
                |  -- Only get data for the first 24 hours
                |  and l.charttime between b.intime and b.intime + interval '1' day
                |  -- exclude rows marked as error
                |  and l.error IS DISTINCT FROM 1
                |  ) pvt
                |  group by pvt.ICUSTAY_ID, pvt.charttime
                |)
                |, gcs as (
                |  select b.*
                |  , b2.GCSVerbal as GCSVerbalPrev
                |  , b2.GCSMotor as GCSMotorPrev
                |  , b2.GCSEyes as GCSEyesPrev
                |  -- Calculate GCS, factoring in special case when they are intubated and prev vals
                |  -- note that the coalesce are used to implement the following if:
                |  --  if current value exists, use it
                |  --  if previous value exists, use it
                |  --  otherwise, default to normal
                |  , case
                |      -- replace GCS during sedation with 15
                |      when b.GCSVerbal = 0
                |        then 15
                |      when b.GCSVerbal is null and b2.GCSVerbal = 0
                |        then 15
                |      -- if previously they were intub, but they aren't now, do not use previous GCS values
                |      when b2.GCSVerbal = 0
                |        then
                |            coalesce(b.GCSMotor,6)
                |          + coalesce(b.GCSVerbal,5)
                |          + coalesce(b.GCSEyes,4)
                |      -- otherwise, add up score normally, imputing previous value if none available at current time
                |      else
                |            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
                |          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
                |          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
                |      end as GCS
                |
                |  from base b
                |  -- join to itself within 6 hours to get previous value
                |  left join base b2
                |    on b.ICUSTAY_ID = b2.ICUSTAY_ID and b.rn = b2.rn+1 and b2.charttime > b.charttime - interval '6' hour
                |)
                |, gcs_final as (
                |  select gcs.*
                |  -- This sorts the data by GCS, so rn=1 is the the lowest GCS values to keep
                |  , ROW_NUMBER ()
                |          OVER (PARTITION BY gcs.ICUSTAY_ID
                |                ORDER BY gcs.GCS
                |               ) as IsMinGCS
                |  from gcs
                |)
                |select ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
                |-- The minimum GCS is determined by the above row partition, we only join if IsMinGCS=1
                |, GCS as MinGCS
                |, coalesce(GCSMotor,GCSMotorPrev) as GCSMotor
                |, coalesce(GCSVerbal,GCSVerbalPrev) as GCSVerbal
                |, coalesce(GCSEyes,GCSEyesPrev) as GCSEyes
                |, EndoTrachFlag as EndoTrachFlag
                |
                |-- subselect down to the cohort of eligible patients
                |from icustays ie
                |left join gcs_final gs
                |  on ie.ICUSTAY_ID = gs.ICUSTAY_ID and gs.IsMinGCS = 1
                |ORDER BY ie.ICUSTAY_ID""".stripMargin)

    spark.sql("DROP TABLE IF EXISTS concepts.oasis")
    spark.sql("""CREATE TABLE concepts.oasis as
                |
                |with surgflag as
                |(
                |  select ie.icustay_id
                |    , max(case
                |        when lower(curr_service) like '%surg%' then 1
                |        when curr_service = 'ORTHO' then 1
                |    else 0 end) as surgical
                |  from icustays ie
                |  left join services se
                |    on ie.hadm_id = se.hadm_id
                |    and se.transfertime < ie.intime + interval '1' day
                |  group by ie.icustay_id
                |)
                |, cohort as
                |(
                |select ie.subject_id, ie.hadm_id, ie.icustay_id
                |      , ie.intime
                |      , ie.outtime
                |      , adm.deathtime
                |      , unix_timestamp(ie.intime) - unix_timestamp(adm.admittime) as PreICULOS
                |      , floor( DATEDIFF(ie.intime, pat.dob) / 365.242 ) as age
                |      , gcs.mingcs
                |      , vital.heartrate_max
                |      , vital.heartrate_min
                |      , vital.meanbp_max
                |      , vital.meanbp_min
                |      , vital.resprate_max
                |      , vital.resprate_min
                |      , vital.tempc_max
                |      , vital.tempc_min
                |      , vent.vent as mechvent
                |      , uo.urineoutput
                |
                |      , case
                |          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
                |            then 1
                |          when adm.ADMISSION_TYPE is null or sf.surgical is null
                |            then null
                |          else 0
                |        end as ElectiveSurgery
                |
                |      -- age group
                |      , case
                |        when DATEDIFF(ie.intime, pat.dob) / 365.242 <= 1 then 'neonate'
                |        when DATEDIFF(ie.intime, pat.dob) / 365.242 <= 15 then 'middle'
                |        else 'adult' end as ICUSTAY_AGE_GROUP
                |
                |      -- mortality flags
                |      , case
                |          when adm.deathtime between ie.intime and ie.outtime
                |            then 1
                |          when adm.deathtime <= ie.intime -- sometimes there are typographical errors in the death date
                |            then 1
                |          when adm.dischtime <= ie.outtime and adm.discharge_location = 'DEAD/EXPIRED'
                |            then 1
                |          else 0 end
                |        as ICUSTAY_EXPIRE_FLAG
                |      , adm.hospital_expire_flag
                |from icustays ie
                |inner join admissions adm
                |  on ie.hadm_id = adm.hadm_id
                |inner join patients pat
                |  on ie.subject_id = pat.subject_id
                |left join surgflag sf
                |  on ie.icustay_id = sf.icustay_id
                |-- join to custom tables to get more data....
                |left join concepts.gcsfirstday gcs
                |  on ie.icustay_id = gcs.icustay_id
                |left join concepts.vitalsfirstday vital
                |  on ie.icustay_id = vital.icustay_id
                |left join concepts.uofirstday uo
                |  on ie.icustay_id = uo.icustay_id
                |left join concepts.ventfirstday vent
                |  on ie.icustay_id = vent.icustay_id
                |)
                |, scorecomp as
                |(
                |select co.subject_id, co.hadm_id, co.icustay_id
                |, co.ICUSTAY_AGE_GROUP
                |, co.icustay_expire_flag
                |, co.hospital_expire_flag
                |
                |-- Below code calculates the component scores needed for OASIS
                |, case when preiculos is null then null
                |     when preiculos < '0 0:10:12' then 5
                |     when preiculos < '0 4:57:00' then 3
                |     when preiculos < '1 0:00:00' then 0
                |     when preiculos < '12 23:48:00' then 1
                |     else 2 end as preiculos_score
                |,  case when age is null then null
                |      when age < 24 then 0
                |      when age <= 53 then 3
                |      when age <= 77 then 6
                |      when age <= 89 then 9
                |      when age >= 90 then 7
                |      else 0 end as age_score
                |,  case when mingcs is null then null
                |      when mingcs <= 7 then 10
                |      when mingcs < 14 then 4
                |      when mingcs = 14 then 3
                |      else 0 end as gcs_score
                |,  case when heartrate_max is null then null
                |      when heartrate_max > 125 then 6
                |      when heartrate_min < 33 then 4
                |      when heartrate_max >= 107 and heartrate_max <= 125 then 3
                |      when heartrate_max >= 89 and heartrate_max <= 106 then 1
                |      else 0 end as heartrate_score
                |,  case when meanbp_min is null then null
                |      when meanbp_min < 20.65 then 4
                |      when meanbp_min < 51 then 3
                |      when meanbp_max > 143.44 then 3
                |      when meanbp_min >= 51 and meanbp_min < 61.33 then 2
                |      else 0 end as meanbp_score
                |,  case when resprate_min is null then null
                |      when resprate_min <   6 then 10
                |      when resprate_max >  44 then  9
                |      when resprate_max >  30 then  6
                |      when resprate_max >  22 then  1
                |      when resprate_min <  13 then 1 else 0
                |      end as resprate_score
                |,  case when tempc_max is null then null
                |      when tempc_max > 39.88 then 6
                |      when tempc_min >= 33.22 and tempc_min <= 35.93 then 4
                |      when tempc_max >= 33.22 and tempc_max <= 35.93 then 4
                |      when tempc_min < 33.22 then 3
                |      when tempc_min > 35.93 and tempc_min <= 36.39 then 2
                |      when tempc_max >= 36.89 and tempc_max <= 39.88 then 2
                |      else 0 end as temp_score
                |,  case when UrineOutput is null then null
                |      when UrineOutput < 671.09 then 10
                |      when UrineOutput > 6896.80 then 8
                |      when UrineOutput >= 671.09
                |       and UrineOutput <= 1426.99 then 5
                |      when UrineOutput >= 1427.00
                |       and UrineOutput <= 2544.14 then 1
                |      else 0 end as UrineOutput_score
                |,  case when mechvent is null then null
                |      when mechvent = 1 then 9
                |      else 0 end as mechvent_score
                |,  case when ElectiveSurgery is null then null
                |      when ElectiveSurgery = 1 then 0
                |      else 6 end as electivesurgery_score
                |
                |
                |-- The below code gives the component associated with each score
                |-- This is not needed to calculate OASIS, but provided for user convenience.
                |-- If both the min/max are in the normal range (score of 0), then the average value is stored.
                |, preiculos
                |, age
                |, mingcs as gcs
                |,  case when heartrate_max is null then null
                |      when heartrate_max > 125 then heartrate_max
                |      when heartrate_min < 33 then heartrate_min
                |      when heartrate_max >= 107 and heartrate_max <= 125 then heartrate_max
                |      when heartrate_max >= 89 and heartrate_max <= 106 then heartrate_max
                |      else (heartrate_min+heartrate_max)/2 end as heartrate
                |,  case when meanbp_min is null then null
                |      when meanbp_min < 20.65 then meanbp_min
                |      when meanbp_min < 51 then meanbp_min
                |      when meanbp_max > 143.44 then meanbp_max
                |      when meanbp_min >= 51 and meanbp_min < 61.33 then meanbp_min
                |      else (meanbp_min+meanbp_max)/2 end as meanbp
                |,  case when resprate_min is null then null
                |      when resprate_min <   6 then resprate_min
                |      when resprate_max >  44 then resprate_max
                |      when resprate_max >  30 then resprate_max
                |      when resprate_max >  22 then resprate_max
                |      when resprate_min <  13 then resprate_min
                |      else (resprate_min+resprate_max)/2 end as resprate
                |,  case when tempc_max is null then null
                |      when tempc_max > 39.88 then tempc_max
                |      when tempc_min >= 33.22 and tempc_min <= 35.93 then tempc_min
                |      when tempc_max >= 33.22 and tempc_max <= 35.93 then tempc_max
                |      when tempc_min < 33.22 then tempc_min
                |      when tempc_min > 35.93 and tempc_min <= 36.39 then tempc_min
                |      when tempc_max >= 36.89 and tempc_max <= 39.88 then tempc_max
                |      else (tempc_min+tempc_max)/2 end as temp
                |,  UrineOutput
                |,  mechvent
                |,  ElectiveSurgery
                |from cohort co
                |)
                |, score as
                |(
                |select s.*
                |    , coalesce(age_score,0)
                |    + coalesce(preiculos_score,0)
                |    + coalesce(gcs_score,0)
                |    + coalesce(heartrate_score,0)
                |    + coalesce(meanbp_score,0)
                |    + coalesce(resprate_score,0)
                |    + coalesce(temp_score,0)
                |    + coalesce(urineoutput_score,0)
                |    + coalesce(mechvent_score,0)
                |    + coalesce(electivesurgery_score,0)
                |    as OASIS
                |from scorecomp s
                |)
                |select
                |  subject_id, hadm_id, icustay_id
                |  , ICUSTAY_AGE_GROUP
                |  , hospital_expire_flag
                |  , icustay_expire_flag
                |  , OASIS
                |  -- Calculate the probability of in-hospital mortality
                |  , 1 / (1 + exp(- (-6.1746 + 0.1275*(OASIS) ))) as OASIS_PROB
                |  , age, age_score
                |  , preiculos, preiculos_score
                |  , gcs, gcs_score
                |  , heartrate, heartrate_score
                |  , meanbp, meanbp_score
                |  , resprate, resprate_score
                |  , temp, temp_score
                |  , urineoutput, UrineOutput_score
                |  , mechvent, mechvent_score
                |  , electivesurgery, electivesurgery_score
                |from score
                |order by icustay_id""".stripMargin
    )
  }
}

object CommunityTableGenerator {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new CommunityTableGenerator(), System.err, args: _*)
  }
}


