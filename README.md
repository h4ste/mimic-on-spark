# mimic-on-spark
Small Java/Scala project for loading MIMIC-III datafiles into a persistent Spark SQL database.

# Compiling
To compile, run:
```shell
./gradlew jar
```
This will create a scripts folder in the current directory containing the following data-loading scripts in Unix (`.sh`) and Windows (`.bat`) variants:
- load_mimic_data: loads MIMIC-III CSV files (gzipped or unzipped) into the persistent SparkSQL tables
- load_ccs: loads CSS CSV files (gzipped or unzipped) into the persistent SparkSQL tables
as well as the following example application scripts:
- extract_haaki: detects hospital acquired acute kidney injuries (HAAKIs) and produces timestampped CSVs
- extract_hapi: detects hospital acquired pressure injuries (HAPIs) and produces timestampped CSVs
- extract_haa: detects hospital acquired anemia (HAA) and produces timestampped CSVs

# Loading data
More information coming soon!
