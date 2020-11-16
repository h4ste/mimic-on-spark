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
To load data into a persistant SparkSQL database, first download the data from: [https://mimic.physionet.org/gettingstarted/dbsetup/](PhysioNet)
Then, you can load the MIMIC-III data with:
```shell
$ sh scripts/load_mimic_data.sh PATH/TO/MIMIC/DATA [-g] [-d DATABASENAME] [-h|--help]
```
Where `-g` indicates that the MIMIC data files are gzipped, and `-d` indicates the name of the MIMIC-III database that will be created (default: 'mimic').
Windows versions are available in `scripts/load_mimic_data.bat`.
