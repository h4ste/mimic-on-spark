# MIMIC-on-SparkSQL
Small Scala/JVM project for loading MIMIC-III datafiles into a persistent Spark SQL database.

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

These scripts were used to prepare the data for our JAMIA paper, ["A Customizable Deep Learning Model for Nosocomial Risk Prediction from Critical Care Notes with Indirect Supervision"](https://doi.org/10.1093/jamia/ocaa004). 

# Loading data
To load data into a persistant SparkSQL database, first download the data from: [PhysioNet](https://mimic.physionet.org/gettingstarted/dbsetup/)
Then, you can load the MIMIC-III data with:
```shell
$ sh scripts/load_mimic_data.sh PATH/TO/MIMIC/DATA [-g] [-d DATABASENAME] [-h|--help]
```
Where `-g` indicates that the MIMIC data files are gzipped, and `-d` indicates the name of the MIMIC-III database that will be created (default: 'mimic').
Windows versions are available in `scripts/load_mimic_data.bat`.

# Loading Community data
Using the CCS data from the [Community MIMIC Code Repository](https://github.com/MIT-LCP/mimic-code/tree/master/concepts/diagnosis), the script
`$ scripts/load_css.sh path/to/css/files` will load the CCS data.
Likewise, the script
`$ scripts/load_community_views.sh` will load various [community view tables](https://github.com/MIT-LCP/mimic-code/tree/master/concepts) (used for examples scripts).

# Example scripts
Three example scripts are available:
- `scripts/extract_haaki.sh path/to/output/dir ` will extract Hospital Aquired Acute Kidney Injury staging information to the given directory
- `scripts/extract_hapi.sh path/to/output/dir ` will extract Hospital Aquired Pressure Injury staging information to the given directory
- `scripts/extract_haa.sh path/to/output/dir ` will extract Hospital Aquired Anemia severity information to the given directory
Windows versions are also available, various options can be viewed with by invoking the scripts with `[-h|--help]`.
