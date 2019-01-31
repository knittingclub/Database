ssh grp8@ssri-midscluster1.vm.duke.edu
pyspark --master local

# q8
from pyspark.sql import SparkSession

if __name__ == "__main__":
	spark = SparkSession.builder.appName("teamProject").getOrCreate()
path="hdfs://ssri-midscluster1:9000/public/mindlinc/"

meds= spark.read.format("csv").option("header","true").load(path+"VDL2011_Meds.txt")
pDiagnose=spark.read.format("csv").option("header","true").load(path+"VDL2011_PDiagnose.txt")
type_of_p=spark.read.format("csv").option("header","true").load(path+"VDL2011_TypePatient.txt")

meds.createOrReplaceTempView("meds_table")
pDiagnose.createOrReplaceTempView("diag_table")
type_of_p.createOrReplaceTempView("patient")

q8_pat = spark.sql("SELECT DISTINCT NOTE_DESCRIPTION FROM patient")
q8_medication = spark.sql("SELECT DISTINCT Medication FROM meds_table AS m")
q8_medication.show()
q8 = spark.sql("SELECT Medication, PsyMed, Diagnosis FROM meds_table AS M JOIN diag_table AS D ON M.BackgroundID = D.BackgroundID WHERE M.PsyMed = 'Yes'")
q8.show()
q8.createOrReplaceTempView("BP_table")
sort_drug = spark.sql("SELECT Medication, COUNT(*) AS Count FROM BP_table GROUP BY Medication ORDER BY Count")
sort_drug.show()

# WHERE Diagnosis LIKE '%Hypertension%'
# var df = spark.read.parquet("/path/to/infile.parquet")

df.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("q8_medication.csv")
scp grp8@ssri-midscluster1.vm.duke.edu:/home/grp8/q8_medication.csv/part-00000-800680e5-2feb-4fa6-9371-3641fc8eb88c-c000.csv Desktop/MIDS_courses/database_system/final_project/download_from_cluster/q8_medication.csv

