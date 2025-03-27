from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, max, min, round, split, sum, to_date, when

# Initialize Spark session
spark = SparkSession.builder.appName("info_corridas_do_dia_etl_process").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Load input data from CSV file
input_file = 'info_transportes.csv'
source_df = spark.read.csv(input_file, header=True, inferSchema=True, sep=';')

# Extract only the date part from datetime columns
source_df = source_df.withColumn("DATA_INICIO", split(source_df.DATA_INICIO, " ").getItem(0))
source_df = source_df.withColumn("DATA_FIM", split(source_df.DATA_FIM, " ").getItem(0))

# Convert date columns to proper date format
source_df = source_df.withColumn("DATA_INICIO", to_date("DATA_INICIO", "MM-dd-yyyy"))
source_df = source_df.withColumn("DATA_FIM", to_date("DATA_FIM", "MM-dd-yyyy"))

# Fill null values in the PROPOSITO column with 'Não informado'
source_df = source_df.na.fill(value="Não informado", subset=["PROPOSITO"])

# Create new columns to categorize rides
source_df = source_df.withColumn("QTD_NEGOCIO", when(source_df.CATEGORIA == "Negocio", 1).otherwise(0))
source_df = source_df.withColumn("QTD_PESSOAL", when(source_df.CATEGORIA == "Pessoal", 1).otherwise(0))
source_df = source_df.withColumn("QTD_REUNIAO", when(source_df.PROPOSITO == "Reunião", 1).otherwise(0))

# Display the transformed data
source_df.show(100, truncate=False)
source_df.printSchema()

# Aggregate total number of rides per day
count_value = source_df.groupBy("DATA_INICIO").count().withColumnRenamed("count", "QT_CORR")
stage_df = source_df.join(count_value, on="DATA_INICIO", how="left")

# Sum of business-related rides per day
sum_negocio = source_df.groupBy("DATA_INICIO").agg(sum("QTD_NEGOCIO").alias("QT_CORR_NEG"))
stage_df = stage_df.join(sum_negocio, on="DATA_INICIO", how="left")

# Sum of personal rides per day
sum_pessoal = source_df.groupBy("DATA_INICIO").agg(sum("QTD_PESSOAL").alias("QT_CORR_PESS"))
stage_df = stage_df.join(sum_pessoal, on="DATA_INICIO", how="left")

# Maximum distance traveled per day
max_distancia = source_df.groupBy("DATA_INICIO").agg(max("DISTANCIA").alias("VL_MAX_DIST"))
stage_df = stage_df.join(max_distancia, on="DATA_INICIO", how="left")

# Minimum distance traveled per day
min_distancia = source_df.groupBy("DATA_INICIO").agg(min("DISTANCIA").alias("VL_MIN_DIST"))
stage_df = stage_df.join(min_distancia, on="DATA_INICIO", how="left")

# Average distance traveled per day
avg_distancia = source_df.groupBy("DATA_INICIO").agg(round(avg("DISTANCIA"), 2).alias("VL_AVG_DIST"))
stage_df = stage_df.join(avg_distancia, on="DATA_INICIO", how="left")

# Count of rides with purpose as "Reunião"
count_reuniao_true = source_df.filter(col("QTD_REUNIAO") == 1).groupBy("DATA_INICIO").count().withColumnRenamed("count", "QT_CORR_REUNI")
stage_df = stage_df.join(count_reuniao_true, on="DATA_INICIO", how="left")
stage_df = stage_df.na.fill(value=0, subset=["QT_CORR_REUNI"])

# Count of rides that are NOT for "Reunião"
count_reuniao_false = source_df.filter(col("QTD_REUNIAO") == 0).groupBy("DATA_INICIO").count().withColumnRenamed("count", "QT_CORR_NAO_REUNI")
stage_df = stage_df.join(count_reuniao_false, on="DATA_INICIO", how="left")
stage_df = stage_df.na.fill(value=0, subset=["QT_CORR_NAO_REUNI"])

# Display final stage data
stage_df.show(200, truncate=False)
stage_df.printSchema()

# Select relevant columns, rename, and remove duplicates
info_corridas_do_dia = stage_df.select("DATA_INICIO", "QT_CORR", "QT_CORR_NEG", "QT_CORR_PESS", "VL_MAX_DIST", "VL_MIN_DIST", "VL_AVG_DIST", "QT_CORR_REUNI", "QT_CORR_NAO_REUNI").distinct().withColumnRenamed("DATA_INICIO", "DT_REFE")

# Sort by date and display final processed data
info_corridas_do_dia.sort("DT_REFE").show(100, truncate=False)

# Write the processed data to CSV output
info_corridas_do_dia.write.options(header='True', delimiter=',').mode("overwrite").csv("/home/oliveiraleo135/Documentos/github/santander_code_assignment/info_corridas_do_dia/output")
