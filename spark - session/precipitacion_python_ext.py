from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col

# Crear una SparkSession
spark = SparkSession.builder \
    .appName("Completar valores nulos y ajustar separación") \
    .getOrCreate()

# Ruta del archivo CSV de entrada
input_csv_path = "/home/xmedina/Escritorio/tfm/inamhi-precipitacion-2019diciembre.csv"

# Ruta del archivo CSV de salida
output_csv_path = "/home/xmedina/Escritorio/tfm/inamhi-precipitacion-2019diciembre_out.csv"

# Leer el archivo CSV con separación por ';'
df = spark.read.csv(input_csv_path, header=True, inferSchema=True, sep=';')

# Forzar las columnas de datos numéricos a tipo decimal (DoubleType)
columns_to_convert = ["columna_mes1", "columna_mes2", "columna_mes3"]  # Ajusta con tus columnas
for column in columns_to_convert:
    df = df.withColumn(column, col(column).cast(DoubleType()))

# Completar valores nulos con 0
filled_df = df.fillna(0)

# Guardar el DataFrame resultante en un nuevo archivo CSV con separación por ';'
filled_df.write.csv(output_csv_path, header=True, mode="overwrite", sep=';')

# Detener la SparkSession -
spark.stop()
