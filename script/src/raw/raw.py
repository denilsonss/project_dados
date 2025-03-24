from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
.appName("Laod JSON to Dataframe") \
.getOrCreate()

# Caminho do JSON e CSV
customers_path = r"C:\Users\Denil\OneDrive\Área de Trabalho\Projeto de dados\Data\customers_data_large.json"
products_path = r"C:\Users\Denil\OneDrive\Área de Trabalho\Projeto de dados\Data\products_data_large.json"
sales_path = r"C:\Users\Denil\OneDrive\Área de Trabalho\Projeto de dados\Data\sales_data_large.csv"

# Carregar o JSON e CSV em um DataFrame
df_customers = spark.read.option("multiline", "true").json(customers_path)
df_products = spark.read.option("multiline", "true").json(products_path)
df_sales = spark.read.option("header", "true").csv(sales_path)


# Write Folder in Parquet
#path = r"C:\Users\Denil\OneDrive\Área de Trabalho\DataLake\raw\\"
#df_customers.write.mode("overwrite").parquet(path + "customers")
#df_products.write.parquet(path + "products")
#df_sales.write.parquet(path + "sales")

#print("Dados salvos no formato Parquet com sucesso!")

# Encerrar a sessão (boa prática)
spark.stop()