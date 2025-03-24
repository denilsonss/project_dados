from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
.appName("Laod JSON to Dataframe") \
.getOrCreate()

# Caminho do arquio parquet no meu datalake
path = r"C:\\Users\\Denil\\OneDrive\\Área de Trabalho\\Projeto de dados\\DataLake\raw\\"

# Carregar o arquivo parquet no dataframe
df_customers = spark.read.parquet(path + "customers\\part-00000-b5ff367a-859e-4fac-811d-d1f016427500-c000.snappy.parquet")
df_products = spark.read.parquet(path + "products\\part-00000-0117e58d-1601-4fa9-bf88-2fea6af7d316-c000.snappy.parquet")
df_sales = spark.read.parquet(path + "sales\\part-00000-ac7fdaa7-e532-4dbb-a8e2-9aeae70c549a-c000.snappy.parquet")

"""#Printando as tabelas e seus schemas 
df_customers.show()
df_customers.printSchema()
df_products.show()
df_products.printSchema()
df_sales.show()
df_sales.printSchema()"""

#convertendo tipo de dados

df_customers = df_customers.withColumn("customer_id", col("customer_id").cast("int")) # Dado estava em tipo Long e coloquei para tipo Int
df_products = df_products.withColumn("product_id", col("product_id").cast("int")) # Dado estava em tipo Long e coloquei para tipo Int

df_sales = df_sales.withColumn("order_id", col("order_id").cast("int")) \
 .withColumn("product_id", col("product_id").cast("int")) \
 .withColumn("customer_id", col("customer_id").cast("int")) \
 .withColumn("date", col("date").cast("date")) \
 .withColumn("quantity", col("quantity").cast("int")) \
 .withColumn("price", col("price").cast("double"))

#normalização da coluna date, utilizando formato Dia/Mês/Ano
df_sales = df_sales.withColumn("date", date_format("date", "dd/MM/yyyy"))

"""path = r""C:\\Users\\Denil\\OneDrive\\Área de Trabalho\\Projeto de dados\\DataLake\\Curated\\"

df_customers.write.parquet(path + "customers")
df_products.write.parquet(path + "products")
df_sales.write.parquet(path + "sales")

df_customers.show()
df_customers.printSchema()
df_products.show()
df_products.printSchema()
df_sales.show()
df_sales.printSchema()"""