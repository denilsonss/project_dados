
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .appName("Load JSON to DataFrame") \
    .getOrCreate()


# Caminho do arquio parquet no meu datalake
path = r"C:\\Users\Denil\\OneDrive\\Área de Trabalho\\Projeto de dados\\DataLake\\Curated\\"


# Carregar o arquivo parquet no dataframe
df_costumers = spark.read.parquet(path + "customers\\part-00000-c52d2026-9851-4499-8008-5f61c3146eac-c000.snappy.parquet")
df_products = spark.read.parquet(path + "products\\part-00000-bf95edc1-c265-4e41-9f21-8db64f806616-c000.snappy.parquet")
df_sales = spark.read.parquet(path + "sales\\part-00000-031a0467-a312-4b80-b27e-ad56847e4437-c000.snappy.parquet")

#Agregar, modelar, otimizar performance

#tabela unificada
df_dados_unificados = df_sales.join(
    df_costumers.select("customer_id","email","name","region"),
    "customer_id",
    "left"

).join(
    df_products.select("category", "product_id", "product_name"),
    "product_id",
    "left"
).select("order_id","customer_id", "name","email","region", "product_id","product_name","category","quantity","price","date")


#total de vendas por pedido

df_vendaPorPedido = df_dados_unificados.withColumn("total_vendas", round(col("quantity") * col("price"),2))

#Vendas totais por regiao

df_vendaPorRegion = df_dados_unificados.groupBy("region") \
 .agg(
    round(sum(col("quantity") * col("price")), 2).alias("total_vendas")
).orderBy("total_vendas")

#Vendas totais por produto
df_vendaPorProduto = df_dados_unificados.groupBy("product_id","product_name") \
 .agg(
    round(sum(col("quantity") * col("price")), 2).alias("total_vendas")
).orderBy("total_vendas")


path = r"C:\\Users\\Denil\\OneDrive\\Área de Trabalho\\Projeto de dados\\datalake\\service\\"

df_dados_unificados.write.parquet(path + "dados_unificados")
df_vendaPorPedido.write.parquet(path + "venda_por_pedido")
df_vendaPorRegion.write.parquet(path + "venda_por_regiao")
df_vendaPorProduto.write.parquet(path + "venda_por_produto")
