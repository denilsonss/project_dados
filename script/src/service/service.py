import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

import json
from utils import load_file
from utils import save

with open(sys.path[-1] + "\\script\\src\\service\\config.json", encoding="utf-8") as f:
    config = json.load(f)

camada = "service"
dfs = load_file(config)  

print(dfs)


# ✅ Alinhe os nomes com o que vem do DFS
if all(tbl in dfs for tbl in ["tb_sales", "tb_customer", "tb_product"]):
    df_sales = dfs["tb_sales"]
    df_customers = dfs["tb_customer"]
    df_products = dfs["tb_product"]

    # 🔄 Funções de transformação
    def unificar_dados(df_sales, df_customers, df_products):
        return df_sales.join(
            df_customers.select("customer_id", "email", "name", "region"),
            "customer_id", "left"
        ).join(
            df_products.select("product_id", "product_name", "category"),
            "product_id", "left"
        ).select(
            "order_id", "customer_id", "name", "email", "region",
            "product_id", "product_name", "category", "quantity", "price", "date"
        )

    def total_vendas_por_pedido(df):
        return df.withColumn("total_vendas", F.round(F.col("quantity") * F.col("price"), 2))

    def total_vendas_por_regiao(df):
        return df.groupBy("region").agg(
            F.round(F.sum(F.col("quantity") * F.col("price")), 2).alias("total_vendas")
        ).orderBy("total_vendas")

    def total_vendas_por_produto(df):
        return df.groupBy("product_id", "product_name").agg(
            F.round(F.sum(F.col("quantity") * F.col("price")), 2).alias("total_vendas")
        ).orderBy("total_vendas")

   
    df_unificado = unificar_dados(df_sales, df_customers, df_products)
    df_pedido = total_vendas_por_pedido(df_unificado)
    df_regiao = total_vendas_por_regiao(df_unificado)
    df_produto = total_vendas_por_produto(df_unificado)

    
    dfs_resultados = {
    "vendas_unificadas": df_unificado,
    "vendas_por_pedido": df_pedido,
    "vendas_por_regiao": df_regiao,
    "vendas_por_produto": df_produto
}

    
    save(dfs_resultados, config, camada)

else:
    print("⚠️ As tabelas 'tb_sales', 'tb_customer' e 'tb_product' não foram encontradas no dicionário.")
