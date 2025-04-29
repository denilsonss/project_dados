import sys
import os
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

import json
from utils import load_file
from utils import save

with open(sys.path[-1] + "\\script\\src\\curated\\config.json", encoding="utf-8") as f:
        config = json.load(f)
camada = "curated"
dfs = load_file(config)  

def tratar_dados(df):
    # Lista de colunas e seus tipos desejados
    colunas_para_converter = {
        "customer_id": IntegerType(),
        "product_id": IntegerType(),
        "order_id": IntegerType(),
        "quantity": IntegerType(),
        "price": DoubleType(),
        "date": "date"
    }

    for coluna, tipo in colunas_para_converter.items():
        if coluna in df.columns:  # Verifica se a coluna existe
            df = df.withColumn(coluna, F.col(coluna).cast(tipo))  # Aplica o cast
    
    # Normalização da coluna 'date', se a coluna existir
    if "date" in df.columns:
        df = df.withColumn("date", F.date_format("date", "dd/MM/yyyy"))
    
    return df

dfs_tratados = {}
for table_name, df in dfs.items():
    print(f"Tratando a tabela: {table_name}")
    
    df_tratado = tratar_dados(df)
    dfs_tratados[table_name] = df_tratado


save(dfs_tratados, config, camada)