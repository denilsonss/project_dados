import json
import os
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Carregando e salvado dados nas camadas ") \
    .getOrCreate()

def get_name_file(path_source: str):
    return [os.path.join(path_source, file_name) for file_name in os.listdir(path_source)]

def load_file(config: dict):
    dfs = {}
    for table, params in config.items():
        if "path_source" not in params:
            print(f"⏭️ Ignorando {table} (sem 'path_source')")
            continue

        path_source = os.path.join(sys.path[-1], params["path_source"].lstrip("\\/"))
        print(f"🔹 Carregando pasta inteira de {table}")

        file_type = params.get("file_type", "parquet").lower()
        options = params.get("options", {})

        df = spark.read.options(**options).format(file_type).load(path_source)
        dfs[table] = df

    return dfs

def save(dfs: dict, config: dict, camada: str):
    for table, df in dfs.items():
        if table not in config or "path_target" not in config[table]:
            print(f"⏭️ Ignorando {table} (sem 'path_target')")
            continue

        target_path = os.path.join(
            sys.path[-1],
            config[table]["path_target"].lstrip("\\/"),
            camada,
            table
        )
        os.makedirs(target_path, exist_ok=True)
        print(f"💾 Salvando {table}")
        df.write.mode("overwrite").parquet(target_path)

