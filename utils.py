import json
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Carregando e salvado dados nas camadas ") \
    .getOrCreate()

def get_name_file(path_source: str):
    return [os.path.join(path_source, file_name) for file_name in os.listdir(path_source)]

def load_file(config: dict):
    dfs = {}

    for table, params in config.items():
        file_type = params["file_type"].lower()
        path_source = params["path_source"]
        options = params.get("options", {})
        match_table = params.get("match_table_in_filename", False)

        if match_table:
            files = get_name_file(path_source)
            for file in files:
                if table.lower().split("_")[1] in os.path.basename(file).lower():
                    print(f"🔹 Carregando arquivo: {file}")
                    df = spark.read.options(**options).format(file_type).load(file)
                    dfs[table] = df
        else:
            print(f"🔹 Carregando pasta inteira de {table}: {path_source}")
            df = spark.read.options(**options).format(file_type).load(path_source)
            dfs[table] = df

    return dfs

def save(dfs: dict, config: dict, camada: str ):
    for table, df in dfs.items():
        target_path = config[table]["path_target"] +f"\\{camada}"+ f"\\{table}"
        os.makedirs(target_path, exist_ok=True)
        print(f"💾 Salvando {table} em {target_path}")
        df.write.mode("overwrite").parquet(target_path)

if __name__ == "__main__":
    with open("C:\\Users\\Denil\\OneDrive\\Área de Trabalho\\Projeto de dados\\config.json", encoding="utf-8") as f:
        config = json.load(f)

    dfs = load_file(config)
    save(dfs, config)