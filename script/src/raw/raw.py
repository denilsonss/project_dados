import sys
import os

# Adiciona o caminho da raiz do projeto ao sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

import json
from utils import load_file
from utils import save

with open("C:\\Users\\Denil\\OneDrive\\Área de Trabalho\\Projeto de dados\\script\\src\\raw\\config.json", encoding="utf-8") as f:
        config = json.load(f)
camada = "raw"
dfs = load_file(config)
save(dfs, config, camada)
