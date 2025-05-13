📊 project_dados
Simulação local de um Data Lake com aplicação prática de ETL utilizando PySpark.

📌 Visão Geral
Este projeto tem como objetivo simular um ambiente de Data Lake local, aplicando processos de Extração, Transformação e Carga (ETL) com o uso do PySpark. A iniciativa visa consolidar conhecimentos em engenharia de dados, proporcionando uma compreensão prática das etapas envolvidas no processamento de grandes volumes de dados.

🧰 Tecnologias Utilizadas
Python 3.x

Apache Spark com PySpark

🗂️ Estrutura do Projeto
A organização dos diretórios e arquivos está estruturada da seguinte forma:

```
project_dados/
├── Data/                 # Dados brutos utilizados na simulação
├── DataLake/             # Armazenamento simulado do Data Lake
├── script/               # Scripts principais de ETL
├── utils.py              # Funções auxiliares para o processo
└── README.md             # Documentação do projeto
```


⚙️ Funcionalidades
Simulação de um ambiente de Data Lake local

Processos de ETL utilizando PySpark

Transformações e limpezas de dados

Armazenamento dos dados processados no Data Lake simulado

🚀 Como Executar
Clone o repositório:
```
git clone https://github.com/denilsonss/project_dados.git

cd project_dados

Crie e ative um ambiente virtual (opcional, mas recomendado):
```
```
python -m venv venv

source venv/bin/activate  # Para Linux/Mac

venv\Scripts\activate     # Para Windows

```
Instale as dependências:

```
pip install -r requirements.txt
```
Nota: Certifique-se de que o Apache Spark esteja corretamente configurado em seu ambiente.

🧪 Exemplos de Uso
Para visualizar o processo de ETL em ação, você pode executar os notebooks disponíveis na pasta script/. Eles demonstram passo a passo a extração, transformação e carga dos dados no Data Lake simulado.

📈 Resultados Esperados
Após a execução dos scripts, os dados processados estarão disponíveis na pasta DataLake/, prontos para análises posteriores ou integrações com outras ferramentas de Business Intelligence.

🤝 Contribuições
Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou pull requests com sugestões de melhorias, correções ou novas funcionalidades.
GitHub
