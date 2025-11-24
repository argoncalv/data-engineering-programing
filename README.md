*Projeto PySpark — FIAP*
Data Engineering Programming — Trabalho Final

Autor
Alexandre Rodrigues Gonçalves
Disciplina: Data Engineering Programming
FIAP — 2025

Professor: Marcelo Barbosa Pinto

Objetivo
Este repositório contém a implementação completa do trabalho final da disciplina Data Engineering Programming, cujo objetivo é desenvolver um projeto PySpark aplicando conceitos de:

Schemas explícitos
Arquitetura orientada a objetos (POO)
Injeção de dependências
Orquestração de pipelines
Logging e tratamento de erros
Testes automatizados com pytest
Empacotamento profissional de aplicação Python
O projeto foi estruturado com foco em legibilidade, performance e boas práticas de engenharia de dados.


Escopo do Problema
A alta gestão da empresa solicitou um relatório contendo pedidos de venda que:
Tiveram pagamento recusado (status = false);
Foram classificados como legítimos na avaliação antifraude (fraude = false);
Foram realizados apenas no ano de 2025;

O relatório deve conter:

Identificador do pedido
UF (estado)
Forma de pagamento
Valor total
Data do pedido

O dataset final deve ser:
Ordenado por: UF → forma de pagamento → data do pedido
Gravado em formato Parquet

Arquitetura do Projeto

A aplicação segue uma arquitetura modular baseada em POO e injeção de dependências.
Os pacotes principais são:

src/
 ├── config/               # Configurações centralizadas
 ├── spark/                # Gerenciamento da SparkSession
 ├── io/                   # Leitura e escrita de dados
 ├── business/             # Regras de negócio (filtros, transformações)
 ├── orchestrator/         # Orquestração do pipeline end-to-end
 ├── main.py               # Aggregation Root — ponto de entrada da aplicação
tests/
 └── test_business.py      # Teste unitário com pytest

Tecnologias Utilizadas
Python 3.10+
Apache Spark (PySpark)
pytest
logging
POO + Dependency Injection
Formatos: JSON, CSV, Parquet

Datasets

Os datasets oficiais do trabalho:

✔ Pagamentos (JSON)

Repositório:
https://github.com/infobarbosa/dataset-json-pagamentos

Pasta utilizada:
dataset-json-pagamentos/data/pagamentos

✔ Pedidos (CSV)

Repositório:
https://github.com/infobarbosa/datasets-csv-pedidos

Pasta utilizada:
datasets-csv-pedidos/data/pedidos/

Como Executar o Projeto
1. Criar ambiente virtual (opcional)
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

2. Instalar dependências
pip install -r requirements.txt

3. Executar o pipeline

O arquivo main.py é o Aggregation Root, onde todas as dependências são instanciadas.

python src/main.py


A saída final será gerada em Parquet no diretório configurado em config/.


Testes Automatizados

Este projeto contém pelo menos um teste unitário da camada de negócios, conforme exigido no trabalho.

Para rodar os testes:

pytest -v

Estrutura de Classes (Resumo)

✔ Configurações
Classe de configuração global do projeto.

✔ SparkSession
Classe dedicada à criação e gerenciamento da sessão Spark.

✔ I/O (Leitura e Escrita)
Classes responsáveis por carregar datasets de pagamentos e pedidos.

Escrita final do resultado em Parquet.

✔ Lógica de Negócio
Implementa filtros para:
Pagamentos recusados
Fraudes = false
Ano de 2025
Joins
Ordenação
Logging
Tratamento de exceções com try/except

✔ Orquestração
Classe responsável por encadear todo o pipeline desde a leitura até a persistência.

Logging

O projeto utiliza:

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


Tratamento de Erros
Todo processamento crítico da camada de negócio utiliza try/except com logging de erros.

Empacotamento da Aplicação

Inclui:
pyproject.toml
requirements.txt
MANIFEST.in
README.md

Material de Apoio

Todo conteúdo pedagógico utilizado como referência está disponível em:
https://github.com/infobarbosa/pyspark-poo

