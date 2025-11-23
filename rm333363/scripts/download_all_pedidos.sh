#!/bin/bash

# Cria a pasta de destino se não existir
mkdir -p ./data/input/pedidos

# URL da página do repositório no GitHub
repo_url="https://github.com/infobarbosa/datasets-csv-pedidos/tree/main/data/pedidos"
base_url="https://raw.githubusercontent.com/infobarbosa/datasets-csv-pedidos/main/data/pedidos"

# Extrai a lista de arquivos pedidos*.csv.gz da página HTML do GitHub
files=$(curl -s $repo_url | grep -o 'pedidos-[0-9-]*\.csv\.gz')

# Loop para baixar cada arquivo encontrado
for file in $files; do
  echo "Baixando $file ..."
  curl -L -o ./data/input/pedidos/$file "$base_url/$file"
done
