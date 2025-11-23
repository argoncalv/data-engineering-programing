#!/bin/bash

# Cria a pasta de destino se não existir
mkdir -p ./data/input/pagamentos

# URL da página do repositório no GitHub
repo_url="https://github.com/infobarbosa/dataset-json-pagamentos/tree/main/data/pagamentos"

# Extrai a lista de arquivos da página HTML do GitHub
files=$(curl -s $repo_url | grep -o 'pagamentos-[0-9-]*\.json\.gz')

# Loop para baixar cada arquivo encontrado
for file in $files; do
  echo "Baixando $file ..."
  curl -L -o ./data/input/pagamentos/$file \
    https://raw.githubusercontent.com/infobarbosa/dataset-json-pagamentos/main/data/pagamentos/$file
done
