# src/io_utils/data_handler.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    ArrayType,
    DateType,
    FloatType,
    TimestampType,
    BooleanType,
)
from pyspark.errors import AnalysisException
import logging

# Configuração centralizada do logging
logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pagamentos_pedidos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),  # UUID como string
                StructField("forma_pagamento", StringType(), True),  # Pix, Boleto, Cartão
                StructField("valor_pagamento", FloatType(), True),
                StructField("status", BooleanType(), True),
                StructField("data_processamento", TimestampType(), True),
                StructField(
                    "avaliacao_fraude",
                    StructType(
                        [
                            StructField("status_fraude", BooleanType(), True),
                            StructField("score_fraude", FloatType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )
    
    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType(
            [
                StructField("id_pedido", StringType(), True),
                StructField("produto", StringType(), True),
                StructField("valor_unitario", FloatType(), True),
                StructField("quantidade", LongType(), True),
                StructField("data_criacao", TimestampType(), True),
                StructField("uf", StringType(), True),
                StructField("id_cliente", LongType(), True),
            ]
        )
    
    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega o dataframe de clientes a partir de um arquivo JSON."""
        schema = self._get_schema_pagamentos()
        return self.spark.read.option("compression", "gzip").json(path, schema=schema)

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        """Carrega o dataframe de pedidos a partir de um arquivo CSV."""
        schema = self._get_schema_pedidos()
        try:
            return self.spark.read.option("compression", "gzip").csv(
                path, header=True, schema=schema, sep=";"
            )
        except AnalysisException as e:
            if "PATH_NOT_FOUND" in str(e):
                logger.error(f"Arquivo não encontrado: {path}")

            raise Exception(f"Erro ao carregar pedidos: {e}")

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").parquet(path)
        print(f"Dados salvos com sucesso em: {path}")
