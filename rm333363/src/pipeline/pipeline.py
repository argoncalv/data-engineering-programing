# src/pipeline/pipeline.py
from pyspark.sql import SparkSession
from io_utils.data_handler import DataHandler
from processing.transformations import Transformation
import logging

logger = logging.getLogger(__name__)


class Pipeline:
    """
    Encapsula a lógica de execução do pipeline de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_handler = DataHandler(self.spark)
        self.transformer = Transformation()

    def run(self, config):
        """
        Executa o pipeline completo: carga, transformação, e salvamento.
        """
        logger.info("Pipeline iniciado...")

        logger.info("Abrindo o dataframe de pagamentos")
        path_pagamentos = config["paths"]["pagamentos"]
        #logger.info(f"Caminho dos pagamentos: {path_pagamentos}")
        pagamentos_df = self.data_handler.load_pagamentos(path=path_pagamentos)
        #logger.info("Abrindo o dataframe de clientes")
        #path_clientes = config["paths"]["clientes"]
        #clientes_df = self.data_handler.load_clientes(path=path_clientes)

        pagamentos_df.show(5, truncate=False)
        #clientes_df.show(5, truncate=False)

        logger.info("Abrindo o dataframe de pedidos")
        path_pedidos = config["paths"]["pedidos"]
        compression_pedidos = config["file_options"]["pedidos_csv"]["compression"]
        header_pedidos = config["file_options"]["pedidos_csv"]["header"]
        separator_pedidos = config["file_options"]["pedidos_csv"]["sep"]

        try:
            pedidos_df = self.data_handler.load_pedidos(
                path=path_pedidos,
                compression=compression_pedidos,
                header=header_pedidos,
                sep=separator_pedidos,
            )
        except Exception as e:
            logger.error(f"Problemas ao carregar dados de pedidos: {e}")
            return  # Interrompe o pipeline se os pedidos não puderem ser carregados

        logger.info("Adicionando a coluna valor_total")
        pedidos_com_valor_total_df = self.transformer.add_valor_total_pedidos(
            pedidos_df
        )
        pedidos_com_valor_total_df.show(5, truncate=False)

        #logger.info(
        #    "Calculando o valor total de pedidos por cliente e filtrar os 10 maiores"
        #)
        #top_10_clientes_df = self.transformer.get_top_10_clientes(
        #    pedidos_com_valor_total_df
        #)

        #top_10_clientes_df.show(10, truncate=False)

        logger.info("Fazendo a junção dos dataframes")
        resultado_final_df = self.transformer.join_pedidos_pagamentos(
            pedidos_com_valor_total_df, pagamentos_df
        )
        #resultado_final_df = self.transformer.join_pedidos_clientes(
        #    top_10_clientes_df, clientes_df
        #)

        resultado_final_df.show(20, truncate=False)

        logger.info("Escrevendo o resultado em parquet")
        path_output = config["paths"]["output"]
        self.data_handler.write_parquet(df=resultado_final_df, path=path_output)
