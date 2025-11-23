# src/processing/transformations.py
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, current_timestamp


class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def add_valor_total_pedidos(self, pedidos_df: DataFrame) -> DataFrame:
        """Adiciona a coluna 'valor_total' (valor_unitario * quantidade) ao DataFrame de pedidos."""
        return pedidos_df.withColumn(
            "valor_total", F.col("valor_unitario") * F.col("quantidade")
        )

    def get_top_10_clientes(self, pedidos_df: DataFrame) -> DataFrame:
        """Calcula o valor total de pedidos por cliente e retorna os 10 maiores."""
        return (
            pedidos_df.groupBy("id_cliente")
            .agg(F.sum("valor_total").alias("valor_total"))
            .orderBy(F.desc("valor_total"))
            .limit(10)
        )   
    
    
    def join_pedidos_pagamentos(
        self, pedidos_df: DataFrame, pagamentos_df: DataFrame
    ) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e pagamentos."""
        return pedidos_df.join(
            pagamentos_df, pagamentos_df.id_pedido == pedidos_df.id_pedido, "inner"
        ).filter(
            (col("status") == False) 
            & (col("avaliacao_fraude.status_fraude") == False)
            & (year(col("data_criacao")) == year(current_timestamp()))
        ).select(
            pedidos_df.id_pedido,             #1. Identificador do pedido (id pedido) 
            pedidos_df.uf,                    #2. Estado (UF) onde o pedido foi feito 
            pagamentos_df.forma_pagamento,    #3. Forma de pagamento
            pedidos_df.valor_total,           #4. Valor total do pedido 
            pedidos_df.data_criacao,          #5. Data do pedido 
        ).orderBy("uf", "forma_pagamento", "data_criacao")
    
    '''
    def join_pedidos_clientes(
        self, pedidos_df: DataFrame, clientes_df: DataFrame
    ) -> DataFrame:
        """Faz a junção entre os DataFrames de pedidos e clientes."""
        return pedidos_df.join(
            clientes_df, clientes_df.id == pedidos_df.id_cliente, "inner"
        ).select(
            pedidos_df.id_cliente,
            clientes_df.nome,
            clientes_df.email,
            pedidos_df.valor_total,
        )
    '''