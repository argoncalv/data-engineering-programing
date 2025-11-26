# tests/test_transformations.py
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, DoubleType, TimestampType, BooleanType
from datetime import datetime
from src.processing.transformations import Transformation




@pytest.fixture(scope="session")
def spark_session():
    """
    Cria uma SparkSession para ser usada em todos os testes.
    A sessão é finalizada automaticamente ao final da execução dos testes.
    """
    spark = SparkSession.builder \
        .appName("PySpark Unit Tests") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_join_pedidos_pagamentos(spark_session):
    """
    Testa a função join_pedidos_pagamentos para garantir que o layout e o conteúdo da saída estão corretos.
    """
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    transformer = Transformation()

    schema_entrada = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
    ])
    dados_entrada = [
        ("Produto A", 10.0, 2),
        ("Produto B", 5.5, 3),
        ("Produto C", 100.0, 1)
    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)

    schema_esperado = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        ("Produto A", 10.0, 2, 20.0),
        ("Produto B", 5.5, 3, 16.5),
        ("Produto C", 100.0, 1, 100.0)
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)

    # 2. Act (Executar a função a ser testada)
    df_resultado = transformer.add_valor_total_pedidos(df_entrada)

    # 3. Assert (Verificar se o resultado é o esperado)
    # Coletamos os dados dos DataFrames para comparar como listas de dicionários
    resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x['produto'])
    esperado_coletado = sorted([row.asDict() for row in df_esperado.collect()], key=lambda x: x['produto'])

    assert df_resultado.count() == df_esperado.count(), "O número de linhas não corresponde ao esperado."
    assert df_resultado.columns == df_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "O conteúdo dos DataFrames não é igual."



def test_add_valor_total_pedidos(spark_session):
    """
    Testa a função add_valor_total_pedidos para garantir que a coluna 'valor_total'
    é calculada corretamente.
    """
    # 1. Arrange (Preparar os dados de entrada e o resultado esperado)
    transformer = Transformation()
    
    
    schema_entrada_pagamentos = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_pagamento", FloatType(), True),
        StructField("status", BooleanType(), True),
        StructField("data_processamento", TimestampType(), True),
        StructField(
            "avaliacao_fraude",
            StructType([
                StructField("status_fraude", BooleanType(), True),
                StructField("score_fraude", FloatType(), True),
            ]),
            True
        )
    ])

    dados_entrada_pagamentos = [
        ("7126ed88-9cf3-40e8-b12f-ec6aa39bb771","CARTAO_CREDITO",700.0,False,datetime(2025,3,10,14,5,25),(True, None)),
        ("da579fb7-8446-4246-9f3c-e655a6389be4","BOLETO",285.0,True,datetime(2025,3,13,18,49,43),(None, None)),
        ("e495a333-0360-4c5c-9f0e-2705212ebc2a","CARTAO_CREDITO",600.0,False,datetime(2025,3,19,19,32,26),(True, None)),
        ("3d984f23-9dfb-47f1-bbfd-f8fc1bc0c841","PIX",2090.0,False,datetime(2025,3,21,9,39,5),(None, None)),
        ("143fa00b-0b40-4b01-b5ff-879a23210e25","CARTAO_CREDITO",500.0,False,datetime(2025,3,17,6,37,28),(None, None)),
        ("4c9f87cb-7dde-4dea-81c7-27b50452b8ac","PIX",2375.0,True,datetime(2025,3,8,21,29,3),(None, None)),
        ("7bb7e1df-41aa-44d7-bf7e-7658d0c5c7d8","CARTAO_CREDITO",300.0,False,datetime(2025,3,19,17,14,7),(None, None)),
        ("d034ddd6-8deb-43f2-bf6e-890166ae39fb","CARTAO_CREDITO",1100.0,True,datetime(2025,3,8,12,26,38),(None, None)),
        ("d77c6ec6-2a9b-43eb-ba21-3842fb44327f","CARTAO_CREDITO",2500.0,False,datetime(2025,3,10,16,19,40),(None, None)),
        ("680f1a90-e015-45f4-a300-b399f2ae5eef","PIX",85.5,False,datetime(2025,3,29,7,30,43),(None, None))
    ]


    df_pagamentos_entrada = spark_session.createDataFrame(dados_entrada_pagamentos, schema_entrada_pagamentos)


    schema_pedidos_entrada = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
        StructField("id_cliente", LongType(), True),
        StructField("valor_total", DoubleType(), True)
    ])

    dados_pedidos_entrada = [
        ("7126ed88-9cf3-40e8-b12f-ec6aa39bb771","GELADEIRA",2000.0,1,datetime(2025,3,10,14,5,25),"MG",7870,2000.0),
        ("da579fb7-8446-4246-9f3c-e655a6389be4","LIVRO",30.0,5,datetime(2025,3,13,18,49,43),"GO",13452,150.0),
        ("e495a333-0360-4c5c-9f0e-2705212ebc2a","TV",2500.0,1,datetime(2025,3,19,19,32,26),"PE",2184,2500.0),
        ("3d984f23-9dfb-47f1-bbfd-f8fc1bc0c841","COMPUTADOR",700.0,1,datetime(2025,3,21,9,39,5),"RJ",1507,700.0),
        ("143fa00b-0b40-4b01-b5ff-879a23210e25","CELULAR",1000.0,2,datetime(2025,3,17,6,37,28),"RS",13858,2000.0),
        ("4c9f87cb-7dde-4dea-81c7-27b50452b8ac","TABLET",1100.0,1,datetime(2025,3,8,21,29,3),"PR",12373,1100.0),
        ("7bb7e1df-41aa-44d7-bf7e-7658d0c5c7d8","TV",2500.0,2,datetime(2025,3,19,17,14,7),"RJ",3615,5000.0),
        ("d034ddd6-8deb-43f2-bf6e-890166ae39fb","LIVRO",30.0,1,datetime(2025,3,8,12,26,38),"ES",11996,30.0),
        ("d77c6ec6-2a9b-43eb-ba21-3842fb44327f","TABLET",1100.0,2,datetime(2025,3,10,16,19,40),"GO",9610,2200.0),
        ("680f1a90-e015-45f4-a300-b399f2ae5eef","HOMETHEATER",500.0,1,datetime(2025,3,29,7,30,43),"MA",6621,500.0)
    ]


    df_pedidos_entrada = spark_session.createDataFrame(dados_pedidos_entrada, schema_pedidos_entrada)

    '''
    schema_entrada = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
    ])
    dados_entrada = [
        ("Produto A", 10.0, 2),
        ("Produto B", 5.5, 3),
        ("Produto C", 100.0, 1)
    ]
    df_entrada = spark_session.createDataFrame(dados_entrada, schema_entrada)
    '''

    schema_resumo_pagamentos_esperado = StructType([
        StructField("id_pedido", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_total", DoubleType(), True),
        StructField("data_criacao", TimestampType(), True),
    ]
    
    )

    dados_resumo_pagamentos_esperado = [
        ("3d984f23-9dfb-47f1-bbfd-f8fc1bc0c841","RJ","PIX",700.0,datetime(2025,3,21,9,39,5)),
        ("143fa00b-0b40-4b01-b5ff-879a23210e25","RS","CARTAO_CREDITO",2000.0,datetime(2025,3,17,6,37,28)),
        ("7bb7e1df-41aa-44d7-bf7e-7658d0c5c7d8","RJ","CARTAO_CREDITO",5000.0,datetime(2025,3,19,17,14,7)),
        ("d77c6ec6-2a9b-43eb-ba21-3842fb44327f","GO","CARTAO_CREDITO",2200.0,datetime(2025,3,10,16,19,40)),
        ("680f1a90-e015-45f4-a300-b399f2ae5eef","MA","PIX",500.0,datetime(2025,3,29,7,30,43)),
]


    df_resumo_pagamentos_esperado = spark_session.createDataFrame(dados_resumo_pagamentos_esperado, schema_resumo_pagamentos_esperado)

    '''
    schema_esperado = StructType([
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("valor_total", FloatType(), True)
    ])
    dados_esperados = [
        ("Produto A", 10.0, 2, 20.0),
        ("Produto B", 5.5, 3, 16.5),
        ("Produto C", 100.0, 1, 100.0)
    ]
    df_esperado = spark_session.createDataFrame(dados_esperados, schema_esperado)
    '''
    
    # 2. Act (Executar a função a ser testada)
    #df_resultado = transformer.add_valor_total_pedidos(df_entrada)
    df_resultado = transformer.join_pedidos_pagamentos(df_pedidos_entrada, df_pagamentos_entrada)

    # 3. Assert (Verificar se o resultado é o esperado)
    # Coletamos os dados dos DataFrames para comparar como listas de dicionários
    resultado_coletado = sorted([row.asDict() for row in df_resultado.collect()], key=lambda x: x['id_pedido'])
    esperado_coletado = sorted([row.asDict() for row in df_resumo_pagamentos_esperado.collect()], key=lambda x: x['id_pedido'])

    assert df_resultado.count() == df_resumo_pagamentos_esperado.count(), "O número de linhas não corresponde ao esperado."
    assert df_resultado.columns == df_resumo_pagamentos_esperado.columns, "As colunas não correspondem ao esperado."
    assert resultado_coletado == esperado_coletado, "O conteúdo dos DataFrames não é igual."
