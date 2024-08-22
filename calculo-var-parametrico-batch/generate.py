import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

def gerar_parquet(num_linhas, nome_arquivo, intervalo_delta=(0.01, 0.2)):
  """
  Gera um DataFrame com colunas aleatórias e salva em formato Parquet.

  Args:
    num_linhas: Número de linhas do DataFrame.
    nome_arquivo: Nome do arquivo Parquet a ser salvo.
    intervalo_delta: Tupla com o intervalo mínimo e máximo para o desvio padrão delta.
  """

  # Gerando dados aleatórios
  data = {
      'R': np.random.rand(num_linhas),
      'z': np.random.normal(loc=1.645, scale=0.2, size=num_linhas),
      'delta': np.random.uniform(intervalo_delta[0], intervalo_delta[1], size=num_linhas),
      'V': np.random.uniform(1000, 10000, size=num_linhas)
  }

  # Criando o DataFrame
  df = pd.DataFrame(data)

  # Convertendo para PyArrow Table e salvando em Parquet
  table = pa.Table.from_pandas(df)
  pa.parquet.write_table(table, nome_arquivo)

qtd = 10000000

gerar_parquet(qtd, './parametros.parquet', intervalo_delta=(0.05, 0.15))
print(qtd)