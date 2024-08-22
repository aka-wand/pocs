import cudf
import cupy as cp
import pyarrow.parquet as pq
import pandas as pd
import timeit

# Kernel CUDA em cuPy
from cupy import ElementwiseKernel

# Definindo o kernel para cálculo do VaR
var_kernel_code = """
    float var = -z * delta * V * sqrt(R);
"""
var_kernel = ElementwiseKernel(
    in_params='float32 z, float32 delta, float32 V, float32 R',
    out_params='float32 var',
    operation=var_kernel_code,
    name='var_kernel'
)

def calcular_var_cuda(parquet_file):
    start_time = timeit.default_timer()
    
    # Carregar dados diretamente com cuDF
    df = cudf.read_parquet(parquet_file)

    # Extrair colunas e converter para arrays cupy
    z = cp.asarray(df['z'].to_cupy())
    delta = cp.asarray(df['delta'].to_cupy())
    V = cp.asarray(df['V'].to_cupy())
    R = cp.asarray(df['R'].to_cupy())

    # Inicializar o array para armazenar os resultados
    result = cp.empty_like(z)

    # Aplicar o kernel
    var_kernel(z, delta, V, R, result)

    # Converter o resultado para cuDF DataFrame
    df_result = cudf.DataFrame({
        'z': df['z'],
        'delta': df['delta'],
        'V': df['V'],
        'R': df['R'],
        'VaR': cp.asnumpy(result)  # Convertendo para numpy para compatibilidade com cuDF
    })
    
    # Salvar o resultado em um novo arquivo Parquet
    df_result.to_parquet("resultado_gpu.parquet")

    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print("Tempo de execução CUDA:", execution_time, "segundos")

calcular_var_cuda('parametros.parquet')
