import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import timeit
import pyarrow as pa

def calcular_var_cpu(parquet_file):
    start_time = timeit.default_timer()
    # Carregar os dados do Parquet
    table = pq.read_table(parquet_file)
    df = table.to_pandas()

    # Converter os dados para arrays NumPy
    z = df['z'].to_numpy(dtype=np.float32)
    delta = df['delta'].to_numpy(dtype=np.float32)
    V = df['V'].to_numpy(dtype=np.float32)
    R = df['R'].to_numpy(dtype=np.float32)

    result = -z * delta * V * np.sqrt(R)

    # Cálculo do VaR e adição ao DataFrame
    df_result = pd.DataFrame({'z': z, 'delta': delta, 'V': V, 'R': R, 'VaR': result})

    # Salvar o resultado no mesmo arquivo Parquet
    table = pa.Table.from_pandas(df_result)
    pa.parquet.write_table(table, "resultado_cpu.parquet")

    end_time = timeit.default_timer()
    execution_time = end_time - start_time
    print("Tempo de execução CPU: ", execution_time, "segundos")

calcular_var_cpu('parametros.parquet')
