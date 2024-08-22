import pyarrow.parquet as pq
import pandas as pd

def comparar_parquet(file1, file2):
    # Carregar os dados dos arquivos Parquet
    table1 = pq.read_table(file1)
    table2 = pq.read_table(file2)
    
    # Converter para DataFrames do Pandas
    df1 = table1.to_pandas()
    df2 = table2.to_pandas()
    
    # Comparar se os DataFrames são iguais
    if df1.equals(df2):
        print("Os arquivos Parquet são idênticos.")
    else:
        print("Os arquivos Parquet são diferentes.")
        # Opcional: Mostrar as diferenças
        df_diff = pd.concat([df1, df2]).drop_duplicates(keep=False)
        if not df_diff.empty:
            print("Diferenças encontradas:")
            print(df_diff)

# Exemplo de uso
comparar_parquet('resultado_cpu.parquet', 'resultado_gpu.parquet')
