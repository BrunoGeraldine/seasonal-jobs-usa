import pandas as pd

# Caminhos dos arquivos
csv_path = "../dataset/seasonal_jobs_raw.csv"
parquet_path = "../dataset/seasonal_jobs_raw.parquet"

# Leitura do CSV
df = pd.read_csv(csv_path)

# Escrita em Parquet
df.to_parquet(
    parquet_path,
    engine="pyarrow",  # ou "fastparquet"
    index=False
)

print("Arquivo convertido com sucesso para Parquet!")
