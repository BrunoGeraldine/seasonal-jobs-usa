import pandas as pd
import os

def parquet_to_excel(parquet_file, output_file=None):
    """
    Lê um arquivo Parquet e salva em formato Excel.
    
    Args:
        parquet_file (str): Caminho do arquivo Parquet
        output_file (str, optional): Caminho do arquivo Excel de saída.
                                     Se None, usa o mesmo nome com extensão .xlsx
    """
    try:
        # Lê o arquivo Parquet
        print(f"Lendo arquivo: {parquet_file}")
        df = pd.read_parquet(parquet_file)
        
        # Remove timezone dos datetimes (Excel não suporta)
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                if df[col].dt.tz is not None:
                    print(f"  Removendo timezone da coluna: {col}")
                    df[col] = df[col].dt.tz_localize(None)
        
        # Define o caminho de saída se não fornecido
        if output_file is None:
            output_file = parquet_file.replace('.parquet', '.xlsx')
        
        # Salva em Excel
        print(f"Salvando em: {output_file}")
        df.to_excel(output_file, index=False, sheet_name='dados')
        
        print(f"✓ Sucesso! Arquivo Excel criado com {len(df)} linhas e {len(df.columns)} colunas.")
        
    except Exception as e:
        print(f"✗ Erro ao processar arquivo: {e}")

if __name__ == "__main__":
    # Define o caminho do arquivo Parquet
    dataset_dir = os.path.join(os.path.dirname(__file__), '..', 'dataset')
    parquet_file = os.path.join(dataset_dir, 'seasonal_jobs_raw.parquet')
    
    # Executa a conversão
    parquet_to_excel(parquet_file)
