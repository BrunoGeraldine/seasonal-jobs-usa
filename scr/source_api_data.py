import requests
import pandas as pd
import time

# ========== CONFIGURAÇÕES ==========
FILTER_ACTIVE = True  # Filtrar apenas registros com active = true
MAX_PAGES = None  # None = sem limite (coletar todas as páginas)
# ===================================

url = "https://api.seasonaljobs.dol.gov/datahub/"
params = {
    "api-version": "2020-06-30"
}

# Adiciona filtro OData para active = true
if FILTER_ACTIVE:
    params["$filter"] = "active eq true"
    print("✓ Filtro ativado: coletando apenas registros com active = TRUE")
else:
    print("⚠️ Sem filtro: coletando todos os registros")

if MAX_PAGES:
    print(f"⚠️ Modo de teste: limitado a {MAX_PAGES} páginas\n")
else:
    print(f"✓ Sem limite de páginas: coletando TODOS os dados disponíveis\n")

all_jobs = []
page = 0
total_fetched = 0

print("Iniciando coleta de dados...")

while True:
    # Verifica limite de páginas (se definido)
    if MAX_PAGES and page >= MAX_PAGES:
        print(f"\n⚠️ Limite de {MAX_PAGES} páginas atingido (modo teste)")
        break
    
    # Adiciona paginação usando $skip e $top
    current_params = params.copy()
    current_params["$skip"] = page * 50
    current_params["$top"] = 50
    
    try:
        response = requests.get(url, params=current_params)
        response.raise_for_status()
        data = response.json()
        
        # Extrai os registros da página atual
        jobs = data.get("value", [])
        
        # Se não houver mais dados, encerra o loop
        if not jobs:
            print(f"\n✓ Coleta finalizada. Nenhum dado adicional encontrado.")
            break
        
        all_jobs.extend(jobs)
        total_fetched += len(jobs)
        page += 1
        
        print(f"Página {page}: {len(jobs)} registros coletados (Total: {total_fetched})")
        
        # Verifica se existe um link de continuação (@odata.nextLink)
        next_link = data.get("@odata.nextLink")
        if not next_link and len(jobs) < 50:
            # Se não há nextLink e recebeu menos de 50, chegou ao fim
            break
        
        # Pequena pausa para não sobrecarregar a API
        time.sleep(0.5)
        
    except requests.exceptions.RequestException as e:
        print(f"\n❌ Erro na requisição: {e}")
        break

# Converte para DataFrame
if all_jobs:
    df = pd.DataFrame(all_jobs)
    
    print(f"\n{'='*60}")
    print(f"✓ Total de registros coletados: {len(df)}")
    print(f"✓ Número de colunas: {len(df.columns)}")
    
    # Verifica quantos registros têm active = True
    if 'active' in df.columns:
        active_count = df['active'].sum() if df['active'].dtype == bool else (df['active'] == True).sum()
        print(f"✓ Registros com active = TRUE: {active_count}")
    
    print(f"{'='*60}")
    
    ## Salva em CSV
    #df.to_csv("../dataset/seasonal_jobs_raw.csv", index=False, encoding='utf-8')
    #print(f"✓ Arquivo CSV salvo: '../dataset/seasonal_jobs_raw.csv'")
    # Salva em Parquet
    df.to_parquet("../dataset/seasonal_jobs_raw.parquet", index=False)
    print(f"✓ Arquivo Parquet salvo: '../dataset/seasonal_jobs_raw.parquet'")
    
    # Exibe primeiras linhas
    print(f"\nPrimeiras linhas do DataFrame:")
    print(df.head())
    
    # Mostra informação sobre a coluna 'active'
    if 'active' in df.columns:
        print(f"\nDistribuição da coluna 'active':")
        print(df['active'].value_counts())
else:
    print("❌ Nenhum dado foi coletado.")