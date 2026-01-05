import json
import pandas as pd

# Ler o arquivo JSON
with open('../dataset/2026-01-05_h2b.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Converter para DataFrame do pandas
# Se o JSON for uma lista de dicionários
if isinstance(data, list):
    df = pd.DataFrame(data)
# Se o JSON for um dicionário com uma chave contendo a lista
elif isinstance(data, dict):
    # Tenta encontrar a primeira lista no dicionário
    for key, value in data.items():
        if isinstance(value, list):
            df = pd.DataFrame(value)
            break
    else:
        # Se não houver lista, converte o dicionário diretamente
        df = pd.DataFrame([data])
else:
    df = pd.DataFrame([data])

# Exibir o DataFrame
print(df)

# Opcional: exibir informações adicionais
print(f"\nNúmero de linhas: {len(df)}")
print(f"Número de colunas: {len(df.columns)}")
print(f"\nColunas disponíveis: {list(df.columns)}")

# Selecionar apenas colunas importantes
colunas_selecionadas = [
    'caseNumber', 'tempneedJobtitle', 'tempneedSocTitle', 'tempneedWkrPos',
    'tempneedStart', 'tempneedEnd', 'tempneedNature', 'tempneedDescription',
    'empBusinessName', 'empAddr1', 'empCity', 'empState', 'empPhone', 'empPhoneext',
    'emppocLastname', 'emppocFirstname', 'emppocMiddlename', 'emppocJobtitle',
    'emppocAddr1', 'emppocAddr2', 'emppocCity', 'emppocState', 'emppocPostcode',
    'emppocPhone', 'emppocEmail', 'attyRepresentType', 'attyLastname', 'attyFirstname',
    'attyMiddlename', 'attyAddr1', 'attyAddr2', 'attyCity', 'attyState',
    'attyPostcode', 'attyCountry', 'attyProvince', 'attyPhone', 'attyPhoneext',
    'attyEmail', 'attyBizname', 'jobDuties', 'jobHoursTotal', 'jobHoursSun',
    'jobHoursMon', 'jobHoursTues', 'jobHoursWed', 'jobHoursThu', 'jobHoursFri',
    'jobHoursSat', 'jobHourStart', 'jobStartperiod', 'jobHourEnd', 'jobEndperiod',
    'jobMinexpmonths', 'jobMinspecialreq', 'jobAddr1', 'jobAddr2', 'jobCity',
    'jobState', 'jobPostcode', 'jobCounty', 'jobMsa', 'wageFrom', 'wageTo',
    'wageOtFrom', 'wageOtTo', 'wagePer', 'wageAdditional', 'jobMultiplesites',
    'recPayDeductions', 'recApplyPhone', 'recApplyEmail', 'recApplyUrl',
    'empflrecEngageH2b', 'empflrecEngageH2bAttached'
]

# Verificar quais colunas existem no DataFrame
colunas_existentes = [col for col in colunas_selecionadas if col in df.columns]
colunas_faltando = [col for col in colunas_selecionadas if col not in df.columns]

if colunas_faltando:
    print(f"\n⚠️ Colunas não encontradas: {colunas_faltando}")

# Criar DataFrame com colunas selecionadas
df_filtrado = df[colunas_existentes]

print(f"\n✓ DataFrame filtrado criado com {len(colunas_existentes)} colunas")
print(f"\nPrimeiras linhas do DataFrame filtrado:")
print(df_filtrado.head())

# Exportar para Excel
df_filtrado.to_excel('../dataset/2026-01-05_h2b_filtrado.xlsx', index=False)
print(f"\n✓ Arquivo Excel exportado: '../dataset/2026-01-05_h2b_filtrado.xlsx'")

## Opcional: exportar para CSV
#df_filtrado.to_csv('../dataset/2026-01-05_h2b_filtrado.csv', index=False, encoding='utf-8')
#print(f"✓ Arquivo CSV exportado: '../dataset/2026-01-05_h2b_filtrado.csv'")