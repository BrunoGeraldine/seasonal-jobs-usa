import pandas as pd
from datetime import datetime

print("=" * 70)
print("TRATAMENTO DE DADOS - SEASONAL JOBS")
print("=" * 70)

# 1. Carregar dados do arquivo parquet original
print("\n[1/4] Carregando dados...")
df = pd.read_parquet("../dataset/seasonal_jobs_raw.parquet")
print(f"✓ {len(df)} registros carregados")
print(f"✓ {len(df.columns)} colunas no arquivo original")

# 2. Mapeamento de renomeação de colunas
print("\n[2/4] Renomeando colunas...")
column_mapping = {
    'caseNumber': 'case_number',
    'caseStatus': 'case_status',
    'visaClass': 'visa_class',
    'jobTitle': 'job_title',
    'basicRateFrom': 'basic_rate_from',
    'basicRateTo': 'basic_rate_to',
    'overtimeRateFrom': 'overtime_rate_from',
    'overtimeRateTo': 'overtime_rate_to',
    'workHourNumBasic': 'work_hour_num_basic',
    'addWageInfo': 'add_wage_info',
    'totalPositions': 'total_positions',
    'fullTime': 'full_time',
    'hourlyWorkScheduleAm': 'hourly_work_schedule_am',
    'hourlyWorkSchedulePm': 'hourly_work_schedule_pm',
    'isOvertimeAvailable': 'is_overtime_available',
    'beginDate': 'begin_date',
    'endDate': 'end_date',
    'empExpNumMonths': 'emp_exp_num_months',
    'specialReq': 'special_req',
    'trainingReq': 'training_req',
    'numMonthsTraining': 'num_months_training',
    'educationLevel': 'education_level',
    'payRangeDesc': 'pay_range_desc',
    'employerBusinessName': 'employer_business_name',
    'employerCity': 'employer_city',
    'employerState': 'employer_state',
    'employerZip': 'employer_zip',
    'worksiteLocations': 'worksite_locations',
    'worksiteAddress': 'worksite_address',
    'worksiteCity': 'worksite_city',
    'worksiteState': 'worksite_state',
    'worksiteZip': 'worksite_zip',
    'applyEmail': 'apply_email',
    'applyPhone': 'apply_phone',
    'applyUrl': 'apply_url',
    'jobOrderExists': 'job_order_exists',
    'activeDate': 'active_date',
    'id': 'case_id'
}

# Renomear colunas que existem no DataFrame
df_renamed = df.rename(columns=column_mapping)
print(f"✓ {len(column_mapping)} colunas renomeadas")

# 3. Criar nova coluna url_job
print("\n[3/4] Criando coluna 'url_job'...")
df_renamed['url_job'] = "https://seasonaljobs.dol.gov/jobs/" + df_renamed['case_number'].astype(str)
print(f"✓ Coluna 'url_job' criada")

# 4. Selecionar apenas as colunas desejadas
print("\n[4/4] Selecionando colunas finais...")
colunas_finais = [
    'case_id', 'case_number', 'case_status', 'visa_class', 'job_title',
    'basic_rate_from', 'basic_rate_to', 'overtime_rate_from', 'overtime_rate_to',
    'work_hour_num_basic', 'add_wage_info', 'total_positions', 'full_time',
    'hourly_work_schedule_am', 'hourly_work_schedule_pm', 'is_overtime_available',
    'begin_date', 'end_date', 'emp_exp_num_months', 'special_req',
    'training_req', 'num_months_training', 'education_level', 'pay_range_desc',
    'employer_business_name', 'employer_city', 'employer_state', 'employer_zip',
    'worksite_locations', 'worksite_address', 'worksite_city', 'worksite_state',
    'worksite_zip', 'apply_email', 'apply_phone', 'apply_url',
    'job_order_exists', 'active_date', 'url_job'
]

# Verificar quais colunas existem
colunas_existentes = [col for col in colunas_finais if col in df_renamed.columns]
colunas_faltando = [col for col in colunas_finais if col not in df_renamed.columns]

if colunas_faltando:
    print(f"\n⚠️ Colunas não encontradas ({len(colunas_faltando)}):")
    for col in colunas_faltando:
        print(f"   - {col}")

df_final = df_renamed[colunas_existentes].copy()
print(f"✓ {len(colunas_existentes)} colunas selecionadas")

# 5. Salvar arquivo tratado
print("\n" + "=" * 70)
output_file = "../dataset/seasonal_jobs_treated.parquet"
df_final.to_parquet(output_file, index=False)
print(f"✓ Arquivo salvo: {output_file}")

# Resumo final
print("\n" + "=" * 70)
print("RESUMO DO TRATAMENTO")
print("=" * 70)
print(f"Registros processados: {len(df_final)}")
print(f"Colunas finais: {len(df_final.columns)}")
print(f"\nPrimeiras linhas:")
print(df_final.head())

print(f"\nExemplo de URL gerada:")
print(df_final['url_job'].iloc[0] if 'url_job' in df_final.columns else "N/A")

print("\n✓ Tratamento concluído com sucesso!")
print("=" * 70)