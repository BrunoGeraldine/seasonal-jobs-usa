# seasonal-jobs-brunss

Ok, organizei minhas ideias e seguirei a seguinte arquitetura:

source_data = https://api.seasonaljobs.dol.gov/datahub/?api-version=2020-06-30
link-json: https://seasonaljobs.dol.gov/feeds
Usar o link para atualizar os dados pois a API nao traz as informacoes necessarias

Tratamento de dados:
1. Renomear colunas para melhor entendimento
obs.: colunas de informacoes dos dados para aplicacao (CLAUDE pode sugerir conforme voce entender)
2. criar coluna "url_job" =CONCATENATE("https://seasonaljobs.dol.gov/jobs/" + "caseNumber")
7. Salvar arquivo em parquet.

Colunas selecionadas:

"case_id", "case_number", "case_status", "visa_class", "job_title", "basic_rate_from", "basic_rate_to", "overtime_rate_to", "overtime_rate_from", "work_hour_num_basic", "add_wage_info", "total_positions", "full_time", "hourly_work_schedule_am	", "hourly_work_schedule_pm	", "is_overtime_available", "begin_date	", "end_date", "emp_exp_num_months", "special_req", "training_req", "num_months_training", "education_level", "pay_range_desc", "employer_business_name", "employer_city	", "employer_state	", "employer_zip", "worksite_locations", "worksite_address", "worksite_city	", "worksite_state	", "worksite_zip", "apply_email", "apply_phone", "apply_url", "job_order_exists", "active_date"

Criar novas colunas
"url_job" = CONCATENATE("https://seasonaljobs.dol.gov/jobs/" + "case_number")





refatore o codigo abaixo para atender os requisitos

Filtros No menu lateral (sidebar)
'case_number': com opcao de preenchimento a mao do valor
basic_rate_from: 
work_hour_num_basic:
emp_experience_reqd:
begin_date: capturar apenas o mes
emp_exp_num_months:
tot_months: encontrar a diferenca entre as datas 'end_date' e 'begin_date'	
worksite_city
worksite_state




o filtros devem resultar numa tabela as colunas abaixo:

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








