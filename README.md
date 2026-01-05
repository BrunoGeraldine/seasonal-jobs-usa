# seasonal-jobs-brunss

Ok, organizei minhas ideias e seguirei a seguinte arquitetura:

source_data = https://api.seasonaljobs.dol.gov/datahub/?api-version=2020-06-30
link-json: https://seasonaljobs.dol.gov/feeds
Usar o link para atualizar os dados pois a API nao traz as informacoes necessarias

Tratamento de dados:
1. Baixar os dois arquivos H2A e H2B
2. transformar em dataframe incluindo uma coluna para cada  tipo de visto
3. Concatenar os dois arquivos
4. Filtrar colunas
5. Renomear colunas para melhor entendimento
obs.: colunas de informacoes dos dados para aplicacao:  "recApplyPhone" = "Phone", "recApplyEmail" = "Email", "recApplyUrl" ="ApplyHere"
6. criar coluna URL vaga =CONCATENATE("https://seasonaljobs.dol.gov/jobs/" + "caseNumber")
7. Salvar arquivo em csv.


Para teste da aplicacao em python, iremos ler os dados da api e salvar num dataframe em csv para criacao dos filtros dentro do streamlit


Salary:
Full Time:
Experience Required:

o filtros devem trazer ocm o resultado os dados abaixo:

Telephone Number to Apply:
Email address to Apply:
Location:
Salary:
Full Time:
Experience Required:
Number of Workers Requested:
Number of Hours Per Week: 
Work Schedule (Start/End time):
Address:
Multiple Worksites:






1. Selecionar apenas algumas colunas importantes:



caseNumber
tempneedJobtitle
tempneedSocTitle
tempneedWkrPos
tempneedStart	
tempneedEnd
tempneedNature
tempneedDescription
empBusinessName
empAddr1
empCity	
empState
empPhone
empPhoneext
emppocLastname	
emppocFirstname
emppocMiddlename
emppocJobtitle
emppocAddr1	
emppocAddr2	
emppocCity	
emppocState
emppocPostcode
emppocPhone
emppocEmail	attyRepresentType	attyLastname	attyFirstname	attyMiddlename	attyAddr1	attyAddr2	attyCity	attyState	attyPostcode	attyCountry	attyProvince	attyPhone	attyPhoneext	attyEmail	attyBizname
jobDuties
jobHoursTotal	jobHoursSun	jobHoursMon	jobHoursTues	jobHoursWed	jobHoursThu	jobHoursFri	jobHoursSat	jobHourStart	jobStartperiod	jobHourEnd	jobEndperiod
jobMinexpmonths
jobMinspecialreq
jobAddr1	jobAddr2	jobCity	jobState	jobPostcode	jobCounty	jobMsa	wageFrom	wageTo
wageOtFrom	wageOtTo	wagePer	wageAdditional
jobMultiplesites
recPayDeductions
recApplyPhone	recApplyEmail	recApplyUrl
empflrecEngageH2b	empflrecEngageH2bAttached




