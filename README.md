# seasonal-jobs-brunss

Ok, organizei minhas ideias e seguirei a seguinte arquitetura:

source_data = https://api.seasonaljobs.dol.gov/datahub/?api-version=2020-06-30
link-json: https://seasonaljobs.dol.gov/feeds
Usar o link para atualizar os dados pois a API nao traz as informacoes necessarias


Repo Git
│
├── CI-CD/
│   ├── extract_seasonal_jobs.py   ← captura da API (CI-ready)
│   └── transform_seasonal_jobs.py ← limpeza / tratamento
│
├── dataset/
│   ├── seasonal_jobs_raw.parquet
│   └── seasonal_jobs_treated.parquet
│
└── .github/workflows/
    └── data_pipeline.yml          ← CI/CD com GitHub Actions






