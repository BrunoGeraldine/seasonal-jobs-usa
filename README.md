# seasonal-jobs-brunss

Ok, organizei minhas ideias e seguirei a seguinte arquitetura:

source_data = https://api.seasonaljobs.dol.gov/datahub/?api-version=2020-06-30
link-json: https://seasonaljobs.dol.gov/feeds
Usar o link para atualizar os dados pois a API nao traz as informacoes necessarias


Repo Git
seasonal-jobs-platform/
│
├── .github/
│   └── workflows/
│       └── data_pipeline.yml        ← CI/CD (ETL automático)
│
├── src/
│   ├── extract_incremental.py       ← Extração incremental da API
│   └── transform_data.py            ← Limpeza / modelagem
│
├── dataset/
│   ├── seasonal_jobs_raw.parquet    ← Dados brutos (histórico)
│   ├── seasonal_jobs_last_run.txt   ← Checkpoint incremental
│   └── seasonal_jobs_treated.parquet← Dados prontos p/ app
│
├── app.py                           ← Streamlit (frontend)
├── requirements.txt
└── README.md


🔁 ETL incremental automático

🧠 Dados versionados

⚙️ CI/CD real com GitHub Actions

🖥️ Streamlit consumindo dados prontos

🚀 Arquitetura de produto real


🔜 Próximo passo natural

OAuth Gmail / Outlook

“Apply with 1 click”

Banco Postgres

API FastAPI

Monetização