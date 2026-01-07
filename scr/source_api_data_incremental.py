import os
import time
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ================== CONFIG ==================
PAGE_SIZE = 50
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
FILTER_ACTIVE = True
# ============================================

BASE_URL = "https://api.seasonaljobs.dol.gov/datahub/"

DATASET_PATH = "../dataset/seasonal_jobs_raw.parquet"
CHECKPOINT_DATE_PATH = "../dataset/seasonal_jobs_last_run.txt"
CHECKPOINT_PAGE_PATH = "../dataset/seasonal_jobs_last_page.txt"

PARAMS_BASE = {
    "api-version": "2020-06-30",
    "$top": PAGE_SIZE
}

if FILTER_ACTIVE:
    PARAMS_BASE["$filter"] = "active eq true"

# --------------------------------------------------
# HTTP session with retry + backoff
# --------------------------------------------------
session = requests.Session()
retry_strategy = Retry(
    total=MAX_RETRIES,
    backoff_factor=BACKOFF_FACTOR,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)

# --------------------------------------------------
# Load checkpoints
# --------------------------------------------------
if os.path.exists(CHECKPOINT_DATE_PATH):
    with open(CHECKPOINT_DATE_PATH, "r") as f:
        last_run_dt = pd.to_datetime(f.read().strip(), errors="coerce", utc=True)
    print(f"▶️ Checkpoint data: {last_run_dt}")
else:
    last_run_dt = None
    print("⚠️ Sem checkpoint de data — FULL LOAD")

if os.path.exists(CHECKPOINT_PAGE_PATH):
    with open(CHECKPOINT_PAGE_PATH, "r") as f:
        start_page = int(f.read().strip())
    print(f"▶️ Retomando da página {start_page}")
else:
    start_page = 0
    print("▶️ Iniciando da página 0")

# --------------------------------------------------
# Paginated extract loop
# --------------------------------------------------
page = start_page
all_pages = []
total_new = 0

print("Iniciando coleta paginada...")

while True:
    params = PARAMS_BASE.copy()
    params["$skip"] = page * PAGE_SIZE

    try:
        response = session.get(
            BASE_URL,
            params=params,
            timeout=REQUEST_TIMEOUT
        )
        response.raise_for_status()
        data = response.json()

        records = data.get("value", [])
        if not records:
            print("✓ Fim da paginação")
            break

        df_page = pd.DataFrame(records)

        if "dhTimestamp" not in df_page.columns:
            raise RuntimeError("Coluna dhTimestamp não encontrada")
######
        #df_page["dhTimestamp"] = pd.to_datetime(
        #    df_page["dhTimestamp"], errors="coerce", utc=True
        #)
        df_page["dhTimestamp"] = pd.to_datetime(
            df_page["dhTimestamp"],
                    format="ISO8601",
                    errors="coerce",
                    utc=True
            )   
        if last_run_dt is not None:
            df_page = df_page[df_page["dhTimestamp"] > last_run_dt]

        if not df_page.empty:
            all_pages.append(df_page)
            total_new += len(df_page)

        page += 1

        with open(CHECKPOINT_PAGE_PATH, "w") as f:
            f.write(str(page))

        print(
            f"✓ Página {page} | "
            f"Registros novos: {len(df_page)} | "
            f"Total novos: {total_new}"
        )

        time.sleep(0.4)

    except requests.exceptions.RequestException as e:
        print(f"⚠️ Timeout / erro de rede: {e}")
        print("⚠️ Salvando progresso e encerrando")
        break

# --------------------------------------------------
# Consolidate dataset
# --------------------------------------------------
if not all_pages:
    print("⚠️ Nenhum dado novo coletado")
    exit(0)

df_new = pd.concat(all_pages, ignore_index=True)

if os.path.exists(DATASET_PATH):
    df_existing = pd.read_parquet(DATASET_PATH)
else:
    df_existing = pd.DataFrame()

if "case_id" in df_new.columns:
    df_final = pd.concat([df_existing, df_new], ignore_index=True)
    df_final = df_final.drop_duplicates(subset=["case_id"])
else:
    df_final = pd.concat([df_existing, df_new], ignore_index=True)
    df_final = df_final.drop_duplicates()

df_final.to_parquet(DATASET_PATH, index=False)
print(f"✓ Parquet atualizado: {DATASET_PATH}")

# --------------------------------------------------
# Update checkpoints
# --------------------------------------------------
new_checkpoint = df_new["dhTimestamp"].max()

with open(CHECKPOINT_DATE_PATH, "w") as f:
    f.write(new_checkpoint.isoformat())

if os.path.exists(CHECKPOINT_PAGE_PATH):
    os.remove(CHECKPOINT_PAGE_PATH)

print(f"✓ Checkpoint data atualizado: {new_checkpoint}")
print("✅ Extração completa com paginação garantida")


#import os
#import requests
#import pandas as pd
#import time
#
## ================== CONFIGURAÇÕES ==================
#FILTER_ACTIVE = True
#MAX_PAGES = None
## ===================================================
#
#DATASET_PATH = "../dataset/seasonal_jobs_raw.parquet"
#CHECKPOINT_PATH = "../dataset/seasonal_jobs_last_run.txt"
#
#url = "https://api.seasonaljobs.dol.gov/datahub/"
#params = {
#    "api-version": "2020-06-30"
#}
#
## --------------------------------------------------
## Load checkpoint
## --------------------------------------------------
#if os.path.exists(CHECKPOINT_PATH):
#    with open(CHECKPOINT_PATH, "r") as f:
#        last_run_raw = f.read().strip()
#    last_run_dt = pd.to_datetime(last_run_raw, errors="coerce", utc=True)
#    print(f"▶️ Checkpoint carregado: {last_run_dt}")
#else:
#    last_run_dt = None
#    print("⚠️ Checkpoint inexistente — FULL LOAD")
#
## --------------------------------------------------
## API filter
## --------------------------------------------------
#if FILTER_ACTIVE:
#    params["$filter"] = "active eq true"
#    print("✓ Filtro ativo: active = TRUE")
#
#print("Iniciando coleta de dados...")
#
#all_jobs = []
#page = 0
#total_fetched = 0
#
## --------------------------------------------------
## Extract loop
## --------------------------------------------------
#while True:
#    if MAX_PAGES and page >= MAX_PAGES:
#        break
#
#    current_params = params.copy()
#    current_params["$skip"] = page * 50
#    current_params["$top"] = 50
#
#    try:
#        response = requests.get(url, params=current_params, timeout=30)
#        response.raise_for_status()
#        data = response.json()
#
#        jobs = data.get("value", [])
#        if not jobs:
#            break
#
#        all_jobs.extend(jobs)
#        total_fetched += len(jobs)
#        page += 1
#
#        print(f"Página {page}: {len(jobs)} registros (Total: {total_fetched})")
#
#        time.sleep(0.4)
#
#    except requests.exceptions.RequestException as e:
#        print(f"❌ Erro na requisição: {e}")
#        break
#
## --------------------------------------------------
## Convert to DataFrame
## --------------------------------------------------
#if not all_jobs:
#    print("❌ Nenhum dado coletado.")
#    exit(0)
#
#df_new = pd.DataFrame(all_jobs)
#print(f"✓ Registros coletados: {len(df_new)}")
#
## --------------------------------------------------
## Incremental logic (dhTimestamp ONLY)
## --------------------------------------------------
#INCREMENTAL_COL = "dhTimestamp"
#
#if INCREMENTAL_COL not in df_new.columns:
#    raise RuntimeError("❌ Coluna dhTimestamp não encontrada — incremental impossível")
#
#df_new[INCREMENTAL_COL] = pd.to_datetime(
#    df_new[INCREMENTAL_COL],
#    errors="coerce",
#    utc=True
#)
#
#if last_run_dt is not None:
#    df_new = df_new[df_new[INCREMENTAL_COL] > last_run_dt]
#    print(f"✓ Incremental aplicado: dhTimestamp > {last_run_dt}")
#else:
#    print("✓ Primeiro run — FULL LOAD")
#
#print(f"✓ Registros após incremental: {len(df_new)}")
#
## --------------------------------------------------
## Load existing dataset
## --------------------------------------------------
#if os.path.exists(DATASET_PATH):
#    df_existing = pd.read_parquet(DATASET_PATH)
#    print(f"✓ Dataset existente: {len(df_existing)} registros")
#else:
#    df_existing = pd.DataFrame()
#    print("⚠️ Dataset inexistente — criando novo")
#
## --------------------------------------------------
## Merge and deduplicate
## --------------------------------------------------
#if "case_id" in df_new.columns:
#    df_final = pd.concat([df_existing, df_new], ignore_index=True)
#    df_final = df_final.drop_duplicates(subset=["case_id"])
#else:
#    df_final = pd.concat([df_existing, df_new], ignore_index=True)
#    df_final = df_final.drop_duplicates()
#
#print(f"✓ Dataset final: {len(df_final)} registros")
#
## --------------------------------------------------
## Save parquet
## --------------------------------------------------
#df_final.to_parquet(DATASET_PATH, index=False)
#print(f"✓ Parquet salvo: {DATASET_PATH}")
#
## --------------------------------------------------
## Update checkpoint
## --------------------------------------------------
#if not df_new.empty:
#    new_checkpoint = df_new[INCREMENTAL_COL].max()
#    with open(CHECKPOINT_PATH, "w") as f:
#        f.write(new_checkpoint.isoformat())
#    print(f"✓ Checkpoint atualizado: {new_checkpoint}")
#else:
#    print("⚠️ Nenhum registro novo — checkpoint mantido")
#
#print("✅ Incremental load finalizado com sucesso")
#
#
#import os
#import requests
#import pandas as pd
#import time
#from datetime import datetime
#
## ========== CONFIGURAÇÕES ==========
#FILTER_ACTIVE = True
#MAX_PAGES = None
## ===================================
#
#DATASET_PATH = "../dataset/seasonal_jobs_raw.parquet"
#CHECKPOINT_PATH = "../dataset/seasonal_jobs_last_run.txt"
#
#url = "https://api.seasonaljobs.dol.gov/datahub/"
#params = {
#    "api-version": "2020-06-30"
#}
#
## --------------------------------------------------
## Load last checkpoint
## --------------------------------------------------
#if os.path.exists(CHECKPOINT_PATH):
#    with open(CHECKPOINT_PATH, "r") as f:
#        last_run = f.read().strip()
#    last_run_dt = pd.to_datetime(last_run, errors="coerce")
#    print(f"▶️ Last checkpoint found: {last_run_dt}")
#else:
#    last_run_dt = None
#    print("⚠️ No checkpoint found — running FULL LOAD")
#
## --------------------------------------------------
## API filters
## --------------------------------------------------
#if FILTER_ACTIVE:
#    params["$filter"] = "active eq true"
#    print("✓ Filtro ativado: active = TRUE")
#
#print("Iniciando coleta de dados...")
#
#all_jobs = []
#page = 0
#total_fetched = 0
#
## --------------------------------------------------
## Extract loop
## --------------------------------------------------
#while True:
#    if MAX_PAGES and page >= MAX_PAGES:
#        break
#
#    current_params = params.copy()
#    current_params["$skip"] = page * 50
#    current_params["$top"] = 50
#
#    try:
#        response = requests.get(url, params=current_params)
#        response.raise_for_status()
#        data = response.json()
#
#        jobs = data.get("value", [])
#        if not jobs:
#            break
#
#        all_jobs.extend(jobs)
#        total_fetched += len(jobs)
#        page += 1
#
#        print(f"Página {page}: {len(jobs)} registros (Total: {total_fetched})")
#
#        time.sleep(0.4)
#
#    except requests.exceptions.RequestException as e:
#        print(f"❌ Erro na requisição: {e}")
#        break
#
## --------------------------------------------------
## Convert to DataFrame
## --------------------------------------------------
#if not all_jobs:
#    print("❌ Nenhum dado coletado.")
#    exit(0)
#
#df_new = pd.DataFrame(all_jobs)
#print(f"✓ Coletados {len(df_new)} registros")
#
## --------------------------------------------------
## Incremental filter
## --------------------------------------------------
##date_cols_priority = [
##    "active_date",
##    "accepted_date_time",
##    "dhTimestamp"
##]
##
##incremental_col = next((c for c in date_cols_priority if c in df_new.columns), None)
#incremental_col = "dhTimestamp"
#
#if incremental_col not in df_new.columns:
#    raise RuntimeError("❌ Coluna dhTimestamp não encontrada — incremental impossível")
#
#
#if incremental_col:
#    df_new[incremental_col] = pd.to_datetime(df_new[incremental_col], errors="coerce")
#
#    if last_run_dt is not None:
#        df_new = df_new[df_new[incremental_col] > last_run_dt]
#        print(f"✓ Incremental aplicado via {incremental_col}")
#else:
#    print("⚠️ Nenhuma coluna temporal encontrada — incremental desativado")
#
#print(f"✓ Registros após incremental: {len(df_new)}")
#
## --------------------------------------------------
## Load existing dataset
## --------------------------------------------------
#if os.path.exists(DATASET_PATH):
#    df_existing = pd.read_parquet(DATASET_PATH)
#    print(f"✓ Dataset existente carregado: {len(df_existing)} registros")
#else:
#    df_existing = pd.DataFrame()
#    print("⚠️ Dataset inexistente — criando novo")
#
## --------------------------------------------------
## Merge + deduplicate
## --------------------------------------------------
#if "case_id" in df_new.columns:
#    df_final = pd.concat([df_existing, df_new], ignore_index=True)
#    df_final = df_final.drop_duplicates(subset=["case_id"])
#else:
#    df_final = pd.concat([df_existing, df_new], ignore_index=True)
#    df_final = df_final.drop_duplicates()
#
#print(f"✓ Dataset final: {len(df_final)} registros")
#
## --------------------------------------------------
## Save dataset
## --------------------------------------------------
#df_final.to_parquet(DATASET_PATH, index=False)
#print(f"✓ Parquet salvo: {DATASET_PATH}")
#
## --------------------------------------------------
## Update checkpoint
## --------------------------------------------------
#if incremental_col and not df_new.empty:
#    new_checkpoint = df_new[incremental_col].max()
#    with open(CHECKPOINT_PATH, "w") as f:
#        f.write(str(new_checkpoint))
#    print(f"✓ Checkpoint atualizado: {new_checkpoint}")
#else:
#    print("⚠️ Checkpoint não atualizado")
#
#print("✅ Incremental load finalizado com sucesso")
