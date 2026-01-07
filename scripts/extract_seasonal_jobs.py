import os
import time
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pathlib import Path

# ================== CONFIG ==================
PAGE_SIZE = 50
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
FILTER_ACTIVE = True
# ============================================

BASE_URL = "https://api.seasonaljobs.dol.gov/datahub/"

# Garantir que os caminhos funcionem tanto localmente quanto no CI/CD
BASE_DIR = Path(__file__).parent.parent
DATASET_DIR = BASE_DIR / "dataset"

# Criar diretório se não existir
DATASET_DIR.mkdir(parents=True, exist_ok=True)

DATASET_PATH = DATASET_DIR / "seasonal_jobs_raw.parquet"
CHECKPOINT_DATE_PATH = DATASET_DIR / "seasonal_jobs_last_run.txt"
CHECKPOINT_PAGE_PATH = DATASET_DIR / "seasonal_jobs_last_page.txt"

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
if CHECKPOINT_DATE_PATH.exists():
    with open(CHECKPOINT_DATE_PATH, "r") as f:
        last_run_dt = pd.to_datetime(f.read().strip(), errors="coerce", utc=True)
    print(f"▶️ Checkpoint data: {last_run_dt}")
else:
    last_run_dt = None
    print("⚠️ Sem checkpoint de data — FULL LOAD")

if CHECKPOINT_PAGE_PATH.exists():
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

if DATASET_PATH.exists():
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

if CHECKPOINT_PAGE_PATH.exists():
    os.remove(CHECKPOINT_PAGE_PATH)

print(f"✓ Checkpoint data atualizado: {new_checkpoint}")
print("✅ Extração completa com paginação garantida")

#end of file