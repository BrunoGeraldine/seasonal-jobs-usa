import os
import requests
import pandas as pd
from datetime import datetime

DATASET_PATH = "dataset/seasonal_jobs_raw.parquet"
CHECKPOINT_PATH = "dataset/seasonal_jobs_last_run.txt"

API_URL = "https://api.seasonaljobs.dol.gov/datahub/"
PARAMS = {
    "api-version": "2020-06-30"
}

# --------------------------------------------------
# Load last checkpoint (if exists)
# --------------------------------------------------
if os.path.exists(CHECKPOINT_PATH):
    with open(CHECKPOINT_PATH, "r") as f:
        last_run = f.read().strip()
    print(f"▶️ Last checkpoint found: {last_run}")
else:
    last_run = None
    print("⚠️ No checkpoint found — running FULL LOAD")

# --------------------------------------------------
# Fetch data
# --------------------------------------------------
response = requests.get(API_URL, params=PARAMS)
response.raise_for_status()

data = response.json()["value"]
df_new = pd.DataFrame(data)

# --------------------------------------------------
# Incremental filter (if applicable)
# --------------------------------------------------
if last_run and "dhTimestamp" in df_new.columns:
    df_new["dhTimestamp"] = pd.to_datetime(df_new["dhTimestamp"])
    df_new = df_new[df_new["dhTimestamp"] > pd.to_datetime(last_run)]
    print(f"🧩 Incremental rows: {len(df_new)}")
else:
    print(f"📦 Full load rows: {len(df_new)}")

# --------------------------------------------------
# Merge with existing dataset
# --------------------------------------------------
if os.path.exists(DATASET_PATH):
    df_old = pd.read_parquet(DATASET_PATH)
    df_final = pd.concat([df_old, df_new]).drop_duplicates(subset=["case_id"])
else:
    df_final = df_new

# --------------------------------------------------
# Save dataset
# --------------------------------------------------
df_final.to_parquet(DATASET_PATH, index=False)

# --------------------------------------------------
# Update checkpoint
# --------------------------------------------------
now = datetime.utcnow().isoformat()

with open(CHECKPOINT_PATH, "w") as f:
    f.write(now)

print(f"✅ Dataset saved with {len(df_final)} rows")
print(f"🕒 Checkpoint updated: {now}")
