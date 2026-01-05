import requests
import pandas as pd

url = "https://api.seasonaljobs.dol.gov/datahub/"
params = {
    "api-version": "2020-06-30"
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()

# 🔴 AQUI ESTÁ O PONTO-CHAVE
jobs = data["value"]

df = pd.DataFrame(jobs)

df.to_csv("../dataset/seasonal_jobs_raw.csv", index=False)


print("CSV generated successfully")
df.head()