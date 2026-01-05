import requests
import pandas as pd

url = "https://api.seasonaljobs.dol.gov/datahub/"
params = {
    "api-version": "2020-06-30"
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()


df = pd.DataFrame(jobs)

df.to_csv("seasonal_jobs_raw.csv", index=False)



columns_map = {
    "telephoneNumber": "Telephone Number to Apply",
    "emailAddress": "Email address to Apply",
    "worksiteCity": "Location",
    "wageRate": "Salary",
    "fullTime": "Full Time",
    "experienceRequired": "Experience Required",
    "totalWorkersRequested": "Number of Workers Requested",
    "hoursPerWeek": "Number of Hours Per Week",
    "workSchedule": "Work Schedule (Start/End time)",
    "worksiteAddress": "Address",
    "multipleWorksites": "Multiple Worksites"
}

df_display = df[list(columns_map.keys())].rename(columns=columns_map)



