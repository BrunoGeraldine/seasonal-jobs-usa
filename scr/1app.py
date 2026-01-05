import streamlit as st
import pandas as pd

st.set_page_config(page_title="H2 Jobs", layout="wide")

df = pd.read_csv("seasonal_jobs_raw.csv")



min_salary, max_salary = st.slider(
    "Salary range",
    min_value=float(df["wageRate"].min()),
    max_value=float(df["wageRate"].max()),
    value=(float(df["wageRate"].min()), float(df["wageRate"].max()))
)


full_time_filter = st.selectbox(
    "Full Time",
    options=["All", "Yes", "No"]
)


experience_filter = st.selectbox(
    "Experience Required",
    options=["All", "Yes", "No"]
)


filtered_df = df.copy()

filtered_df = filtered_df[
    (filtered_df["wageRate"] >= min_salary) &
    (filtered_df["wageRate"] <= max_salary)
]

if full_time_filter != "All":
    filtered_df = filtered_df[
        filtered_df["fullTime"] == (full_time_filter == "Yes")
    ]

if experience_filter != "All":
    filtered_df = filtered_df[
        filtered_df["experienceRequired"] == (experience_filter == "Yes")
    ]

# Mapping from API fields to UI-friendly labels
COLUMNS_MAP = {
    "telephoneNumber": "Telephone Number to Apply",
    "emailAddress": "Email Address to Apply",
    "worksiteCity": "Location",
    "wageRate": "Salary",
    "fullTime": "Full Time",
    "experienceRequired": "Experience Required",
    "totalWorkersRequested": "Number of Workers Requested",
    "hoursPerWeek": "Number of Hours Per Week",
    "workSchedule": "Work Schedule (Start/End time)",
    "worksiteAddress": "Address",
    "multipleWorksites": "Multiple Worksites",
}

FINAL_COLUMNS = list(COLUMNS_MAP.values())


available_columns = [col for col in COLUMNS_MAP.keys() if col in filtered_df.columns]

df_display = (
    filtered_df
    .loc[:, available_columns]
    .rename(columns={k: COLUMNS_MAP[k] for k in available_columns})
)

#df_display = (
#    filtered_df
#    .loc[:, COLUMNS_MAP.keys()]
#    .rename(columns=COLUMNS_MAP)
#)

st.dataframe(
    df_display[FINAL_COLUMNS],
    use_container_width=True
)
