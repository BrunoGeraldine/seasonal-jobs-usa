import streamlit as st
import pandas as pd

# --------------------------------------------------
# Page config
# --------------------------------------------------
st.set_page_config(page_title="H2 Jobs", layout="wide")

# --------------------------------------------------
# Load data
# --------------------------------------------------
df = pd.read_csv("../dataset/seasonal_jobs_raw.csv")

# Debug (use apenas se precisar)
# st.write(df.columns.tolist())

# --------------------------------------------------
# Defensive type casting (important)
# --------------------------------------------------
salary_cols = ["basic_rate_from", "basic_rate_to"]
for col in salary_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# --------------------------------------------------
# Salary filter
# --------------------------------------------------
salary_min = float(df["basic_rate_from"].min())
salary_max = float(df["basic_rate_to"].max())

min_salary, max_salary = st.slider(
    "Salary range",
    min_value=salary_min,
    max_value=salary_max,
    value=(salary_min, salary_max)
)

# --------------------------------------------------
# Full Time filter
# --------------------------------------------------
full_time_filter = st.selectbox(
    "Full Time",
    options=["All", "Yes", "No"]
)

# --------------------------------------------------
# Experience Required filter
# --------------------------------------------------
experience_filter = st.selectbox(
    "Experience Required",
    options=["All", "Yes", "No"]
)

# --------------------------------------------------
# Apply filters
# --------------------------------------------------
filtered_df = df.copy()

filtered_df = filtered_df[
    (filtered_df["basic_rate_from"] >= min_salary) &
    (filtered_df["basic_rate_to"] <= max_salary)
]

if full_time_filter != "All":
    filtered_df = filtered_df[
        filtered_df["full_time"] == (full_time_filter == "Yes")
    ]

if experience_filter != "All":
    filtered_df = filtered_df[
        filtered_df["emp_experience_reqd"] == (experience_filter == "Yes")
    ]

# --------------------------------------------------
# Columns mapping (API -> UI)
# --------------------------------------------------
COLUMNS_MAP = {
    "apply_phone": "Telephone Number to Apply",
    "apply_email": "Email Address to Apply",
    "worksite_city": "Location",
    "basic_rate_from": "Salary (From)",
    "basic_rate_to": "Salary (To)",
    "full_time": "Full Time",
    "emp_experience_reqd": "Experience Required",
    "total_positions": "Number of Workers Requested",
    "work_hour_num_basic": "Number of Hours Per Week",
    "hourly_work_schedule_am": "Work Schedule (Start time)",
    "hourly_work_schedule_pm": "Work Schedule (End time)",
    "worksite_address": "Address",
    "worksite_locations": "Multiple Worksites",
}

FINAL_COLUMNS = list(COLUMNS_MAP.values())

# --------------------------------------------------
# Build display DataFrame (safe)
# --------------------------------------------------
available_columns = [
    col for col in COLUMNS_MAP.keys()
    if col in filtered_df.columns
]

df_display = (
    filtered_df
    .loc[:, available_columns]
    .rename(columns={k: COLUMNS_MAP[k] for k in available_columns})
)

final_columns_available = [
    col for col in FINAL_COLUMNS
    if col in df_display.columns
]

# --------------------------------------------------
# Display
# --------------------------------------------------
st.dataframe(
    df_display[final_columns_available],
    use_container_width=True
)

# Show number of results
st.markdown(f"**Number of results:** {df_display.shape[0]}")