import streamlit as st
import pandas as pd
from pathlib import Path

st.set_page_config(page_title="H2 Jobs", layout="wide")

DATA_PATH = Path("../dataset/seasonal_jobs_treated.parquet")

@st.cache_data(ttl=3600)
def load_data():
    return pd.read_parquet(DATA_PATH)

df = load_data()

# ---------------- Defensive casting ----------------
numeric_cols = [
    "basic_rate_from", "basic_rate_to",
    "work_hour_num_basic", "emp_exp_num_months"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

date_cols = ["begin_date", "end_date"]
for col in date_cols:
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")

# ---------------- Derived columns ----------------
df["begin_month"] = df["begin_date"].dt.to_period("M").astype(str)
df["tot_months"] = (df["end_date"] - df["begin_date"]).dt.days.div(30)

# ---------------- Sidebar filters ----------------
st.sidebar.header("Filters")

salary_min = float(df["basic_rate_from"].min())
salary_max = float(df["basic_rate_from"].max())
salary_range = st.sidebar.slider(
    "Basic Rate From", salary_min, salary_max, (salary_min, salary_max)
)

work_hour_min = int(df["work_hour_num_basic"].min())
work_hour_max = int(df["work_hour_num_basic"].max())
work_hour_range = st.sidebar.slider(
    "Work Hours (Basic)", work_hour_min, work_hour_max, (work_hour_min, work_hour_max)
)

experience_filter = st.sidebar.selectbox(
    "Experience Required", ["All", "Yes", "No"]
)

begin_months = sorted(df["begin_month"].dropna().unique())
selected_months = st.sidebar.multiselect("Begin Month", begin_months)

exp_min = int(df["emp_exp_num_months"].min())
exp_max = int(df["emp_exp_num_months"].max())
exp_months_range = st.sidebar.slider(
    "Experience (Months)", exp_min, exp_max, (exp_min, exp_max)
)

tot_min = int(df["tot_months"].min())
tot_max = int(df["tot_months"].max())
tot_months_range = st.sidebar.slider(
    "Total Contract Months", tot_min, tot_max, (tot_min, tot_max)
)

cities = sorted(df["worksite_city"].dropna().unique())
states = sorted(df["worksite_state"].dropna().unique())

selected_cities = st.sidebar.multiselect("Worksite City", cities)
selected_states = st.sidebar.multiselect("Worksite State", states)

# ---------------- Apply filters ----------------
filtered_df = df.copy()

filtered_df = filtered_df[
    filtered_df["basic_rate_from"].between(*salary_range)
]

filtered_df = filtered_df[
    filtered_df["work_hour_num_basic"].between(*work_hour_range)
]

filtered_df = filtered_df[
    filtered_df["emp_exp_num_months"].between(*exp_months_range)
]

filtered_df = filtered_df[
    filtered_df["tot_months"].between(*tot_months_range)
]

if selected_months:
    filtered_df = filtered_df[filtered_df["begin_month"].isin(selected_months)]

if selected_cities:
    filtered_df = filtered_df[filtered_df["worksite_city"].isin(selected_cities)]

if selected_states:
    filtered_df = filtered_df[filtered_df["worksite_state"].isin(selected_states)]

if experience_filter != "All":
    filtered_df = filtered_df[
        filtered_df["emp_experience_reqd"] == (experience_filter == "Yes")
    ]

# ---------------- Display ----------------
st.dataframe(filtered_df, use_container_width=True)
st.markdown(f"**Number of results:** {filtered_df.shape[0]}")

