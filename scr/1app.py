import streamlit as st
import pandas as pd

# --------------------------------------------------
# Page config
# --------------------------------------------------
st.set_page_config(page_title="H2 Jobs", layout="wide")

# --------------------------------------------------
# Load data
# --------------------------------------------------
df = pd.read_parquet("../dataset/seasonal_jobs_treated.parquet")

# --------------------------------------------------
# Defensive casting
# --------------------------------------------------
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

# --------------------------------------------------
# Derived columns
# --------------------------------------------------
df["begin_month"] = df["begin_date"].dt.to_period("M").astype(str)

df["tot_months"] = (
    (df["end_date"] - df["begin_date"])
    .dt.days.div(30)
)

# --------------------------------------------------
# Sidebar filters
# --------------------------------------------------
st.sidebar.header("Filters")

# Salary
salary_min = float(df["basic_rate_from"].min())
salary_max = float(df["basic_rate_from"].max())

salary_range = st.sidebar.slider(
    "Basic Rate From",
    salary_min,
    salary_max,
    (salary_min, salary_max)
)

# Work hours
work_hour_min = int(df["work_hour_num_basic"].min())
work_hour_max = int(df["work_hour_num_basic"].max())

work_hour_range = st.sidebar.slider(
    "Work Hours (Basic)",
    work_hour_min,
    work_hour_max,
    (work_hour_min, work_hour_max)
)

# Experience required
experience_filter = st.sidebar.selectbox(
    "Experience Required",
    ["All", "Yes", "No"],
    index=0
)

# Begin month
begin_months = sorted(df["begin_month"].dropna().unique())
selected_months = st.sidebar.multiselect(
    "Begin Month",
    begin_months,
    default=[]
)

# Experience months
exp_min = int(df["emp_exp_num_months"].min())
exp_max = int(df["emp_exp_num_months"].max())

exp_months_range = st.sidebar.slider(
    "Experience (Months)",
    exp_min,
    exp_max,
    (exp_min, exp_max)
)

# Total contract months
tot_min = int(df["tot_months"].min())
tot_max = int(df["tot_months"].max())

tot_months_range = st.sidebar.slider(
    "Total Contract Months",
    tot_min,
    tot_max,
    (tot_min, tot_max)
)

# Location filters
cities = sorted(df["worksite_city"].dropna().unique())
states = sorted(df["worksite_state"].dropna().unique())

selected_cities = st.sidebar.multiselect(
    "Worksite City",
    cities,
    default=[]
)

selected_states = st.sidebar.multiselect(
    "Worksite State",
    states,
    default=[]
)

# --------------------------------------------------
# Apply filters (conditional)
# --------------------------------------------------
filtered_df = df.copy()

# Salary
if salary_range != (salary_min, salary_max):
    filtered_df = filtered_df[
        filtered_df["basic_rate_from"].between(*salary_range)
    ]

# Work hours
if work_hour_range != (work_hour_min, work_hour_max):
    filtered_df = filtered_df[
        filtered_df["work_hour_num_basic"].between(*work_hour_range)
    ]

# Experience months
if exp_months_range != (exp_min, exp_max):
    filtered_df = filtered_df[
        filtered_df["emp_exp_num_months"].between(*exp_months_range)
    ]

# Total months
if tot_months_range != (tot_min, tot_max):
    filtered_df = filtered_df[
        filtered_df["tot_months"].between(*tot_months_range)
    ]

# Begin month
if selected_months:
    filtered_df = filtered_df[
        filtered_df["begin_month"].isin(selected_months)
    ]

# Location
if selected_cities:
    filtered_df = filtered_df[
        filtered_df["worksite_city"].isin(selected_cities)
    ]

if selected_states:
    filtered_df = filtered_df[
        filtered_df["worksite_state"].isin(selected_states)
    ]

# Experience required
if experience_filter != "All":
    filtered_df = filtered_df[
        filtered_df["emp_experience_reqd"] == (experience_filter == "Yes")
    ]

# --------------------------------------------------
# Final columns
# --------------------------------------------------
FINAL_COLUMNS = [
    'case_id', 'case_number', 'case_status', 'visa_class', 'job_title',
    'basic_rate_from', 'basic_rate_to', 'overtime_rate_from', 'overtime_rate_to',
    'work_hour_num_basic', 'add_wage_info', 'total_positions', 'full_time',
    'hourly_work_schedule_am', 'hourly_work_schedule_pm', 'is_overtime_available',
    'begin_date', 'end_date', 'emp_exp_num_months', 'special_req',
    'training_req', 'num_months_training', 'education_level', 'pay_range_desc',
    'employer_business_name', 'employer_city', 'employer_state', 'employer_zip',
    'worksite_locations', 'worksite_address', 'worksite_city', 'worksite_state',
    'worksite_zip', 'apply_email', 'apply_phone', 'apply_url',
    'job_order_exists', 'active_date', 'url_job'
]

final_df = filtered_df[
    [col for col in FINAL_COLUMNS if col in filtered_df.columns]
]

# --------------------------------------------------
# Display
# --------------------------------------------------
st.dataframe(final_df, use_container_width=True)
st.markdown(f"**Number of results:** {final_df.shape[0]}")
