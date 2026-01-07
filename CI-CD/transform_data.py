import pandas as pd
from pathlib import Path
import sys

INPUT_PATH = Path("dataset/seasonal_jobs_raw.parquet")
OUTPUT_PATH = Path("dataset/seasonal_jobs_treated.parquet")


COLUMN_MAPPING = {
    'caseNumber': 'case_number',
    'caseStatus': 'case_status',
    'visaClass': 'visa_class',
    'jobTitle': 'job_title',
    'basicRateFrom': 'basic_rate_from',
    'basicRateTo': 'basic_rate_to',
    'overtimeRateFrom': 'overtime_rate_from',
    'overtimeRateTo': 'overtime_rate_to',
    'workHourNumBasic': 'work_hour_num_basic',
    'addWageInfo': 'add_wage_info',
    'totalPositions': 'total_positions',
    'fullTime': 'full_time',
    'hourlyWorkScheduleAm': 'hourly_work_schedule_am',
    'hourlyWorkSchedulePm': 'hourly_work_schedule_pm',
    'isOvertimeAvailable': 'is_overtime_available',
    'beginDate': 'begin_date',
    'endDate': 'end_date',
    'empExpNumMonths': 'emp_exp_num_months',
    'specialReq': 'special_req',
    'trainingReq': 'training_req',
    'numMonthsTraining': 'num_months_training',
    'educationLevel': 'education_level',
    'payRangeDesc': 'pay_range_desc',
    'employerBusinessName': 'employer_business_name',
    'employerCity': 'employer_city',
    'employerState': 'employer_state',
    'employerZip': 'employer_zip',
    'worksiteLocations': 'worksite_locations',
    'worksiteAddress': 'worksite_address',
    'worksiteCity': 'worksite_city',
    'worksiteState': 'worksite_state',
    'worksiteZip': 'worksite_zip',
    'applyEmail': 'apply_email',
    'applyPhone': 'apply_phone',
    'applyUrl': 'apply_url',
    'jobOrderExists': 'job_order_exists',
    'activeDate': 'active_date',
    'id': 'case_id'
}


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


def main():
    if not INPUT_PATH.exists():
        print("❌ Arquivo de entrada não encontrado")
        sys.exit(1)

    df = pd.read_parquet(INPUT_PATH)

    df = df.rename(columns=COLUMN_MAPPING)

    if "case_number" in df.columns:
        df["url_job"] = "https://seasonaljobs.dol.gov/jobs/" + df["case_number"].astype(str)

    final_cols = [c for c in FINAL_COLUMNS if c in df.columns]
    df_final = df[final_cols]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_final.to_parquet(OUTPUT_PATH, index=False)

    print(f"✅ Transformação concluída: {len(df_final)} registros")
    print(f"📁 Arquivo: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
