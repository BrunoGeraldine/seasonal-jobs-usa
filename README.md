# Seasonal Jobs Platform

A comprehensive ETL pipeline and analytics platform for processing and visualizing H2 seasonal job opportunities from the U.S. Department of Labor.

## 📋 Overview

This project implements an **incremental ETL pipeline** that:
- Extracts seasonal job data from the Department of Labor API
- Transforms and cleans the data
- Stores versioned datasets in Parquet format
- Serves interactive analytics via Streamlit

## 🏗️ Application Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    SEASONAL JOBS PLATFORM                       │
└─────────────────────────────────────────────────────────────────┘

                        DATA FLOW DIAGRAM

┌─────────────────────┐
│  External Data      │
│  Source (DOL API)   │
└──────────┬──────────┘
           │
           │ https://api.seasonaljobs.dol.gov/datahub/
           │ (api-version: 2020-06-30)
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  EXTRACTION LAYER (extract_seasonal_jobs.py)                    │
│  ✓ Paginated API requests with retry strategy                   │
│  ✓ Checkpoint-based incremental loading                         │
│  ✓ Deduplication by case_id                                     │
│  ✓ Output: seasonal_jobs_raw.parquet                            │
└──────────┬──────────────────────────────────────────────────────┘
           │
           │ Stores checkpoint timestamps
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  DATASET LAYER                                                  │
│  ├── seasonal_jobs_raw.parquet      (Raw data + history)        │
│  ├── seasonal_jobs_last_run.txt     (Checkpoint timestamp)      │
│  └── seasonal_jobs_last_page.txt    (Pagination checkpoint)     │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  TRANSFORMATION LAYER (transform_seasonal_jobs.py)              │
│  ✓ Column renaming (camelCase → snake_case)                     │
│  ✓ Data type casting (dates, numerics)                          │
│  ✓ URL generation for job links                                 │
│  ✓ Output: seasonal_jobs_treated.parquet                        │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│  ANALYTICS LAYER (app.py - Streamlit)                           │
│  ✓ Interactive filtering (salary, hours, experience)            │
│  ✓ Data-driven visualizations                                   │
│  ✓ Cached data loading (1 hour TTL)                             │
│  ✓ Responsive web interface                                     │
└──────────┬──────────────────────────────────────────────────────┘
           │
           ▼
    ┌─────────────────┐
    │  End User       │
    │  Dashboard      │
    └─────────────────┘
```

## 📁 Project Structure

```
seasonal-jobs-brunss/
│
├── README.md                              ← This file
├── LICENSE                                ← Project license
├── requirements.txt                       ← Python dependencies
│
├── scripts/
│   ├── extract_seasonal_jobs.py          ← Data extraction module
│   ├── transform_seasonal_jobs.py        ← Data transformation module
│   └── app.py                            ← Streamlit web application
│
└── dataset/
    ├── seasonal_jobs_raw.parquet         ← Raw data (cumulative history)
    ├── seasonal_jobs_treated.parquet     ← Cleaned & processed data
    ├── seasonal_jobs_last_run.txt        ← Last extraction timestamp
    └── seasonal_jobs_last_page.txt       ← Pagination checkpoint (temp)
```

## 🔄 ETL Pipeline Components

### 1. **Extraction Module** (`extract_seasonal_jobs.py`)

**Purpose:** Fetch seasonal job data from the Department of Labor API with intelligent pagination and incremental loading.

**Key Features:**
- Paginated API requests (50 records per page)
- HTTP retry strategy with exponential backoff (max 5 retries)
- Request timeout: 30 seconds
- Active jobs filtering (can be toggled via `FILTER_ACTIVE`)
- Checkpoint-based incremental extraction to avoid re-fetching

**Configuration:**
```python
PAGE_SIZE = 50
REQUEST_TIMEOUT = 30
MAX_RETRIES = 5
BACKOFF_FACTOR = 2
FILTER_ACTIVE = True
```

**Process Flow:**
1. Load last extraction timestamp from checkpoint
2. Paginate through API results
3. Filter new records (timestamp > last checkpoint)
4. Handle deduplication by `case_id`
5. Append to existing raw dataset
6. Update checkpoint for next run

**Output:**
- `seasonal_jobs_raw.parquet` - All raw records (cumulative)
- `seasonal_jobs_last_run.txt` - Latest extraction timestamp (ISO format)

### 2. **Transformation Module** (`transform_seasonal_jobs.py`)

**Purpose:** Clean, standardize, and enrich the raw data for analytics consumption.

**Key Features:**
- Column name normalization (camelCase → snake_case)
- Data type casting for dates and numeric fields
- Dynamic URL generation for job listings
- Maintains final column order for consistency

**Column Mapping (Sample):**
- `caseNumber` → `case_number`
- `jobTitle` → `job_title`
- `basicRateFrom` → `basic_rate_from`
- `beginDate` → `begin_date`
- `employerBusinessName` → `employer_business_name`
- etc. (37 total columns)

**Process Flow:**
1. Load raw Parquet file
2. Rename columns using mapping dictionary
3. Generate job URLs: `https://seasonaljobs.dol.gov/jobs/{case_number}`
4. Export standardized dataset

**Output:**
- `seasonal_jobs_treated.parquet` - Clean, ready-to-analyze data

### 3. **Analytics Application** (`app.py`)

**Purpose:** Interactive web dashboard for exploring seasonal job opportunities.

**Technology Stack:**
- **Streamlit** - Frontend framework
- **Pandas** - Data processing
- **Altair** - Visualizations (via Streamlit)

**Key Features:**
- **Cached Data Loading** - 1-hour TTL for performance
- **Defensive Casting** - Automatic type conversion for numeric and date fields
- **Dynamic Filtering:**
  - Salary range (Basic Rate From)
  - Weekly work hours (Basic)
  - Experience requirements
  - Custom filters expandable
- **Derived Columns:**
  - `begin_month` - Extracted from begin_date
  - `tot_months` - Calculated duration (end_date - begin_date)
- **Wide Layout** - Optimized for desktop viewing

## 📦 Dependencies

**Core Libraries:**
- `pandas==2.3.3` - Data manipulation
- `streamlit==1.52.2` - Web framework
- `fastparquet==2025.12.0` - Parquet file I/O
- `requests==2.32.5` - HTTP client with retry logic
- `pyarrow==22.0.0` - Arrow format support

**Visualization:**
- `altair==6.0.0` - Declarative visualization grammar

**See `requirements.txt` for complete list**

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- pip or conda

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/yourusername/seasonal-jobs-brunss.git
   cd seasonal-jobs-brunss
   ```

2. **Create virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

### Running the ETL Pipeline

**Step 1: Extract data**
```bash
cd scripts
python extract_seasonal_jobs.py
```

**Step 2: Transform data**
```bash
python transform_seasonal_jobs.py
```

**Step 3: Launch analytics app**
```bash
streamlit run app.py
```

The application will open at `http://localhost:8501`

## 🔑 Key Configuration Options

### Extraction (`extract_seasonal_jobs.py`)
```python
FILTER_ACTIVE = True    # Only extract active job postings
PAGE_SIZE = 50          # Records per API request
REQUEST_TIMEOUT = 30    # Seconds
MAX_RETRIES = 5         # HTTP retry attempts
BACKOFF_FACTOR = 2      # Exponential backoff multiplier
```

### API Endpoints
- **Main API:** `https://api.seasonaljobs.dol.gov/datahub/?api-version=2020-06-30`
- **Query Example:**
  ```
  GET /datahub/?api-version=2020-06-30&$top=50&$skip=0&$filter=active eq true
  ```

## 📊 Data Schema (Treated Dataset)

| Column | Type | Description |
|--------|------|-------------|
| `case_id` | int | Unique identifier |
| `case_number` | str | Case reference number |
| `case_status` | str | Active/Inactive status |
| `visa_class` | str | H2A/H2B/etc. |
| `job_title` | str | Position title |
| `basic_rate_from` | float | Hourly minimum wage |
| `basic_rate_to` | float | Hourly maximum wage |
| `work_hour_num_basic` | int | Weekly hours |
| `begin_date` | datetime | Start date |
| `end_date` | datetime | End date |
| `employer_business_name` | str | Employer name |
| `employer_city` | str | Employer location |
| `employer_state` | str | Employer state |
| `worksite_city` | str | Work location city |
| `worksite_state` | str | Work location state |
| `apply_email` | str | Application email |
| `apply_phone` | str | Application phone |
| `apply_url` | str | Application URL |
| `url_job` | str | Generated job listing URL |
| *...and 18+ more fields* | | |

## ⚙️ Technical Highlights

### Incremental Loading Strategy
- **Checkpoint System:** Uses timestamps to track last extraction
- **Benefit:** Only new/modified records are fetched, reducing API calls and processing time
- **Deduplication:** Records with same `case_id` are kept once

### Retry & Resilience
- **HTTP Retries:** Exponential backoff for transient failures (429, 500, 502, 503, 504)
- **Graceful Degradation:** Errors are logged; progress is saved to resume later

### Data Quality
- **Type Casting:** Defensive conversion of numeric/date fields with error coercion
- **Path Handling:** Works correctly in local and CI/CD environments via Path resolution

## 🔜 Roadmap & Future Enhancements

**Phase 2 - User Features:**
- [ ] OAuth integration (Gmail / Outlook)
- [ ] One-click job application
- [ ] Job recommendations based on user profile
- [ ] Saved searches & alerts

**Phase 3 - Backend Infrastructure:**
- [ ] PostgreSQL database for user data
- [ ] FastAPI backend for scalability
- [ ] RESTful API endpoints
- [ ] Job application tracking

**Phase 4 - Monetization:**
- [ ] Premium filters & analytics
- [ ] Employer dashboard
- [ ] Subscription tiers
- [ ] Automated job matching

## 📝 License

This project is licensed under the terms specified in the LICENSE file.

## 🤝 Contributing

Contributions are welcome! Please follow these steps:
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit changes (`git commit -m 'Add amazing feature'`)
4. Push to branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## ❓ FAQ

**Q: How often is data refreshed?**  
A: The extraction runs on a manual schedule. Implement GitHub Actions for automated hourly/daily runs.

**Q: Can I filter by specific states or employers?**  
A: Yes! The Streamlit app supports sidebar filters. Extend `app.py` to add more filter options.

**Q: What if the API is down?**  
A: The retry strategy will attempt 5 times with backoff. If it fails, previous data remains available.

**Q: How is incremental loading tracked?**  
A: A checkpoint file stores the last successful extraction timestamp. The next run fetches only newer records.

## 📞 Contact

For questions or support, please open an issue on GitHub.
