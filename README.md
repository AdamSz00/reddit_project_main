# Reddit ETL Pipeline

## 📌 Project Overview
This project implements a complete **ETL (Extract, Transform, Load)** pipeline for collecting Reddit data, transforming it, and storing it in a **PostgreSQL** database. The pipeline also includes **toxicity analysis using BERT** and username anonymization to protect user privacy.

The solution is automated using **cron jobs** and supports easy configuration through environment variables and YAML settings.

---

## ✅ Features
- **Extract**: Collect data from Reddit API using PRAW.
- **Transform**: Clean raw data, add metadata (date, time, community), and anonymize usernames.
- **Load**: Insert new posts and authors into PostgreSQL.
- **Toxicity Analysis**: Run BERT-based sentiment analysis on new posts.
- **Logging**: Detailed logs of every ETL run.
- **Automation**: Run daily via `cron`.

---

## 🛠 Tech Stack
- **Python 3.x**
- **PRAW** (Reddit API)
- **Pandas**
- **SQLAlchemy** for PostgreSQL connection
- **Transformers (HuggingFace)** for toxicity analysis
- **Logging** for monitoring ETL runs

---

## 🔧 Project Structure
```
reddit_project_main/   
├── config/  
│     └── settings.yaml            # Configuration file for scraping parameters  
├── logs/  
│     ├── cron_output.log          # Output logs from cron job runs  
│     └── etl.log                  # Detailed ETL process logs  
├── PY/  
│     ├── main.py                  # Main script to run the ETL pipeline  
│     └── run_etl.sh               # Shell script to execute the ETL pipeline  
│     └── UT/
│        ├── power_bi/  
│        │        └── Tree_hierarchy.ipynb # Power BI notebook for data visualization
│        ├── add_missing.py               # Utility for handling missing data  
│        ├── bert_analysis4.py            # Toxicity analysis using BERT model  
│        ├── logger_config.py             # Logging configuration setup  
│        ├── reddit_scrapper1.py          # Reddit data extraction module  
│        ├── sql_connect3.py              # PostgreSQL connection utilities  
│        └── transform2.py                # Data transformation functions   
├── SQL/  
│     ├── 1.create_database.sql    # SQL script to create the database  
│     ├── 2.create_tables.sql      # SQL script to create necessary tables   
│     └── 3.tests.sql              # SQL tests for validation  
├── requirements.txt             # Python dependencies  
└── README.md                    # Project documentation  
```

---

## ⚙️ Configuration
Edit the file `config/settings.yaml`:
```yaml
scrape_limit: 1000
target_community: "roosterteeth"
log_level: "INFO"
```

---

## 🛠 Installation

Choose one of the following installation options:

**Option A – Conda (recommended)**
```bash
conda env create -f environment.yml
conda activate reddit
```

**Option B – Pip**
```bash
# (Optional) Create and activate a virtual environment:
python -m venv venv
source venv/bin/activate  # On Windows use: venv\Scripts\activate

# Install required packages:
pip install -r requirements.txt
```


---

**Set up PostgreSQL database:**
- Run the SQL scripts in the `SQL/` folder in order to create the database and tables.
---

**Configure environment variables and `config/settings.yaml` as needed.**


## ▶️ Running the ETL

Run the ETL pipeline manually by executing:

```bash
python ETL/main.py
```

Or use the shell script:

```bash
bash ETL/run_etl.sh
```

Logs will be generated in the `logs/` folder for monitoring.

---

## ⏰ Automation with Cron

To automate the ETL pipeline to run daily at 01:00 AM, add the following cron job:

1. Open the crontab editor:

   ```bash
   crontab -e
   ```

2. Add the line below, adjusting paths as necessary:

   ```cron
   0 1 * * * /bin/bash /path/to/reddit_project_main/ETL/run_etl.sh >> /path/to/reddit_project_main/logs/cron_output.log 2>&1
   ```

3. Save and exit. The ETL will now run automatically every day at 1 AM.

---