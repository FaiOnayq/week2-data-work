

# ETL Pipeline & EDA Project
A reproducible data pipeline with quality checks, transformations, and exploratory analysis.


### Setup
```
uv venv -p 3.11
uv pip install -r requirements.txt
```

### Run ETL
```
python scripts/run_etl.py
```
### Run analysis
```jupyter notebook notebooks/eda.ipynb```

### Structure
```
data/
  raw/              # Source CSV files
  processed/        # Output Parquet files + metadata
src/bootcamp_data/  # Pipeline modules
scripts/            # Entry point
notebooks/          # Analysis notebook
reports/            # Figures + summary
```

### Outputs

- `data/processed/analytics_table.parquet` - Main analytics table
- `data/processed/users.parquet` - users table
- `data/processed/_run_meta.json` - Statistics 
- `reports/figures/` - EDA plots
- `reports/summary.md` - Findings + caveats


<br>
<br>
<br>

---
Bootcamp: AI Professionals | Week 2 <br>
Date: December 25, 2025
