from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.etl import ETLConfig, run_etl

cfg = ETLConfig(root=ROOT, 
                raw_orders = ROOT/"data"/"raw"/"orders_x.csv",
                raw_users = ROOT/"data"/"raw"/"users_x.csv",
                out_orders_clean = ROOT/"data"/"processed"/"orders_clean.parquet",
                out_users = ROOT/"data"/"processed"/"users.parquet",
                out_analytics = ROOT/"data"/"processed"/"analytics_table.parquet",
                run_meta = ROOT/"data"/"processed"/"_run_meta.json",
                reports = ROOT/"reports")

run_etl(cfg)