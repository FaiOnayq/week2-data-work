from dataclasses import dataclass, asdict

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet, read_parquet
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.transforms import enforce_schema, missingness_report, normalize_text, add_missing_flags, apply_mapping, parse_datetime, add_time_parts, add_outlier_flag, winsorize
from bootcamp_data.joins import safe_left_join
import pandas as pd
import logging
import json

log = logging.getLogger(__name__)

@dataclass(frozen=True) 
class ETLConfig: 
    root: Path 
    raw_orders: Path 
    raw_users: Path 
    out_orders_clean: Path 
    out_users: Path 
    out_analytics: Path 
    run_meta: Path
    reports: Path
    
    
def load_inputs(cfg: ETLConfig):
    orders = read_orders_csv(cfg.raw_orders)
    users = read_users_csv(cfg.raw_users)
    return orders, users


def transform(orders: pd.DataFrame, users: pd.DataFrame): 
    # assert
    require_columns(orders, ["order_id", "user_id", "amount","created_at", "status", "quantity"])
    require_columns(users, ["user_id","country","signup_date"])
    
    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    
    assert_unique_key(orders, "order_id")
    assert_unique_key(users, "user_id")
    
    # norm and clean
    orders_schema = enforce_schema(orders)
    
    status_norm = normalize_text(orders_schema["status"])
    
    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund", "paided": "paid", "success": "paid"}
    status_map = apply_mapping(status_norm, mapping)
    orders_clean = orders_schema.assign(status_proccess=status_map)
    
    # add 'amount' and 'quantity' missing flags
    orders=add_missing_flags(orders_clean, ["amount","quantity"])
    
    # add time parts
    orders_dt = parse_datetime(orders, "created_at", utc=True)
    users = parse_datetime(users, "signup_date", utc=True)
    orders_dt = add_time_parts(orders_dt, "created_at")
    
    # joins    
    join_orders_users = safe_left_join(orders_dt, users, "user_id", validate="many_to_one", suffixes=("", "_user"))
    assert len(join_orders_users) == len(orders_dt), f"Row count changed to {len(join_orders_users)}"
    
    # winsor and outlier flag
    join_orders_users = join_orders_users.assign(amount_winsor=winsorize(join_orders_users["amount"]))
    join_orders_users = add_outlier_flag(join_orders_users, "amount", k=1.5)
    
    return join_orders_users


def load_outputs(analtrics: pd.DataFrame, users: pd.DataFrame, cfg: ETLConfig):
    write_parquet(analtrics, cfg.out_analytics)
    write_parquet(users, cfg.out_users)
    

def write_run_meta(analtrics: pd.DataFrame, cfg: ETLConfig):
    n_dt_missing = int(analtrics["created_at"].isna().sum())
    
    match_rate = round(1.0 - float(analtrics["country"].isna().mean()),2)
    
    meta = {
    "rows_count": int(len(analtrics)),
    "missing_created_at": n_dt_missing,
    "country_match_rate": match_rate,
    "config": {k: str(v) for k, v in asdict(cfg).items()},
    }
    
    cfg.run_meta.parent.mkdir(parents=True, exist_ok=True)
    cfg.run_meta.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    

def run_etl(cfg: ETLConfig) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s") 
    
    log.info("Loading inputs") 
    orders, users = load_inputs(cfg)
    log.info("Rows:\n orders_raw=%s, users=%s", len(orders), len(users))
    
    log.info("Transforming orders, users")
    analytics = transform(orders, users)
    
    log.info("Wrote missingness report: %s", ROOT / "reports")    
    missingness_report(analytics).to_csv(cfg.reports/ "missingness_orders.csv", index=False)
    
    log.info("Wrote summary of revenue by country report: %s", ROOT / "reports")   
    summary =(analytics.groupby("country", dropna=False)
                .agg(revenue=("amount","sum"), orders=("order_id","size")).reset_index())
    summary.to_csv(cfg.reports/"revenue_by_country.csv", index=False)
    
    log.info("Writing outputs to %s", cfg.out_analytics.parent)
    load_outputs(analytics, users, cfg)
    
    log.info("Writing run metadata: %s", cfg.run_meta)
    write_run_meta(analytics, cfg)
