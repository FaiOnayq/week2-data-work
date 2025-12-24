from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet, read_parquet
from bootcamp_data.transforms import parse_datetime, add_time_parts, add_outlier_flag, winsorize
from bootcamp_data.quality import require_columns, assert_non_empty, assert_unique_key
from bootcamp_data.joins import safe_left_join
import pandas as pd

import logging

log = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    path = make_paths(ROOT)
    
    orders_output = path.processed / "orders_clean.parquet"
    orders_output_mimic = path.processed / "orders_clean_mimic.parquet"
    users_output =  path.processed / "users.parquet"
    
    orders = pd.read_parquet(orders_output)
    print(orders)
    users = pd.read_parquet(users_output)
    log.info("Rows: orders_raw=%s, users=%s", len(orders), len(users))

        
    require_columns(orders, ["order_id", "user_id", "amount","created_at", "status", "quantity"])
    require_columns(users, ["user_id","country","signup_date"])
    
    
    assert_non_empty(orders, "orders")
    assert_non_empty(users, "users")
    
    assert_unique_key(orders, "order_id")
    assert_unique_key(users, "user_id")
    
    orders_dt = parse_datetime(orders, "created_at", utc=True)
    orders_dt = add_time_parts(orders_dt, "created_at")
    
    n_dt_missing = int(orders_dt["created_at"].isna().sum())
    print("missing created_at after parse:", n_dt_missing)
    
    join_orders_users = safe_left_join(orders_dt, users, "user_id", validate="many_to_one", suffixes=("", "_user"))
    assert len(join_orders_users) == len(orders_dt), f"Row count changed to {len(join_orders_users)}"
    print(f" Count of 'orders' rows \n before join: {len(orders_dt)} \n after join: {len(join_orders_users)}")
    match_rate = 1.0 - float(join_orders_users["country"].isna().mean())
    print("match rate for 'country' after join:", round(match_rate, 2))
    
    summary =(join_orders_users.groupby("country", dropna=False)
                .agg(revenue=("amount","sum"), orders=("order_id","size")).reset_index())
    print("#-- Summary: \n ",summary)
    summary.to_csv(ROOT/"reports"/"revenue_by_country.csv", index=False)
    
    # may add lo, hi with iqr_bounds function
    join_orders_users = join_orders_users.assign(amount_winsor=winsorize(join_orders_users["amount"]))
    join_orders_users = add_outlier_flag(join_orders_users, "amount", k=1.5)
    print(join_orders_users["amount__is_outlier"])
    
    output_path = path.processed / "analytics_table.parquet"
    
    write_parquet(join_orders_users, output_path)
    print("wrote:", output_path)
    log.info("Wrote processed outputs: %s", path.processed)
    
    df = pd.read_parquet(output_path)
    print(df.dtypes)
    print(df.head())
    
    
    




