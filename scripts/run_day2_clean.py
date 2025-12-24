from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet, read_parquet
from bootcamp_data.transforms import enforce_schema, missingness_report, normalize_text, add_missing_flags, apply_mapping
from bootcamp_data.quality import require_columns, assert_non_empty
import pandas as pd

import logging

log = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    path = make_paths(ROOT)
    
    log.info("Loading raw inputs")
    orders = read_orders_csv(path.raw / "orders.csv")
    #--
    orders_mimic = read_orders_csv(path.raw / "empty.csv")
    users = read_users_csv(path.raw / "users.csv")
    log.info("Rows: orders_raw=%s, users=%s", len(orders), len(users))
    
    
    require_columns(orders, ["order_id", "user_id", "amount","created_at", "status", "quantity"])
    #--
    require_columns(orders_mimic, ["order_id", "user_id", "amount","created_at", "status", "quantity"])

    require_columns(users, ["user_id","country","signup_date"])
    
    assert_non_empty(orders, "orders")
    assert_non_empty(orders_mimic,"orders_mimic" )
    assert_non_empty(users, "users")

    
    orders_schema = enforce_schema(orders)
    orders_schema_mimic = enforce_schema(orders_mimic)

    
    missingness_report(orders_schema).to_csv(ROOT / "reports" / "missingness_orders.csv")
    missingness_report(orders_schema_mimic).to_csv(ROOT / "reports" / "missingness_orders_mimic.csv")
    log.info("Wrote missingness report: %s", ROOT / "reports")

    print(orders_schema["status"].unique())
    print(orders_schema_mimic["status"].unique())

    orders_norm = orders_schema.assign(status_norm=normalize_text(orders_schema["status"]))
    orders_norm_mimic = orders_schema_mimic.assign(status_norm=normalize_text(orders_schema_mimic["status"]))

    print("before:", orders_norm["status"].unique())
    print("after:", orders_norm["status_norm"].unique())
    
    status_norm = normalize_text(orders_schema["status"])
    status_norm_mimic = normalize_text(orders_schema_mimic["status"])

    mapping = {"paid": "paid", "refund": "refund", "refunded": "refund"}
    status_map = apply_mapping(status_norm, mapping)
    status_map_mimic = apply_mapping(status_norm_mimic, mapping)

    orders_clean = orders_schema.assign(status_proccess=status_map)
    orders_clean_mimic = orders_schema_mimic.assign(status_proccess=status_map_mimic)

    orders_flags=add_missing_flags(orders_clean, ["amount","quantity"])
    orders_flags_mimic=add_missing_flags(orders_clean_mimic, ["amount","quantity"])
    
    orders_output = path.processed / "orders_clean.parquet"
    orders_output_mimic = path.processed / "orders_clean_mimic.parquet"

    users_output =  path.processed / "users.parquet"
    
    write_parquet(orders_flags, orders_output)
    write_parquet(orders_flags_mimic, orders_output_mimic)

    write_parquet(users, users_output)
    log.info("Wrote processed outputs: %s", path.processed)
    
    df = pd.read_parquet(orders_output)
    print(df.dtypes)
    print(df.head())
    
    
    
    



