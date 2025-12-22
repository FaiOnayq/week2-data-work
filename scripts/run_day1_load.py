from bootcamp_data.config import make_paths
from bootcamp_data.io import read_orders_csv, read_users_csv, write_parquet, read_parquet
from bootcamp_data.transforms import enforce_schema
from pathlib import Path
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]



if __name__ == "__main__":
    path = make_paths(ROOT)
    
    orders = read_orders_csv(path.raw / "orders.csv")
    users = read_users_csv(path.raw / "users.csv")
    
    orders_schema = enforce_schema(orders)
    
    orders_output = path.processed / "orders.parquet"
    users_output =  path.processed / "users.parquet"
    
    write_parquet(orders_schema, orders_output)
    write_parquet(users, users_output)
    

    df = pd.read_parquet(orders_output)
    print(df.dtypes)
    print(df.head())

    
    
    



