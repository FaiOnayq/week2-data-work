
from pathlib import Path
import pandas as pd

NA = ["", "NA", "N/A", "null", "None"]


def read_orders_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={'order_id':"String"}, na_values=NA)
    

def read_users_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, dtype={'user_id':"String"}, na_values=NA)
    

def write_parquet(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)
    


def read_parquet(path):
    return pd.read_parquet(path)
    
    