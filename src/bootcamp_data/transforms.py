import pandas as pd

def enforce_schema(df) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string") if "order_id" in df.columns else None,
        user_id=df["user_id"].astype("string") if "user_id" in df.columns else None,
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64") if "amount" in df.columns else None,
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64") if "quantity" in df.columns else None,
        )

def missingness_report(df) -> pd.DataFrame:
    n = len(df)
    missing = df.isna().sum()
    report = missing.to_frame(name="n_missing")
    report["p_missing"] = missing/n
    return report.sort_values("p_missing", ascending=False)
    

def add_missing_flags(df, cols) -> pd.DataFrame:
    out = df.copy()
    for c in cols:
        out[f"{c}__isna"] = out[c].isna()
    return out

def normalize_text(s: pd.Series):
    return(s.apply(lambda x: x.strip().casefold()))

def apply_mapping(s: pd.Series , mapping: dict):
    return s.map(lambda x: mapping.get(x, x))
    
def dedupe_keep_latest(df: pd.DataFrame, key_cols: list[str], ts_col: str) -> pd.DataFrame:
    return df.sort_values(ts_col).drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


def parse_datetime(df, col, utc=True):
    df[col] = pd.to_datetime(df[col], errors="coerce", utc=utc)
    return df

def add_time_parts(df, ts_col):
    ts = df[ts_col] 
    return df.assign( date=ts.dt.date, year=ts.dt.year, month=ts.dt.to_period("M").astype("string"), dow=ts.dt.day_name(),hour=ts.dt.hour, quarter= ts.dt.quarter )


# output lower and higher outliers
def iqr_bounds(s, k=1.5):
    s = s.dropna()
    q1 = s.quantile(0.25)
    q3 = s.quantile(0.75)
    iqr = q3 - q1
    return float(q1 - k*iqr), float(q3 + k*iqr)

def winsorize(s, lo=0.01, hi=0.99):
    s = s.dropna()
    a = s.quantile(lo)
    b = s.quantile(hi)
    return s.clip(lower=a, upper=b)

def add_outlier_flag(df, col, k=1.5):
    lo , hi = iqr_bounds(df[col],k)
    col_name = f'{col}__is_outlier'
    df[col_name] = ((df[col] < lo) | (df[col] > hi)).fillna(False)
    return df