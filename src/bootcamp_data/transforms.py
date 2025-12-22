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



