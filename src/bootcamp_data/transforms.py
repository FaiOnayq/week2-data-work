import pandas as pd

def enforce_schema(df) -> pd.DataFrame:
    return df.assign(
        order_id=df["order_id"].astype("string") if "order_id" in df.columns else None,
        user_id=df["user_id"].astype("string") if "user_id" in df.columns else None,
        amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64") if "amount" in df.columns else None,
        quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64") if "quantity" in df.columns else None,
        )
