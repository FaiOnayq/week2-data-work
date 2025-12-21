import pandas as pd

def enforce_schema(df) -> df:
    if "amount" in df.columns or "quantity" in df.columns :
        return df.assign(
            amount=pd.to_numeric(df["amount"], errors="coerce").astype("Float64"),
            quantity=pd.to_numeric(df["quantity"], errors="coerce").astype("Int64")
        )
    return df.assign()