import pandas as pd

def require_columns(df: pd.DataFrame, cols: list):
    missing = [c for c in cols if c not in df.columns ]
    assert not missing, f"missing columns {missing}"
    
    
def assert_non_empty(df, name="df"):
    assert len(df)>0 , f'{name} is empty'
    
def assert_unique_key(df, key, allow_na=False):
    if not allow_na:
        assert df[key].isna().all(), f"There is empty cell in {key} "
    dups = df[key].duplicated(keep=False).sum()
    assert dups==0 , f'the {key} has {dups} times of duplicated'


def assert_in_range(series, lo=None, hi=None, name="value"):
    if lo is not None:
        assert (series.dropna()>lo).all() , f"{name} has value lower than {lo}"
    if hi is not None:
        assert (series.dropna()<hi).all() , f"{name} has value lower than {hi}"
        
