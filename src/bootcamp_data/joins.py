import pandas as pd


def safe_left_join(left, right, key, validate, suffixes=("_L","_R")):
    pd.merge(left, right, on=key, how="left", validate=validate)
    
    
