from config import cols, ETF_REGISTRY, APIKEY, OUTPUT_PATH, PARQUET_FILE_PATH, ETF_list_PATH
import pandas as pd

def save_raport(df, OUTPUT_PATH):
    df.to_csv(OUTPUT_PATH, index=False)
