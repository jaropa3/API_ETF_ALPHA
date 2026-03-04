"""
Scrape and compare ETF chart data from alphavantage
https://www.alphavantage.co/documentation/#etf-profile
"""

import pandas as pd
import re
import requests
from transform import merge
from io import StringIO
from config import cols, ETF_REGISTRY, APIKEY, OUTPUT_PATH
import csv
from pathlib import Path

def save_raport(df, OUTPUT_PATH):
    df.to_csv(OUTPUT_PATH, index=False)

class ISharesClient:
    BASE_SUFFIX = "1467271812596.ajax" #numer ajax nie jest przypadkowy — ale też nie zmienia się od lat.

    def build_url(self, ticker):
        meta = ETF_REGISTRY[ticker]
        return (
            f"https://www.ishares.com/{meta['region']}/products/{meta['id']}/{meta.get('slug')}/"
            f"{self.BASE_SUFFIX}"
            f"?fileType=csv&fileName={ticker}_holdings&dataType=fund"
        )
    def build_url_2(self, ticker):
        meta = ETF_REGISTRY[ticker]
        return (
            f"https://www.ishares.com/uk/individual/en/products/251882/ishares-msci-world-ucits-etf-acc-fund/1506575576011.ajax?fileType=csv&fileName=SWDA_holdings&dataType=fund"
        )
    def fetch(self, ticker):
        url = self.build_url(ticker)
        raw = requests.get(url, timeout=30).text
        second_line = raw.splitlines()[1]
        print(ticker, second_line)
        return raw

def main():

    # client = ISharesClient()
    # client_SWDA = ISharesClient()
    PARQUET_FILE = Path("etf_list.parquet")

    if PARQUET_FILE.exists():
        df = pd.read_parquet(PARQUET_FILE)
        print("Wczytano z istniejącego pliku Parquet.")
    else:
    
        CSV_URL = 'https://www.alphavantage.co/query?function=LISTING_STATUS&apikey={APIKEY}}'

        with requests.Session() as s:
            download = s.get(CSV_URL)
            download.raise_for_status()  # dopilnuj, żeby nie przeszło 404/500
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            # pierwszy wiersz to nagłówki
            headers = my_list[0]
            data_rows = my_list[1:]

# zamiana w DataFrame
            df = pd.DataFrame(data_rows, columns=headers)
            df.to_parquet("etf_list.parquet", index=False)

# późniejsze wczytanie
    df_loaded = pd.read_parquet("etf_list.parquet")
    nasd = df_loaded[df_loaded["symbol"] == "SPY"]
        
    print(nasd)

    #raw_csv = client.fetch("SOXX")
     
    
    #raw_csv_SWDA = client.fetch("SWDA")

    # df = pd.read_csv(client.build_url("SOXX"), skiprows=10, header=None)   
    # df_SWDA = pd.read_csv(client_SWDA.build_url_2("SWDA"), skiprows=10, header=None)    
    # df.columns = cols + list(df.columns[len(cols):])
    # df_SWDA.columns = cols
    # df["waga"] = 0.4
    
    # df_SWDA["waga"] = 0.6
   
    # merge(df, df_SWDA)

    # df_merge = pd.concat([df, df_SWDA], ignore_index=True)
    # df_merge["waga_spółek"] = (df_merge["waga"] * df_merge["weight_pct"]) 
    # df_merge = df_merge.dropna(subset=['name'])
    

    # pattern = "Cash"
    # mask = df_merge["sector"].str.contains(pattern, case=False, na=False)
    # df_merge = df_merge.loc[~mask]

    # df_merge = df_merge.groupby(["ticker"])["waga_spółek"].sum()
    # df_merge = df_merge.sort_values(ascending=False).reset_index(drop=False)
    # print(df_merge)

if __name__ == "__main__":
    main()