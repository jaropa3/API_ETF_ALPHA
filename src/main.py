"""
Scrape and compare ETF chart data from alphavantage
https://www.alphavantage.co/documentation/#etf-profile
"""

import pandas as pd
import re
import requests
#from transform import merge
from io import StringIO
from config import cols, ETF_REGISTRY, APIKEY, OUTPUT_PATH, PARQUET_FILE_PATH, ETF_list_PATH
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

def load_ETF(ETF_ticker):

        url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ETF_ticker}&apikey={APIKEY}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            raise ValueError(f"Brak danych dla {ETF_ticker}")

        df = pd.DataFrame([data])
        output_path = ETF_list_PATH / f"{ETF_ticker}.parquet"
        df.to_parquet(output_path, index=False)
        return df

def main():

    # client = ISharesClient()
    # client_SWDA = ISharesClient()
    PARQUET_FILE = Path(PARQUET_FILE_PATH)

    if PARQUET_FILE.exists():
        df = pd.read_parquet(PARQUET_FILE)
        #print("Wczytano z istniejącego pliku Parquet.")
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
            df.to_parquet(PARQUET_FILE_PATH, index=False)

# późniejsze wczytanie
    df_loaded = pd.read_parquet(PARQUET_FILE_PATH)

    ticker = "REMX"
    nasd = df_loaded[df_loaded["symbol"] == ticker]
    TICKER_PATH = Path(f"F:\ITwork\API_ETF_ALPHA\data\{ticker}.parquet")
    if TICKER_PATH.exists():
        df = pd.read_parquet(TICKER_PATH)
        print(f"Wczytano {ticker} z istniejącego pliku Parquet.")
    else:
        load_ETF(ticker)
        print("Wczytano z API.")    
        
    df = pd.read_parquet(TICKER_PATH)
    
    holdings_df = (
    df[["holdings"]]
    .explode("holdings")
    .dropna()
)

    holdings_df = pd.json_normalize(holdings_df["holdings"])

    print(holdings_df.head(10))
    
    #raw_csv = client.fetch("SOXX")
     
    

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