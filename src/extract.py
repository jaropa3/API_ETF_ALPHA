from config import cols, ETF_REGISTRY, APIKEY, OUTPUT_PATH, PARQUET_FILE_PATH, ETF_list_PATH
import pandas as pd
import sys
import requests
from pathlib import Path

def load_ETF(ETF_ticker):

        url = f"https://www.alphavantage.co/query?function=ETF_PROFILE&symbol={ETF_ticker}&apikey={APIKEY}"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if not data:
            raise ValueError(f"Brak danych dla {ETF_ticker}")

        df = pd.DataFrame([data])
        
        if "holdings" not in data:
            sys.exit(f"df jest pusty")
        else:
            output_path = ETF_list_PATH / f"{ETF_ticker}.parquet"
            df.to_parquet(output_path, index=False)
            return df
        
def load_ETF_LIST():
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

def ETF_ADD(ticker):

        TICKER_PATH = Path(rf"F:\ITwork\API_ETF_ALPHA\data\etf_holdings\{ticker}.parquet")
        if TICKER_PATH.exists():
            df = pd.read_parquet(TICKER_PATH)
            #do loggera
            print(f"Wczytano {ticker} z istniejącego pliku Parquet.")
        else:
            load_ETF(ticker)
            #do loggera
            print("Wczytano z API.")    
            
def ETF_holdings(TICKER_PATH):
        df = pd.read_parquet(TICKER_PATH)     
         
        holdings_df = (
        df[["holdings"]]
        .explode("holdings")
        .dropna()
    )
        holdings_df = pd.json_normalize(holdings_df["holdings"])
        return holdings_df