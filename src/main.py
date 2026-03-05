"""
Scrape and compare ETF chart data from alphavantage
https://www.alphavantage.co/documentation/#etf-profile
"""
import pandas as pd
import re
import requests
#from transform import load
from extract import load_ETF, load_ETF_LIST, ETF_ADD, ETF_holdings
from load import save_raport
from io import StringIO
from config import cols, ETF_REGISTRY, APIKEY, OUTPUT_PATH, PARQUET_FILE_PATH, ETF_list_PATH
import csv
from pathlib import Path
import sys 
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import queue

DATA_DIR = ETF_list_PATH

class AddETFWindow(tk.Toplevel):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent

        self.title("Dodaj ETF")
        self.geometry("300x120")

        ttk.Label(self, text="Ticker:").pack(pady=5)

        self.ticker_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.ticker_var).pack()

        ttk.Button(self, text="Dodaj", command=self.add_etf).pack(pady=10)

    def add_etf(self):
        ticker = self.ticker_var.get().strip().upper()

        if not ticker:
            messagebox.showerror("Błąd", "Podaj ticker")
            return

        df = pd.read_parquet(PARQUET_FILE_PATH)
        if df["symbol"].eq(ticker).any():
            ETF_ADD(ticker)
            messagebox.showinfo("Informacja", "Dodano")

        else:
            print("Nie można pobrać danych. Taki ETF nie istnieje w bazie")  

        
        self.parent.refresh_list()
        self.destroy()


class ETFSelector(tk.Tk):
    def __init__(self):
        super().__init__()

        self.title("ETF Portfolio Builder")
        self.geometry("500x600")

        self.etf_vars = {}     # ticker -> BooleanVar
        self.weight_vars = {}  # ticker -> StringVar

        self._build_ui()
        self.refresh_list()
        self._updating = False

    def _build_ui(self):
        top_frame = ttk.Frame(self)
        top_frame.pack(fill="x", pady=5)

        ttk.Button(top_frame, text="Dodaj ETF", command=self.open_add_window).pack()

        self.canvas = tk.Canvas(self)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.scroll_frame = ttk.Frame(self.canvas)

        self.scroll_frame.bind(
            "<Configure>",
            lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        self.canvas.create_window((0, 0), window=self.scroll_frame, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        ttk.Button(self, text="Zatwierdź", command=self.get_selection).pack(pady=10)
        
    def refresh_list(self):
        # wyczyść UI
        for widget in self.scroll_frame.winfo_children():
            widget.destroy()

        self.etf_vars.clear()
        self.weight_vars.clear()

        tickers = sorted([p.stem for p in DATA_DIR.glob("*.parquet")])

        for ticker in tickers:
            frame = ttk.Frame(self.scroll_frame)
            frame.pack(fill="x", pady=2, padx=5)

            var = tk.BooleanVar()
            weight_var = tk.StringVar()

            self.etf_vars[ticker] = var
            self.weight_vars[ticker] = weight_var

            chk = ttk.Checkbutton(frame, text=ticker, variable=var)
            chk.pack(side="left")

            entry = ttk.Entry(frame, textvariable=weight_var, width=8)
            entry.pack(side="right")

            # 🔹 automatyczne przeliczenie
            #weight_var.trace_add("write", self.recalculate_percentages)

    def recalculate_percentages(self, *args):
        if self._updating:
            return

        self._updating = True

        try:
            values = []
            for var in self.weight_vars.values():
                try:
                    values.append(float(var.get()))
                except (ValueError, TypeError):
                    values.append(0.0)

            total = sum(values)

            if total == 0:
                return

            for var, val in zip(self.weight_vars.values(), values):
                percent = (val / total) * 100
                var.set(f"{percent:.2f}")

        finally:
            self._updating = False

    def przelicz(self, df):

        df["weight"] = pd.to_numeric(df["weight"], errors="coerce")
        df["weight_portolio"] = pd.to_numeric(df["weight_portolio"], errors="coerce")
        df = df.loc[df["weight"] >= 0].copy()
        
        df = df.loc[
            ~(
                (df["symbol"] == "n/a") &
                (df["description"] == "n/a")
            )
        ].copy()
        df["stock_weight_at_portofolio"] = ((df["weight"] * df["weight_portolio"]))
        df = (
            df
            .groupby("description", as_index=False)
            .agg({
                "symbol": "first",  # tylko do wyświetlania
                "stock_weight_at_portofolio": "sum"
            })
            .sort_values("stock_weight_at_portofolio", ascending=False)
            .reset_index(drop=True)
        )
        
        return df
        
        

    def get_selection(self):
        selected = {}
        dfs = []
        for ticker, var in self.etf_vars.items():
            if var.get():
                selected[ticker] = self.weight_vars[ticker].get()
                file_path = Path(ETF_list_PATH) / f"{ticker}.parquet"
                
                df_part = ETF_holdings(file_path)

                if not df_part.empty:
                    df_part["weight_portolio"] = self.weight_vars[ticker].get()
                    dfs.append(df_part)
                portfolio_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

                portfolio_df = self.przelicz(portfolio_df)
          
        print(portfolio_df)
        return portfolio_df

    def open_add_window(self):
        AddETFWindow(self)

def main():

    # client = ISharesClient()
    # client_SWDA = ISharesClient()

    ticker = "KWEB"
    #nasd = df_loaded[df_loaded["symbol"] == ticker]
    #ETF_ADD(ticker)
    

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
    app = ETFSelector()
    app.mainloop()
   # main()