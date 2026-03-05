from pathlib import Path

ETF_REGISTRY = {
    "SOXX": {"id": "239705", "region": "us", "slug" : "ishares-phlx-semiconductor-etf"},
    "SWDA": {"id": "251882", "region": "uk", "slug": "ishares-msci-world-ucits-etf-acc-fund"},
}

cols = [
    "ticker",
    "name",
    "sector",
    "asset_class",
    "market_value",
    "weight_pct",
    "notional_value",
    "shares",
    "price",
    "country",
    "exchange",
    "currency",
]

APIKEY = "2CSNN9OZC14L43X5"

OUTPUT_PATH = "F:\ITwork\API_ETF_ALPHA\data\ETF_list.csv"

ETF_list = {
    "SMH",
    "QWD",

}

PARQUET_FILE_PATH = "F:\ITwork\API_ETF_ALPHA\data\etf_list.parquet"

ETF_list_PATH = Path(r"F:\ITwork\API_ETF_ALPHA\data\etf_holdings")