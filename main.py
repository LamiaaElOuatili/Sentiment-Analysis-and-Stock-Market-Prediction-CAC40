# finance_api/main.py
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Literal
from fastapi.responses import JSONResponse
import os

from finance_api.utils.fetch_data_fin import fetch_stock_data, to_json_format
from finance_api.utils.fetch_news_data import filter_sentiments
from finance_api.utils.fetch_reddit_data import filter_and_analyze_posts
from finance_api.utils.fetch_google_data import filter_news_by_company


app = FastAPI(title="Finance Data API", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

TICKERS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "AMZN": "Amazon",
    "GOOGL": "Alphabet (Google)",
    "TSLA": "Tesla",
    "MC.PA": "LVMH",
    "TTE.PA": "TotalEnergies",
    "SAN.PA": "Sanofi",
    "AIR.PA": "Airbus",
    "SU.PA": "Schneider Electric"
}

@app.get("/")
def root():
    return {
        "message": "📊 Welcome to the Finance Data API",
        "available_endpoints": {
            "/stocks": "Get stock data by ticker and period",
        },
        "example_usage": "/stocks?ticker=TSLA&period=7d"
    }


@app.get("/stocks")
def get_stock_data(
    ticker: str = Query(...), 
    period: Literal["1d", "3d", "7d", "1mo"] = "7d",
    interval: Literal["15m", "1h", "1d"] = "1h"
):
    if ticker not in TICKERS:
        return {"error": f"Ticker '{ticker}' non reconnu."}

    df = fetch_stock_data(ticker, period, interval)
    return to_json_format(ticker, TICKERS[ticker], df)

@app.get("/tickers")
def get_tickers():
    return [{"ticker": t, "name": n} for t, n in TICKERS.items()]


CSV_PATH = os.path.join("finance_api", "data", "news_sentiment_raw.csv")


@app.get("/get_new_sentiments")
def get_sentiments(
    ticker: str = Query(..., description="Ticker ou nom de l'entreprise (ex: AAPL)"),
    period: str = Query("7j", description="Période en jours, ex: 7j ou 30j")
):
    """
    Retourne les sentiments d'une entreprise sur une période donnée.
    """
    try:
        df = filter_sentiments(CSV_PATH, ticker, period)
        if df.empty:
            return JSONResponse({"message": "Aucun article trouvé pour cette période."})
        
        return df.to_dict(orient="records")
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)



@app.get("/get_reddit_sentiments")
def get_reddit_sentiments(
    company_name: str = Query(..., description="Nom de l'entreprise"),
    days_back: int = Query(7, description="Nombre de jours dans le passé")
):
    # Path to the single JSON containing all companies
    file_path = "finance_api/data/reddit/reddit_data.json"

    try:
        result = filter_and_analyze_posts(file_path, company_name, days_back)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    

# FastAPI endpoint
@app.get("/get_news_sentiments_google")
def get_news_sentiments(
    company_name: str = Query(..., description="Company name"),
    days_back: int = Query(7, description="Number of days to look back")
):
    file_path = "finance_api/data/stock_news_google.json"  # path to your JSON file
    try:
        result = filter_news_by_company(file_path, company_name, days_back)
        return JSONResponse(result)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)