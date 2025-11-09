# finance_api/utils/fetch_news.py
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse


def filter_news_by_company(file_path: str, company_name: str, days_back: int):
    """
    Load JSON news data and filter by company and period.
    """
    # Load the JSON file
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Filter by company name (case-insensitive)
    df = pd.DataFrame([item for item in data if item['company'].lower() == company_name.lower()])

    if df.empty:
        return {"error": f"No articles found for '{company_name}'."}

    # Convert published_at to datetime and remove timezone
    df['published_at'] = pd.to_datetime(df['published_at'], errors='coerce').dt.tz_convert(None)

    # Filter by date period
    end_date = datetime.utcnow()  # naive UTC
    start_date = end_date - timedelta(days=days_back)
    df_filtered = df[(df['published_at'] >= start_date) & (df['published_at'] <= end_date)].copy()

    if df_filtered.empty:
        return {"error": f"No articles for '{company_name}' in the last {days_back} days."}

    # Ensure numeric fields are JSON-safe
    df_filtered = df_filtered.replace([np.inf, -np.inf], np.nan)
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)

    # Convert datetime to string for JSON
    df_filtered['published_at'] = df_filtered['published_at'].astype(str)

    # Prepare result
    result = {
        "company": company_name,
        "days_back": days_back,
        "num_articles": len(df_filtered),
        "articles": df_filtered.to_dict(orient='records')
    }

    return json.loads(json.dumps(result, default=str))
