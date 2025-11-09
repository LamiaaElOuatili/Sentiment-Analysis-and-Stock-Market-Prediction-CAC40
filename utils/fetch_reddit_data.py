import os
import re
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

def load_all_companies_json(file_path):
    """Load the JSON file containing all companies."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def filter_and_analyze_posts(file_path, company_name, days_back):
    """
    Filter posts for a specific company and period, 
    and compute sentiment statistics.
    """
    data = load_all_companies_json(file_path)

    # Find the company in the JSON
    company_data = next((item for item in data if item['company'].lower() == company_name.lower()), None)
    if not company_data:
        return {"error": f"Aucun JSON trouvé pour '{company_name}'"}

    df = pd.DataFrame(company_data['posts'])

    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'], errors='coerce')

    # Filter by period
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=days_back)
    df_filtered = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()

    if df_filtered.empty:
        return {"error": f"Aucun post trouvé pour '{company_name}' dans les {days_back} derniers jours."}

    # Ensure sentiment is numeric
    df_filtered['sentiment_numeric'] = df_filtered['sentiment'].map(lambda x: float(x) if pd.notnull(x) else None)

    # Replace NaN/inf with None
    df_filtered = df_filtered.replace([np.inf, -np.inf], np.nan)
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)

    # Convert date back to string for JSON
    df_filtered['date'] = df_filtered['date'].astype(str)

    mean_sentiment = df_filtered['sentiment_numeric'].mean()
    if mean_sentiment is not None and (np.isnan(mean_sentiment) or np.isinf(mean_sentiment)):
        mean_sentiment = None

    sentiment_counts = df_filtered['sentiment'].value_counts().to_dict()

    result = {
        "company": company_name,
        "days_back": days_back,
        "num_posts": len(df_filtered),
        "mean_sentiment": mean_sentiment,
        "sentiment_counts": sentiment_counts,
        "posts": df_filtered.to_dict(orient='records')
    }

    # Make everything JSON-safe
    return json.loads(json.dumps(result, default=str))