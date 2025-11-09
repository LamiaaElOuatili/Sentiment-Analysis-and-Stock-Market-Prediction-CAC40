import pandas as pd
import json
import numpy as np

# -----------------------------
# --- Load JSON files ---
# -----------------------------
with open(r"C:\Users\user\Desktop\Fichiers_2\ENSIIE_DD\S5\backend\hackathon2025\finance_api\data\stocks.json", "r", encoding="utf-8") as f:
    stocks_data = json.load(f)

with open(r"C:\Users\user\Desktop\Fichiers_2\ENSIIE_DD\S5\backend\hackathon2025\finance_api\data\sentiment_compact.json", "r", encoding="utf-8") as f:
    sentiment_data = json.load(f)

# -----------------------------
# --- Helper functions ---
# -----------------------------
def compute_daily_stock(stock_entries):
    df = pd.DataFrame(stock_entries)
    df['date'] = pd.to_datetime(df['date'])
    
    daily_close = df.groupby('date')['close'].last().reset_index()
    daily_close['daily_return'] = daily_close['close'].pct_change()
    
    daily_vol = df.groupby('date')['close'].std().reset_index().rename(columns={'close':'volatility'})
    
    daily_df = pd.merge(daily_close, daily_vol, on='date')
    return daily_df

def compute_daily_sentiment(sentiment_entries):
    df = pd.DataFrame(sentiment_entries)
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'mean_sentiment']].sort_values('date').reset_index(drop=True)
    return df

def safe_corr(x, y):
    if len(x) < 2 or x.isnull().all() or y.isnull().all():
        return np.nan
    return np.corrcoef(x.fillna(0), y.fillna(0))[0,1]

# -----------------------------
# --- Metrics calculation ---
# -----------------------------
metrics_all = {}

for ticker, company_name in sentiment_data['tickers'].items():
    if ticker not in stocks_data['tickers']:
        continue
    
    stock_daily = compute_daily_stock(stocks_data['tickers'][ticker]['data'])
    sentiment_entries = sentiment_data['data'].get(ticker, {}).get('global', [])
    sentiment_df = compute_daily_sentiment(sentiment_entries)
    
    if sentiment_df.empty or stock_daily.empty:
        continue
    
    merged = pd.merge(stock_daily, sentiment_df, on='date', how='inner')
    if merged.empty:
        continue
    
    # --- Compute cumulative correlation only ---
    cum_corrs = []
    for i in range(2, len(merged)+1):
        x = merged['daily_return'].iloc[:i]
        y = merged['mean_sentiment'].iloc[:i]
        cum_corrs.append(safe_corr(x, y))
    merged['cum_corr'] = [np.nan] + cum_corrs  # first value NaN
    
    # --- Convert date to string for JSON ---
    merged['date'] = merged['date'].dt.strftime('%Y-%m-%d')
    
    metrics_all[company_name] = merged[['date','daily_return','volatility','mean_sentiment','cum_corr']].to_dict(orient='records')

# -----------------------------
# --- Save metrics to JSON ---
# -----------------------------
with open("company_metrics.json", "w", encoding="utf-8") as f:
    json.dump(metrics_all, f, indent=2, ensure_ascii=False)

print("✅ Metrics JSON generated successfully!")
