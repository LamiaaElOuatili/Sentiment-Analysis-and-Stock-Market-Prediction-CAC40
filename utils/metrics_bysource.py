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
    
    # Calcul du prix de clôture et du rendement
    daily_close = df.groupby('date')['close'].last().reset_index()
    daily_close['daily_return'] = daily_close['close'].pct_change()
    
    # Volatilité journalière = écart-type des prix du jour
    daily_vol = df.groupby('date')['close'].std().reset_index().rename(columns={'close': 'volatility'})
    
    daily_df = pd.merge(daily_close, daily_vol, on='date')
    return daily_df


def compute_daily_sentiment(sentiment_entries):
    df = pd.DataFrame(sentiment_entries)
    df['date'] = pd.to_datetime(df['date'])
    df = df[['date', 'mean_sentiment']].sort_values('date').reset_index(drop=True)
    return df


def safe_corr(x, y):
    """Renvoie corrélation ou 0 si non calculable."""
    if len(x) < 2 or x.isnull().all() or y.isnull().all():
        return 0.0
    corr = np.corrcoef(x.fillna(0), y.fillna(0))[0, 1]
    return 0.0 if np.isnan(corr) else corr


# -----------------------------
# --- Metrics calculation ---
# -----------------------------
metrics_all = {}

for ticker, company_name in sentiment_data['tickers'].items():
    if ticker not in stocks_data['tickers']:
        continue

    stock_daily = compute_daily_stock(stocks_data['tickers'][ticker]['data'])
    sentiment_company = sentiment_data['data'].get(ticker, {})

    sentiment_global = compute_daily_sentiment(sentiment_company.get('global', []))
    if stock_daily.empty or sentiment_global.empty:
        continue

    merged_global = pd.merge(stock_daily, sentiment_global, on='date', how='inner')

    # --- Corrélations (globale, 7j, 15j) ---
    corr_global = safe_corr(merged_global['daily_return'], merged_global['mean_sentiment'])
    corr_7d = safe_corr(merged_global['daily_return'].tail(7), merged_global['mean_sentiment'].tail(7))
    corr_15d = safe_corr(merged_global['daily_return'].tail(15), merged_global['mean_sentiment'].tail(15))

    # --- Volatilité ---
    vol_global = merged_global['volatility'].mean() if not merged_global['volatility'].isnull().all() else 0.0
    vol_7d = merged_global['volatility'].tail(7).mean() if len(merged_global) >= 7 else vol_global
    vol_15d = merged_global['volatility'].tail(15).mean() if len(merged_global) >= 15 else vol_global

    # --- Corrélation et volatilité par source ---
    correlations_by_source = {}
    for source, entries in sentiment_company.get('by_source', {}).items():
        df_source = compute_daily_sentiment(entries)
        if df_source.empty:
            correlations_by_source[source] = {
                "corr_global": 0.0,
                "corr_7d": 0.0,
                "corr_15d": 0.0,
                "vol_global": 0.0,
                "vol_7d": 0.0,
                "vol_15d": 0.0
            }
            continue

        merged_source = pd.merge(stock_daily, df_source, on='date', how='inner')
        if merged_source.empty:
            correlations_by_source[source] = {
                "corr_global": 0.0,
                "corr_7d": 0.0,
                "corr_15d": 0.0,
                "vol_global": 0.0,
                "vol_7d": 0.0,
                "vol_15d": 0.0
            }
            continue

        # Corrélations source
        c_global = safe_corr(merged_source['daily_return'], merged_source['mean_sentiment'])
        c_7d = safe_corr(merged_source['daily_return'].tail(7), merged_source['mean_sentiment'].tail(7))
        c_15d = safe_corr(merged_source['daily_return'].tail(15), merged_source['mean_sentiment'].tail(15))

        # Volatilités source
        v_global = merged_source['volatility'].mean()
        v_7d = merged_source['volatility'].tail(7).mean()
        v_15d = merged_source['volatility'].tail(15).mean()

        correlations_by_source[source] = {
            "corr_global": c_global,
            "corr_7d": c_7d,
            "corr_15d": c_15d,
            "vol_global": v_global,
            "vol_7d": v_7d,
            "vol_15d": v_15d
        }

    metrics_all[company_name] = {
        "correlation": {
            "global": corr_global,
            "7d": corr_7d,
            "15d": corr_15d
        },
        "volatility": {
            "global": vol_global,
            "7d": vol_7d,
            "15d": vol_15d
        },
        "by_source": correlations_by_source
    }


# -----------------------------
# --- Save to JSON ---
# -----------------------------
with open("company_metrics.json", "w", encoding="utf-8") as f:
    json.dump(metrics_all, f, indent=2, ensure_ascii=False)

print("✅ Metrics JSON generated successfully!")
