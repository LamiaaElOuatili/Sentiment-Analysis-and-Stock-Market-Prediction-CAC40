import pandas as pd
import json
from collections import defaultdict

# -----------------------------
# --- Load data files ---
# -----------------------------

# Reddit JSON
with open(r"C:\Users\user\Desktop\Fichiers_2\ENSIIE_DD\S5\backend\hackathon2025\finance_api\data\reddit\reddit_data.json", "r", encoding="utf-8") as f:
    reddit_raw = json.load(f)

# Google News JSON
with open(r"C:\Users\user\Desktop\Fichiers_2\ENSIIE_DD\S5\backend\hackathon2025\finance_api\data\stock_news_google.json", "r", encoding="utf-8") as f:
    news_google_flat_list = json.load(f)

# CSV news
news_df = pd.read_csv(r"C:\Users\user\Desktop\Fichiers_2\ENSIIE_DD\S5\backend\hackathon2025\finance_api\data\news_sentiment_raw.csv")
news_flat_list = news_df.to_dict(orient="records")

# -----------------------------
# --- Helper functions ---
# -----------------------------

def flatten_reddit_json(reddit_json):
    """Flatten nested Reddit JSON into a list of posts with company info."""
    flat_list = []
    for company_entry in reddit_json:
        company_name = company_entry['company']
        for post in company_entry.get('posts', []):
            flat_post = post.copy()
            flat_post['company'] = company_name
            flat_list.append(flat_post)
    return flat_list

def group_by_company(flat_list, company_key='company'):
    """Group a list of dicts by company name (case-insensitive)."""
    grouped = defaultdict(list)
    for item in flat_list:
        key = item.get(company_key)
        if key:
            grouped[key.lower()].append(item)  # lowercase keys
    return grouped

# -----------------------------
# --- Daily sentiment functions ---
# -----------------------------

def compute_daily_sentiment_reddit(reddit_json):
    df = pd.DataFrame(reddit_json)
    if df.empty:
        return pd.DataFrame(columns=['Date', 'AvgSentiment'])
    
    if 'date' not in df.columns:
        raise ValueError("Reddit JSON must have a 'date' column")
    
    df['PublishedAt'] = pd.to_datetime(df['date'])
    df['SentimentNumeric'] = df['sentiment'].apply(lambda s: 1 if s > 0.05 else (-1 if s < -0.05 else 0))
    df['Date'] = df['PublishedAt'].dt.date
    daily_sentiment = df.groupby('Date')['SentimentNumeric'].mean().reset_index()
    daily_sentiment.rename(columns={'SentimentNumeric': 'AvgSentiment'}, inplace=True)
    return daily_sentiment

def compute_daily_sentiment_news(news_json):
    df = pd.DataFrame(news_json)
    if df.empty:
        return pd.DataFrame(columns=['Date', 'AvgSentiment'])
    
    if 'PublishedAt' not in df.columns and 'published_at' in df.columns:
        df['PublishedAt'] = pd.to_datetime(df['published_at'])
    else:
        df['PublishedAt'] = pd.to_datetime(df['PublishedAt'])
    
    if 'Sentiment' in df.columns:
        df['SentimentNumeric'] = df['Sentiment'].map({'positive':1, 'neutral':0, 'negative':-1})
    elif 'sentiment_score' in df.columns:
        df['SentimentNumeric'] = df['sentiment_score']
    else:
        raise ValueError("News JSON must have 'Sentiment' or 'sentiment_score' column")
    
    df['Date'] = df['PublishedAt'].dt.date
    daily_sentiment = df.groupby('Date')['SentimentNumeric'].mean().reset_index()
    daily_sentiment.rename(columns={'SentimentNumeric': 'AvgSentiment'}, inplace=True)
    return daily_sentiment

def compute_daily_sentiment_news_google(news_json):
    return compute_daily_sentiment_news(news_json)  # same as news

# -----------------------------
# --- Generate daily sentiment JSON per company ---
# -----------------------------

def generate_daily_sentiment_json(company_name, reddit_json=[], news_json=[], news_google_json=[]):
    output = []

    sources = [
        ("reddit", reddit_json, compute_daily_sentiment_reddit),
        ("news", news_json, compute_daily_sentiment_news),
        ("news_google", news_google_json, compute_daily_sentiment_news_google)
    ]

    for source_name, data, func in sources:
        if data:
            daily_df = func(data)
            
            # Convert Date column to string for JSON
            if not daily_df.empty:
                daily_df['Date'] = daily_df['Date'].apply(lambda d: d.isoformat() if hasattr(d, 'isoformat') else str(d))

            daily_list = daily_df.to_dict(orient='records')
            output.append({
                "source": source_name,
                "company": company_name,
                "daily_sentiment": daily_list
            })

    return output  # return as Python object

# -----------------------------
# --- Main processing ---
# -----------------------------

# Flatten Reddit posts
reddit_flat_list = flatten_reddit_json(reddit_raw)

# Group by company (case-insensitive)
reddit_data = group_by_company(reddit_flat_list, company_key='company')
news_data = group_by_company(news_flat_list, company_key='Company')
news_google_data = group_by_company(news_google_flat_list, company_key='company')

# List of companies
companies = [
    "LVMH","TotalEnergies","Sanofi","Airbus","Schneider Electric",
    "Apple","Microsoft","Amazon","Alphabet (Google)","Tesla",
    "lvmh", "totalenergies", "sanofi", "airbus", "schneider electric",
    "apple", "microsoft", "amazon", "alphabet (google)", "tesla"
]

# Store all companies sentiment
all_companies_sentiment = []

for company in companies:
    key = company.lower()  # lowercase for fetching
    company_sentiment = generate_daily_sentiment_json(
        company_name=company,
        reddit_json=reddit_data.get(key, []),
        news_json=news_data.get(key, []),
        news_google_json=news_google_data.get(key, [])
    )
    all_companies_sentiment.extend(company_sentiment)

# Save to JSON
with open("all_companies_daily_sentiment.json", "w", encoding="utf-8") as f:
    json.dump(all_companies_sentiment, f, indent=2, ensure_ascii=False)

print("✅ Fichier JSON généré pour toutes les entreprises !")
