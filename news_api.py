import requests
import pandas as pd
from datetime import datetime, timedelta

# Your API key
API_KEY = "c682a0eeff664f1b83715929492c7007"

# List of 10 companies/tickers
companies = ["AAPL", "MSFT", "AMZN", "GOOGL", "TSLA",
           "MC.PA", "TTE.PA", "SAN.PA", "AIR.PA", "SU.PA"]

# Date range: last month
end_date = datetime.today()
start_date = end_date - timedelta(days=30)
from_date = start_date.strftime("%Y-%m-%d")
to_date = end_date.strftime("%Y-%m-%d")

# Prepare a list to store all articles
all_articles = []

# Loop through each company
for company in companies:
    url = (
    f"https://newsapi.org/v2/everything?"
    f"q={company}&"
    f"from={from_date}&"
    f"to={to_date}&"
    f"sortBy=publishedAt&"
    f"pageSize=100&"
    f"language=en&"  # <-- filter English articles
    f"apiKey={API_KEY}"
    )

    response = requests.get(url)
    data = response.json()

    # Check if articles exist
    articles = data.get("articles", [])
    for article in articles:
        all_articles.append({
            "Company": company,
            "Title": article.get("title"),
            "Summary": article.get("description"),
            "URL": article.get("url"),
            "PublishedAt": article.get("publishedAt")
        })

# Convert to DataFrame
df = pd.DataFrame(all_articles)
