# finance_api/utils/fetch_data.py
import yfinance as yf
import pandas as pd

def fetch_stock_data(ticker: str, period: str = "7d", interval: str = "1h") -> pd.DataFrame:
    """
    Télécharge et nettoie les données pour un ticker donné.

    Args:
        ticker (str): symbole (ex: "TSLA", "AAPL", "MC.PA")
        period (str): période ("1d", "3d", "7d", "1mo", etc.)
        interval (str): granularité ("15m", "1h", "1d", etc.)

    Returns:
        pd.DataFrame: dataframe propre avec colonnes ['date', 'time', 'Close', 'Volume']
    """

    # 1️⃣ Récupération via yfinance
    df = yf.Ticker(ticker).history(period=period, interval=interval)[["Close", "Volume"]]

    if df.empty:
        raise ValueError(f"Aucune donnée récupérée pour {ticker} ({period}, {interval})")

    # 2️⃣ Nettoyage & formatage
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = df.index.tz_convert("UTC")
    else:
        raise TypeError("Index is not a DatetimeIndex, cannot convert timezone.")
    df = df.interpolate(method="time", limit_direction="both").reset_index()

    df.rename(columns={"Datetime": "datetime"}, inplace=True)
    df["date"] = df["datetime"].dt.strftime("%Y-%m-%d")
    df["time"] = df["datetime"].dt.strftime("%H:%M:%S")

    # 3️⃣ Optionnel : calcul du pourcentage de variation horaire
    df["change_pct"] = df["Close"].pct_change() * 100
    df["change_pct"] = df["change_pct"].round(3).fillna(0)

    # 4️⃣ Colonnes finales dans l’ordre logique
    df = df[["date", "time", "Close", "Volume", "change_pct"]]

    return df


def to_json_format(ticker: str, name: str, df: pd.DataFrame) -> dict:
    """
    Transforme le DataFrame en JSON structuré pour le front.

    Args:
        ticker (str): symbole de l’entreprise
        name (str): nom lisible
        df (pd.DataFrame): dataframe propre

    Returns:
        dict: dictionnaire formaté prêt à être renvoyé par FastAPI
    """
    return {
        "ticker": ticker,
        "name": name,
        "data": [
            {
                "date": row["date"],
                "time": row["time"],
                "close": round(row["Close"], 2),
                "volume": int(row["Volume"]),
                "change_pct": float(row["change_pct"]),
            }
            for _, row in df.iterrows()
        ],
    }
