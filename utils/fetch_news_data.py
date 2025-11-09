from datetime import datetime, timedelta
import pandas as pd

csv_path = ""

def filter_sentiments(csv_path: str, ticker: str, period: str):
    """
    Filtre les lignes d'un CSV selon le ticker et la période.
    
    Args:
        csv_path (str): Chemin du fichier CSV.
        ticker (str): Nom ou symbole de l'entreprise (ex: 'AAPL').
        period (str): Durée, ex: '7j' ou '30j'.
    
    Returns:
        pd.DataFrame: DataFrame filtrée.
    """
    # Lire le CSV
    df = pd.read_csv(csv_path)
    
    # Nettoyer le nom des colonnes (au cas où)
    df.columns = df.columns.str.strip()

    # Convertir les dates au format datetime
    df["PublishedAt"] = pd.to_datetime(df["PublishedAt"], utc=True).dt.tz_localize(None)
    
    # Calculer la date de début selon la période
    days = int(period.replace("j", ""))  # ex: '7j' -> 7
    now = datetime.now()
    start_date = now - timedelta(days=days)


    # Filtrer par ticker et période
    filtered = df[
        (df["Company"].str.upper() == ticker.upper())
        & (df["PublishedAt"] >= start_date)
        & (df["PublishedAt"] <= now)
    ]

    return filtered
