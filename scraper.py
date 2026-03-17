import requests
import pandas as pd
import os
from datetime import datetime

def fetch_and_process_tor_data():
    url = "https://onionoo.torproject.org/details"
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Błąd pobierania: {e}")
        exit(1)

    relays = data.get("relays", [])
    processed = []
    
    for r in relays:
        if not r.get("running", False):
            continue
            
        platform = r.get("platform", "Unknown")
        os_name = platform.split(" on ")[-1] if " on " in platform else platform
        
        processed.append({
            "Nickname": r.get("nickname", "Unnamed"),
            "Country": str(r.get("country", "??")).upper(),
            "AS_Name": r.get("as_name", "Unknown"),
            "Version": r.get("version", "Unknown"),
            "OS": os_name,
            "Consensus_Weight": r.get("consensus_weight", 0) or 0
        })
    return pd.DataFrame(processed)

def main():
    # 1. Pobierz dane
    df = fetch_and_process_tor_data()
    df.sort_values(by="Consensus_Weight", ascending=False, inplace=True)
    
    # 2. Przygotuj ścieżki
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    if not os.path.exists("history"):
        os.makedirs("history")
    
    # 3. Zapisz: 
    # Plik główny (najświeższy)
    df.to_csv("latest.csv", index=False, encoding='utf-8')
    # Plik historyczny w folderze history/
    df.to_csv(f"history/tor_report_{timestamp}.csv", index=False, encoding='utf-8')
    
    print(f"Zapisano dane o godzinie {timestamp}")

if __name__ == "__main__":
    main()