import json
import requests
import pandas as pd
from datetime import datetime

def fetch_and_process_tor_data():
    url = "https://onionoo.torproject.org/details"
    print(f"[{datetime.now()}] Pobieranie danych z Onionoo API...")
    
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"Błąd podczas pobierania danych: {e}")
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
            "Fingerprint": r.get("fingerprint", "Unknown"),
            "Country": str(r.get("country", "??")).upper(),
            "AS_Number": r.get("as", "Unknown"),
            "AS_Name": r.get("as_name", "Unknown"),
            "Version": r.get("version", "Unknown"),
            "OS": os_name,
            "Consensus_Weight": r.get("consensus_weight", 0) or 0
        })

    return pd.DataFrame(processed)

def main():
    df = fetch_and_process_tor_data()
    # Format daty: RRRR-MM-DD_HH-MM, aby uniknąć nadpisywania plików w ciągu dnia
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # Sortowanie najważniejsze
    df.sort_values(by="Consensus_Weight", ascending=False, inplace=True)
    
    # --- ZAPIS CSV ---
    csv_filename = f"tor_report_{timestamp}.csv"
    df.to_csv(csv_filename, index=False, encoding='utf-8')
    df.to_csv("latest_tor_data.csv", index=False, encoding='utf-8')
    
    # --- ZAPIS JSON ---
    json_filename = f"tor_report_{timestamp}.json"
    json_data = df.to_dict(orient='records')
    
    with open(json_filename, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
        
    with open("latest_tor_data.json", "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
        
    print(f"[{datetime.now()}] Zapisano raporty z datą {timestamp}")

if __name__ == "__main__":
    main()