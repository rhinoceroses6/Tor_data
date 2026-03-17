import requests
import json
import os
from datetime import datetime

def fetch_full_json():
    url = "https://onionoo.torproject.org/details"
    print(f"[{datetime.now()}] Pobieranie pełnego JSONa z Onionoo...")
    
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        # Zwracamy surowe dane (słownik Pythonowy)
        return response.json()
    except Exception as e:
        print(f"Błąd podczas pobierania: {e}")
        exit(1)

def main():
    data = fetch_full_json()
    
    # Tworzymy folder historyczny
    if not os.path.exists("history"):
        os.makedirs("history")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # Zapisujemy "latest.json" (zawsze najświeższy plik)
    with open("latest.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        
    # Zapisujemy historię w folderze history/
    history_filename = f"history/tor_details_{timestamp}.json"
    with open(history_filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        
    print(f"Zapisano pełny zrzut danych: {history_filename}")

if __name__ == "__main__":
    main()