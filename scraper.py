import requests
import json
import os
import sys
from datetime import datetime

def fetch_full_json():
    # Pobieramy pełne dane z Onionoo
    url = "https://onionoo.torproject.org/details"
    print(f"[{datetime.now()}] Pobieranie danych z: {url}")
    
    try:
        # Timeout 300 sekund dla dużych plików
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"BŁĄD: {e}")
        sys.exit(1)

def main():
    full_data = fetch_full_json()
    relays = full_data.get("relays", [])
    
    # Dzielimy listę węzłów na pół, aby pliki miały < 100MB
    midpoint = len(relays) // 2
    part1_relays = relays[:midpoint]
    part2_relays = relays[midpoint:]

    # Przygotowujemy strukturę JSON
    data1 = {"relays": part1_relays, "relays_published": full_data.get("relays_published")}
    data2 = {"relays": part2_relays, "relays_published": full_data.get("relays_published")}

    if not os.path.exists("history"):
        os.makedirs("history")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # Funkcja pomocnicza do zapisu
    def save_json(data, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    # Pliki "latest" (zawsze nadpisywane)
    save_json(data1, "latest_1.json")
    save_json(data2, "latest_2.json")
    
    # Pliki historyczne (z timestampem, nigdy nie nadpisywane)
    save_json(data1, f"history/tor_details_{timestamp}_part1.json")
    save_json(data2, f"history/tor_details_{timestamp}_part2.json")
    
    print(f"Sukces! Dane zapisano (łącznie {len(relays)} węzłów).")

if __name__ == "__main__":
    main()