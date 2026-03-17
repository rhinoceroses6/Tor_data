import requests
import json
import os
import sys
from datetime import datetime

def fetch_full_json():
    url = "https://onionoo.torproject.org/details"
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"BŁĄD: {e}")
        sys.exit(1)

def main():
    # Pobieramy pełne dane
    full_data = fetch_full_json()
    
    # Wyciągamy tablicę węzłów
    relays = full_data.get("relays", [])
    
    # Tworzymy kopię nagłówka (wszystko co nie jest listą 'relays')
    header = {k: v for k, v in full_data.items() if k != "relays"}
    
    # Dzielimy listę węzłów na pół
    midpoint = len(relays) // 2
    
    # Tworzymy dwa nowe słowniki, które mają ten sam nagłówek, co oryginał
    data1 = header.copy()
    data1["relays"] = relays[:midpoint]
    
    data2 = header.copy()
    data2["relays"] = relays[midpoint:]

    if not os.path.exists("history"):
        os.makedirs("history")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    def save_json(data, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    # Zapisujemy
    save_json(data1, "latest_1.json")
    save_json(data2, "latest_2.json")
    
    save_json(data1, f"history/tor_details_{timestamp}_part1.json")
    save_json(data2, f"history/tor_details_{timestamp}_part2.json")
    
    print(f"Sukces! Dane z nagłówkiem podzielone na pół. Łącznie {len(relays)} węzłów.")

if __name__ == "__main__":
    main()