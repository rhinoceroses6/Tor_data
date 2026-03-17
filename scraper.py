import requests
import json
import os
import sys
from datetime import datetime

def fetch_full_json():
    # Pobieramy pełne 'details'
    url = "https://onionoo.torproject.org/details"
    try:
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Błąd sieci: {e}")
        sys.exit(1)

def main():
    full_data = fetch_full_json()
    relays = full_data.get("relays", [])
    
    # Dzielimy listę węzłów na pół
    midpoint = len(relays) // 2
    part1_relays = relays[:midpoint]
    part2_relays = relays[midpoint:]

    # Przygotowujemy strukturę dla obu części
    data1 = {"relays": part1_relays, "relays_published": full_data.get("relays_published")}
    data2 = {"relays": part2_relays, "relays_published": full_data.get("relays_published")}

    if not os.path.exists("history"):
        os.makedirs("history")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    # Zapisujemy pliki
    def save_json(data, filename):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)

    # Pliki "latest" (stałe nazwy)
    save_json(data1, "latest_1.json")
    save_json(data2, "latest_2.json")
    
    # Pliki historyczne z timestampem
    save_json(data1, f"history/tor_details_{timestamp}_1.json")
    save_json(data2, f"history/tor_details_{timestamp}_2.json")
    
    print(f"Zapisano dane. Podział: {len(part1_relays)} + {len(part2_relays)} węzłów.")

if __name__ == "__main__":
    main()