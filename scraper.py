import requests
import json
import os
import sys
from datetime import datetime

def main():
    url = "https://onionoo.torproject.org/details"
    print(f"[{datetime.now()}] Pobieranie...")
    
    # Pobieramy surowy tekst (nie parsowany przez json.json() na starcie)
    response = requests.get(url, timeout=300)
    response.raise_for_status()
    full_text = response.text

    # Parsujemy tylko po to, żeby znaleźć miejsce podziału w tablicy "relays"
    data = json.loads(full_text)
    relays = data.get("relays", [])
    
    # Dzielimy indeksy
    mid = len(relays) // 2
    
    # --- TERAZ MAGIA: Dzielimy tekstowo ---
    # Znajdujemy fragmenty w tekście, żeby zachować surowy format API
    # Szukamy początku tablicy relays
    prefix = full_text.split('"relays":[')[0] + '"relays":['
    suffix = ']}'
    
    # Rozdzielamy tablicę obiektów
    relays_json_list = json.dumps(relays, separators=(',', ':'))[1:-1].split('},{')
    
    # Naprawiamy nawiasy po splicie
    p1 = [relays_json_list[i] + ('}' if i == 0 else ('{' + relays_json_list[i] + '}') if i < len(relays_json_list)-1 else '{') for i in range(len(relays_json_list))]
    # To jest zbyt skomplikowane, uprośćmy:
    
    # PO PROSTU podzielmy listę obiektów na dwa stringi
    part1_str = json.dumps(relays[:mid], separators=(',', ':'))[1:-1]
    part2_str = json.dumps(relays[mid:], separators=(',', ':'))[1:-1]
    
    # Składamy: nagłówek + część_relays + zamykający nawias
    final1 = prefix + part1_str + suffix
    final2 = prefix + part2_str + suffix

    if not os.path.exists("history"):
        os.makedirs("history")
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    
    def save_raw(content, filename):
        with open(filename, "w", encoding="utf-8") as f:
            f.write(content)

    save_raw(final1, "latest_1.json")
    save_raw(final2, "latest_2.json")
    save_raw(final1, f"history/tor_details_{timestamp}_part1.json")
    save_raw(final2, f"history/tor_details_{timestamp}_part2.json")

    print("Gotowe. Pliki zachowują oryginalną strukturę nagłówka.")

if __name__ == "__main__":
    main()