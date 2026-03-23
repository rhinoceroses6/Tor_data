"""
Historyczny analizator węzłów sieci Tor (Onionoo JSON)
═══════════════════════════════════════════════════════
Wczytuje wszystkie pliki JSON z podanego folderu, sortuje je wg daty
i generuje raport zmian między kolejnymi snapshotami.

Użycie:
  python tor_history_analyzer.py <folder>
  python tor_history_analyzer.py <folder> --country de
  python tor_history_analyzer.py <folder> --filter new,gone,flags,bw,as
  python tor_history_analyzer.py <folder> --from 2025-01-01 --to 2026-01-01
  python tor_history_analyzer.py <folder> --export changes.csv
  python tor_history_analyzer.py <folder> --html raport.html
  python tor_history_analyzer.py <folder> --json zmiany.json
"""

import json
import sys
import os
import csv
import argparse
from datetime import datetime, date
from collections import defaultdict, Counter
from pathlib import Path
from typing import Optional


# ══════════════════════════════════════════════════════════════════
# Wczytywanie snapshotów
# ══════════════════════════════════════════════════════════════════

def date_from_filename(name: str) -> Optional[datetime]:
    """
    Wyciąga datę i czas z nazwy pliku.
    Obsługiwane wzorce (przed rozszerzeniem):
      tor_details_2026-03-18_03-34_part1   → 2026-03-18 03:34
      tor_details_2026-03-18_03-34         → 2026-03-18 03:34
      tor_details_2026-03-18               → 2026-03-18 00:00
      cokolwiek_2026-03-18T03:34           → 2026-03-18 03:34
    """
    import re
    stem = Path(name).stem  # bez rozszerzenia

    # wzorzec: YYYY-MM-DD_HH-MM (opcjonalnie _partN na końcu)
    m = re.search(r"(\d{4}-\d{2}-\d{2})[_T](\d{2})[-:](\d{2})", stem)
    if m:
        try:
            return datetime.strptime(f"{m.group(1)} {m.group(2)}:{m.group(3)}", "%Y-%m-%d %H:%M")
        except ValueError:
            pass

    # fallback: sama data YYYY-MM-DD
    m = re.search(r"(\d{4}-\d{2}-\d{2})", stem)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except ValueError:
            pass

    return None


def part_number(name: str) -> int:
    """Zwraca numer 'partN' z nazwy pliku (0 jeśli brak)."""
    import re
    m = re.search(r"part(\d+)", name, re.IGNORECASE)
    return int(m.group(1)) if m else 0


def snapshot_key(name: str, dt) -> str:
    """Klucz grupowania: data+czas bez numeru partu."""
    if dt:
        return dt.strftime("%Y-%m-%d %H:%M")
    return Path(name).stem


def load_snapshots(folder: str) -> list[dict]:
    """
    Wczytuje wszystkie pliki JSON z folderu.
    Pliki z tym samym znacznikiem czasu (part1, part2 itd.) są SCALANE
    w jeden snapshot przed porównaniem.
    """
    from collections import OrderedDict
    p = Path(folder)
    if not p.is_dir():
        print(f"❌  '{folder}' nie jest folderem.")
        sys.exit(1)

    # 1. Wczytaj wszystkie pliki
    raw = []
    for f in p.glob("*.json"):
        try:
            with open(f, encoding="utf-8") as fh:
                data = json.load(fh)
            dt = date_from_filename(f.name) or parse_dt(data.get("relays_published", ""))
            published = dt.strftime("%Y-%m-%d %H:%M:%S") if dt else data.get("relays_published", f.stem)
            raw.append({
                "file":       f.name,
                "published":  published,
                "published_dt": dt,
                "part":       part_number(f.name),
                "key":        snapshot_key(f.name, dt),
                "relays_raw": data.get("relays", []),
            })
        except Exception as e:
            print(f"⚠️  Pominięto {f.name}: {e}")

    raw.sort(key=lambda s: (s["published_dt"] or datetime.min, s["part"]))

    # 2. Grupuj po kluczu czasowym i scalaj relay'e
    groups: dict = OrderedDict()
    for r in raw:
        k = r["key"]
        if k not in groups:
            groups[k] = {
                "files":        [],
                "published":    r["published"],
                "published_dt": r["published_dt"],
                "relays":       {},
            }
        groups[k]["files"].append(r["file"])
        for relay in r["relays_raw"]:
            fp = relay.get("fingerprint")
            if fp:
                groups[k]["relays"][fp] = relay

    snapshots = list(groups.values())

    # 3. Wydruk tabeli
    print(f"\n  {'Czas snapshotu':<22} {'Węzłów':>7}  Pliki")
    print("  " + "─"*72)
    for s in snapshots:
        files_str = ", ".join(s["files"])
        print(f"  {s['published']:<22} {len(s['relays']):>7}  {files_str}")

    merged = sum(1 for s in snapshots if len(s["files"]) > 1)
    if merged:
        print(f"\n  ℹ️  Scalono {merged} par plików part1+part2 w pojedyncze snapshoty.\n")

    return snapshots


def parse_dt(s: str) -> Optional[datetime]:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            pass
    return None


# ══════════════════════════════════════════════════════════════════
# Porównanie dwóch snapshotów
# ══════════════════════════════════════════════════════════════════

def compare(snap_a: dict, snap_b: dict) -> dict:
    """Zwraca słownik zmian między dwoma snapshotami."""
    ra, rb = snap_a["relays"], snap_b["relays"]
    fps_a, fps_b = set(ra), set(rb)

    new_fps   = fps_b - fps_a
    gone_fps  = fps_a - fps_b
    common    = fps_a & fps_b

    changes = {
        "from_file":       ", ".join(snap_a.get("files", [snap_a.get("file","?")])),
        "to_file":         ", ".join(snap_b.get("files", [snap_b.get("file","?")])),
        "from_date":       snap_a["published"],
        "to_date":         snap_b["published"],
        "new":             [rb[fp] for fp in new_fps],
        "gone":            [ra[fp] for fp in gone_fps],
        "flag_changes":    [],
        "version_changes": [],
        "bw_changes":      [],
        "as_changes":      [],
        "country_changes": [],
    }

    for fp in common:
        a, b = ra[fp], rb[fp]
        nick = b.get("nickname", fp[:8])

        # Flagi
        fa, fb = set(a.get("flags", [])), set(b.get("flags", []))
        if fa != fb:
            changes["flag_changes"].append({
                "fingerprint": fp, "nickname": nick,
                "added":   sorted(fb - fa),
                "removed": sorted(fa - fb),
            })

        # Wersja
        va, vb = a.get("version"), b.get("version")
        if va != vb:
            changes["version_changes"].append({
                "fingerprint": fp, "nickname": nick,
                "from": va, "to": vb,
                "status": b.get("version_status"),
            })

        # Przepustowość (zmiana > 10%)
        bwa = a.get("advertised_bandwidth", 0)
        bwb = b.get("advertised_bandwidth", 0)
        if bwa > 0:
            delta_pct = (bwb - bwa) / bwa * 100
            if abs(delta_pct) >= 10:
                changes["bw_changes"].append({
                    "fingerprint": fp, "nickname": nick,
                    "from_MB": round(bwa / 1e6, 2),
                    "to_MB":   round(bwb / 1e6, 2),
                    "delta_pct": round(delta_pct, 1),
                })

        # AS
        if a.get("as") != b.get("as"):
            changes["as_changes"].append({
                "fingerprint": fp, "nickname": nick,
                "from_as": f"{a.get('as')} {a.get('as_name','')}",
                "to_as":   f"{b.get('as')} {b.get('as_name','')}",
            })

        # Kraj
        if a.get("country") != b.get("country"):
            changes["country_changes"].append({
                "fingerprint": fp, "nickname": nick,
                "from": a.get("country_name", a.get("country")),
                "to":   b.get("country_name", b.get("country")),
            })

    return changes


# ══════════════════════════════════════════════════════════════════
# Agregacja timeline
# ══════════════════════════════════════════════════════════════════

def build_timeline(snapshots: list[dict]) -> dict:
    """Dla każdego fingerprinta buduje pełną historię obecności i zmian."""
    history = defaultdict(lambda: {
        "nickname": "?", "appearances": [], "flag_history": [],
        "version_history": [], "bw_history": [],
    })
    for snap in snapshots:
        dt = snap["published"]
        for fp, r in snap["relays"].items():
            h = history[fp]
            h["nickname"] = r.get("nickname", h["nickname"])
            h["appearances"].append(dt)
            h["flag_history"].append((dt, sorted(r.get("flags", []))))
            h["version_history"].append((dt, r.get("version")))
            h["bw_history"].append((dt, r.get("advertised_bandwidth", 0)))
    return dict(history)


def build_aggregate_trend(snapshots: list[dict]) -> list[dict]:
    """Agregat sieci per snapshot – do wykresów trendów."""
    trend = []
    for snap in snapshots:
        relays = list(snap["relays"].values())
        total_bw = sum(r.get("advertised_bandwidth", 0) for r in relays)
        country_cnt = Counter(r.get("country", "?") for r in relays)
        as_cnt      = Counter(f"{r.get('as','?')} {r.get('as_name','')}" for r in relays)
        trend.append({
            "date": snap["published"],
            "total": len(relays),
            "running": sum(1 for r in relays if r.get("running")),
            "exit": sum(1 for r in relays if "Exit" in r.get("flags", [])),
            "guard": sum(1 for r in relays if "Guard" in r.get("flags", [])),
            "total_bw_MB": round(total_bw / 1e6, 2),
            "top_countries": country_cnt.most_common(5),
            "top_as": as_cnt.most_common(10),
        })
    return trend



# ══════════════════════════════════════════════════════════════════
# Grupowanie: AS
# ══════════════════════════════════════════════════════════════════

def group_by_as(snapshots: list[dict]) -> None:
    """Wyświetla statystyki per AS dla każdego snapshotu + zmiany w czasie."""
    W2 = 80

    # Zbierz wszystkie AS ze wszystkich snapshotów
    all_as_keys = set()
    for snap in snapshots:
        for r in snap["relays"].values():
            all_as_keys.add(r.get("as", "?"))

    # Statystyki per AS per snapshot
    # as_data[as_key][snapshot_idx] = {count, bw, exit, guard}
    as_history: dict = defaultdict(list)
    for snap in snapshots:
        as_snap: dict = defaultdict(lambda: {"count":0,"bw":0,"exit":0,"guard":0,"name":""})
        for r in snap["relays"].values():
            k = r.get("as", "?")
            as_snap[k]["count"] += 1
            as_snap[k]["bw"]    += r.get("advertised_bandwidth", 0)
            as_snap[k]["name"]   = r.get("as_name", "")
            if "Exit"  in r.get("flags",[]): as_snap[k]["exit"]  += 1
            if "Guard" in r.get("flags",[]): as_snap[k]["guard"] += 1
        for k in all_as_keys:
            as_history[k].append(as_snap.get(k, {"count":0,"bw":0,"exit":0,"guard":0,"name":""}))

    # Sortuj po średniej liczbie węzłów
    as_sorted = sorted(as_history.items(),
                       key=lambda x: sum(s["count"] for s in x[1]) / len(x[1]),
                       reverse=True)

    print(f"\n{'═'*W2}")
    print(f"  GRUPOWANIE PO SIECI AS  ({len(snapshots)} snapshotów, top 30)")
    print(f"{'═'*W2}")

    last_snap_idx = len(snapshots) - 1
    hdr = f"  {'AS':<10} {'Nazwa':<32} {'Węzłów':>7} {'Exit':>5} {'Guard':>6} {'BW MB/s':>8}  Trend"
    print(hdr)
    print("  " + "─"*(W2-2))

    for asn, history in as_sorted[:30]:
        last = history[last_snap_idx]
        first = history[0]
        avg_cnt = sum(s["count"] for s in history) / len(history)
        bw_mb   = round(last["bw"] / 1e6, 1)
        name    = last["name"][:30] or first["name"][:30]

        # Mini trend ASCII: ostatnie 10 snapshotów
        trend_vals = [s["count"] for s in history[-10:]]
        mx = max(trend_vals) if max(trend_vals) > 0 else 1
        trend_str = "".join("▁▂▃▄▅▆▇█"[min(7, int(v/mx*7))] for v in trend_vals)

        delta = last["count"] - first["count"]
        delta_str = f" ({'▲+' if delta>0 else '▼'}{delta})" if delta != 0 else ""

        print(f"  {asn:<10} {name:<32} {last['count']:>7}{delta_str:<8} {last['exit']:>5} {last['guard']:>6} {bw_mb:>8.1f}  {trend_str}")

    print()

    # Zmiany obecności AS między pierwszym a ostatnim snapshotem
    first_as = {k for k, h in as_history.items() if h[0]["count"] > 0}
    last_as  = {k for k, h in as_history.items() if h[last_snap_idx]["count"] > 0}
    new_as   = last_as - first_as
    gone_as  = first_as - last_as

    if new_as:
        print(f"  🟢 Nowe sieci AS (nie było w 1. snapshocie): {', '.join(sorted(new_as))}")
    if gone_as:
        print(f"  🔴 Zniknięte sieci AS (brak w ostatnim): {', '.join(sorted(gone_as))}")
    print()


# ══════════════════════════════════════════════════════════════════
# Grupowanie: Rodziny węzłów
# ══════════════════════════════════════════════════════════════════

def group_by_family(snapshots: list[dict]) -> None:
    """Wyświetla statystyki per rodzina (effective_family) dla każdego snapshotu."""
    W2 = 80

    # Zbierz rodziny ze wszystkich snapshotów – rodzina to posortowany tuple FP członków
    # Używamy ostatniego snapshotu jako referencji (najbardziej aktualne dane)
    snap_last = snapshots[-1]
    snap_first = snapshots[0]

    def extract_families(snap: dict) -> dict:
        """Zwraca {family_id: {fps, nicknames, bw, exit, guard, as_set}}"""
        families: dict = {}
        for fp, r in snap["relays"].items():
            fam = r.get("effective_family", [fp])
            # Normalizuj: sortuj FP i użyj jako klucza
            fam_key = tuple(sorted(fam))
            if len(fam_key) < 2:
                continue  # pomiń samotne węzły
            if fam_key not in families:
                families[fam_key] = {
                    "fps": set(), "nicknames": [], "bw": 0,
                    "exit": 0, "guard": 0, "as_set": set(),
                }
            d = families[fam_key]
            d["fps"].add(fp)
            d["nicknames"].append(r.get("nickname","?"))
            d["bw"]   += r.get("advertised_bandwidth", 0)
            d["as_set"].add(r.get("as","?"))
            if "Exit"  in r.get("flags",[]): d["exit"]  += 1
            if "Guard" in r.get("flags",[]): d["guard"] += 1
        return families

    fam_last  = extract_families(snap_last)
    fam_first = extract_families(snap_first)

    print(f"\n{'═'*W2}")
    print(f"  GRUPOWANIE PO RODZINACH WĘZŁÓW")
    print(f"  Pierwszy snapshot: {snap_first['published'][:16]}  →  Ostatni: {snap_last['published'][:16]}")
    print(f"{'═'*W2}")

    if not fam_last:
        print("  ℹ️  Brak rodzin wieloczłonkowych w ostatnim snapshocie.")
        print("      (Węzły mają effective_family = [tylko_siebie])")
        print()
        return

    # Sortuj po liczbie węzłów
    sorted_fams = sorted(fam_last.items(), key=lambda x: len(x[1]["fps"]), reverse=True)

    print(f"  {'Rodzina (nick. reprezentanta)':<34} {'Węzłów':>7} {'Exit':>5} {'Guard':>6} {'BW MB/s':>8}  AS  Zmiana vs. pierwszy")
    print("  " + "─"*(W2-2))

    for fam_key, d in sorted_fams[:40]:
        rep_nick = sorted(d["nicknames"])[0][:30]
        bw_mb    = round(d["bw"] / 1e6, 1)
        as_str   = ",".join(sorted(d["as_set"]))[:14]
        count    = len(d["fps"])

        # Porównaj z pierwszym snapshotem
        prev = fam_last_prev = fam_first.get(fam_key)
        if prev:
            delta = count - len(prev["fps"])
            delta_str = f"{'▲+' if delta>0 else ('▼' if delta<0 else '═')}{abs(delta) if delta!=0 else ''}"
            delta_col = delta_str
        else:
            delta_col = "🟢 nowa"

        print(f"  {rep_nick:<34} {count:>7} {d['exit']:>5} {d['guard']:>6} {bw_mb:>8.1f}  {as_str:<14}  {delta_col}")

    # Podsumowanie
    keys_last  = set(fam_last.keys())
    keys_first = set(fam_first.keys())
    new_fams   = keys_last - keys_first
    gone_fams  = keys_first - keys_last

    print()
    print(f"  Łącznie rodzin wieloczłonkowych: {len(fam_last)}")
    if new_fams:
        print(f"  🟢 Nowe rodziny:      {len(new_fams)}")
    if gone_fams:
        print(f"  🔴 Rozwiązane rodziny: {len(gone_fams)}")

    # Szczegóły nowych rodzin
    if new_fams:
        print(f"\n  Nowe rodziny (nieobecne w 1. snapshocie):")
        for k in sorted(new_fams, key=lambda x: len(fam_last[x]["fps"]), reverse=True)[:10]:
            d = fam_last[k]
            nicks = ", ".join(sorted(d["nicknames"])[:5])
            print(f"    +{len(d['fps'])} węzłów: {nicks}")

    # Szczegóły rozwiązanych rodzin
    if gone_fams:
        print(f"\n  Rozwiązane rodziny (brak w ostatnim snapshocie):")
        for k in sorted(gone_fams, key=lambda x: len(fam_first[x]["fps"]), reverse=True)[:10]:
            d = fam_first[k]
            nicks = ", ".join(sorted(d["nicknames"])[:5])
            print(f"    -{len(d['fps'])} węzłów: {nicks}")
    print()

# ══════════════════════════════════════════════════════════════════
# Filtry
# ══════════════════════════════════════════════════════════════════

def apply_filters(all_changes: list[dict], args) -> list[dict]:
    """Filtruje listę zmian wg argumentów CLI."""
    result = []
    # Mapowanie polskich aliasów → angielskie klucze
    PL_ALIASES = {
        "nowe":        "new",
        "new":         "new",
        "zamkniete":   "gone",
        "zamknięte":   "gone",
        "znikniete":   "gone",
        "zniknięte":   "gone",
        "gone":        "gone",
        "flagi":       "flags",
        "flags":       "flags",
        "wersje":      "version",
        "wersja":      "version",
        "version":     "version",
        "przepustowosc": "bw",
        "przepustowość": "bw",
        "bw":          "bw",
        "as":          "as",
        "kraj":        "country",
        "country":     "country",
    }
    if args.filter:
        raw_types = [t.strip().lower() for t in args.filter.split(",")]
        mapped = {PL_ALIASES.get(t, t) for t in raw_types}
        unknown = {t for t, orig in zip(mapped, raw_types) if t not in PL_ALIASES.values()}
        if unknown - {"new","gone","flags","bw","version","as","country"}:
            print(f"⚠️  Nieznane typy filtra: {unknown}")
            print(f"   Dostępne: new/nowe, gone/zamknięte/zniknięte, flags/flagi,")
            print(f"             version/wersje, bw/przepustowość, as, country/kraj")
        allowed = mapped - unknown
        if not allowed:
            allowed = {"new","gone","flags","bw","version","as","country"}
    else:
        allowed = {"new","gone","flags","bw","version","as","country"}

    date_from = parse_dt(args.date_from + " 00:00:00") if args.date_from else None
    date_to   = parse_dt(args.date_to   + " 23:59:59") if args.date_to   else None

    for c in all_changes:
        dt_to = parse_dt(c["to_date"])
        if date_from and dt_to and dt_to < date_from:
            continue
        if date_to and dt_to and dt_to > date_to:
            continue

        fc = {k: v for k, v in c.items() if k not in ("new","gone","flag_changes","version_changes","bw_changes","as_changes","country_changes")}

        def filt_relay_list(items, key_field):
            if not args.country:
                return items
            return [i for i in items if i.get("country","").lower() == args.country.lower()
                    or i.get("country_name","").lower().startswith(args.country.lower())]

        fc["new"]             = filt_relay_list(c["new"], "country")             if "new"     in allowed else []
        fc["gone"]            = filt_relay_list(c["gone"], "country")            if "gone"    in allowed else []
        fc["flag_changes"]    = c["flag_changes"]                                if "flags"   in allowed else []
        fc["version_changes"] = c["version_changes"]                             if "version" in allowed else []
        fc["bw_changes"]      = c["bw_changes"]                                  if "bw"      in allowed else []
        fc["as_changes"]      = c["as_changes"]                                  if "as"      in allowed else []
        fc["country_changes"] = c["country_changes"]                             if "country" in allowed else []

        if args.country:
            def cc_filt(lst):
                return [x for x in lst if
                        rb.get(x.get("fingerprint",""), {}).get("country","").lower() == args.country.lower()
                        ] if False else lst  # uproszczone – filtr country dla zmian
            # Dla pozostałych list – brak łatwego dostępu do country bez lookup, zostawiamy

        result.append(fc)

    return result


# ══════════════════════════════════════════════════════════════════
# Wydruk terminalowy
# ══════════════════════════════════════════════════════════════════

W = 70

def hr(ch="─"): print(ch * W)
def section(title): print(f"\n{'═'*W}\n  {title}\n{'═'*W}")

def print_changes(all_changes: list[dict], trend: list[dict]) -> None:
    section("PODSUMOWANIE TRENDÓW SIECI")
    hdr = f"{'Data':<22} {'Węzłów':>7} {'Exit':>6} {'Guard':>6} {'BW [MB/s]':>10}"
    print(hdr)
    hr()
    for t in trend:
        print(f"{t['date']:<22} {t['total']:>7} {t['exit']:>6} {t['guard']:>6} {t['total_bw_MB']:>10.1f}")

    for c in all_changes:
        section(f"ZMIANY: {c['from_date']}  →  {c['to_date']}")
        print(f"  Pliki: {c['from_file']} → {c['to_file']}")

        if c["new"]:
            print(f"\n  🟢 NOWE WĘZŁY ({len(c['new'])})")
            hr("·")
            for r in c["new"]:
                flags = " ".join(r.get("flags",[]))
                bw    = round(r.get("advertised_bandwidth",0)/1e6, 2)
                asn   = f"{r.get('as','?')} {r.get('as_name','')}"[:28]
                print(f"  + {r.get('nickname','?'):<22} {r.get('country_name', r.get('country','?')):<16} {asn:<30} {bw:>6.2f} MB/s  [{flags}]")

        if c["gone"]:
            print(f"\n  🔴 ZNIKNIĘTE WĘZŁY ({len(c['gone'])})")
            hr("·")
            for r in c["gone"]:
                flags = " ".join(r.get("flags",[]))
                bw    = round(r.get("advertised_bandwidth",0)/1e6, 2)
                asn   = f"{r.get('as','?')} {r.get('as_name','')}"[:28]
                print(f"  - {r.get('nickname','?'):<22} {r.get('country_name', r.get('country','?')):<16} {asn:<30} {bw:>6.2f} MB/s  [{flags}]")

        if c["flag_changes"]:
            print(f"\n  🏳  ZMIANY FLAG ({len(c['flag_changes'])})")
            hr("·")
            for x in c["flag_changes"]:
                added   = "+"+",".join(x["added"])   if x["added"]   else ""
                removed = "-"+",".join(x["removed"]) if x["removed"] else ""
                print(f"  {x['nickname']:<22}  {added:<25} {removed}")

        if c["version_changes"]:
            print(f"\n  🔄 ZMIANY WERSJI ({len(c['version_changes'])})")
            hr("·")
            for x in c["version_changes"]:
                print(f"  {x['nickname']:<22}  {x['from']} → {x['to']}  [{x['status']}]")

        if c["bw_changes"]:
            print(f"\n  📶 ZMIANY PRZEPUSTOWOŚCI ≥10% ({len(c['bw_changes'])})")
            hr("·")
            for x in sorted(c["bw_changes"], key=lambda i: i["delta_pct"]):
                arrow = "▲" if x["delta_pct"] > 0 else "▼"
                print(f"  {x['nickname']:<22}  {x['from_MB']:>7.2f} → {x['to_MB']:>7.2f} MB/s  {arrow}{abs(x['delta_pct']):.1f}%")

        if c["as_changes"]:
            print(f"\n  🌐 ZMIANY SIECI AS ({len(c['as_changes'])})")
            hr("·")
            for x in c["as_changes"]:
                print(f"  {x['nickname']:<22}  {x['from_as']} → {x['to_as']}")

        if c["country_changes"]:
            print(f"\n  🗺  ZMIANY KRAJU ({len(c['country_changes'])})")
            hr("·")
            for x in c["country_changes"]:
                print(f"  {x['nickname']:<22}  {x['from']} → {x['to']}")


# ══════════════════════════════════════════════════════════════════
# Eksport CSV
# ══════════════════════════════════════════════════════════════════

def export_csv(all_changes: list[dict], path: str) -> None:
    rows = []
    for c in all_changes:
        base = {"from_date": c["from_date"], "to_date": c["to_date"]}
        for r in c["new"]:
            rows.append({**base, "change_type": "new", "nickname": r.get("nickname"), "fingerprint": r.get("fingerprint"),
                         "country": r.get("country"), "bw_MB": round(r.get("advertised_bandwidth",0)/1e6,2), "detail": "|".join(r.get("flags",[]))})
        for r in c["gone"]:
            rows.append({**base, "change_type": "gone", "nickname": r.get("nickname"), "fingerprint": r.get("fingerprint"),
                         "country": r.get("country"), "bw_MB": round(r.get("advertised_bandwidth",0)/1e6,2), "detail": "|".join(r.get("flags",[]))})
        for x in c["flag_changes"]:
            rows.append({**base, "change_type": "flags", "nickname": x["nickname"], "fingerprint": x["fingerprint"],
                         "detail": f"+{','.join(x['added'])} -{','.join(x['removed'])}"})
        for x in c["version_changes"]:
            rows.append({**base, "change_type": "version", "nickname": x["nickname"], "fingerprint": x["fingerprint"],
                         "detail": f"{x['from']} → {x['to']}"})
        for x in c["bw_changes"]:
            rows.append({**base, "change_type": "bandwidth", "nickname": x["nickname"], "fingerprint": x["fingerprint"],
                         "detail": f"{x['from_MB']} → {x['to_MB']} MB/s ({x['delta_pct']:+.1f}%)"})
        for x in c["as_changes"]:
            rows.append({**base, "change_type": "as", "nickname": x["nickname"], "fingerprint": x["fingerprint"],
                         "detail": f"{x['from_as']} → {x['to_as']}"})

    fields = ["from_date","to_date","change_type","nickname","fingerprint","country","bw_MB","detail"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"✅  CSV zapisany: {path}  ({len(rows)} wierszy)")


# ══════════════════════════════════════════════════════════════════
# Eksport JSON
# ══════════════════════════════════════════════════════════════════

def export_json(all_changes: list[dict], trend: list[dict], path: str) -> None:
    payload = {"trend": trend, "changes": all_changes}
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2, default=str)
    print(f"✅  JSON zapisany: {path}")



# ══════════════════════════════════════════════════════════════════
# Eksport raportu HTML (folder z plikami danych)
# ══════════════════════════════════════════════════════════════════

CHUNK_SIZE = 2000  # wierszy na plik JSON

def _relay_mini(r: dict, period: str) -> dict:
    """Minimalna reprezentacja zmiany węzła – tylko pola wyświetlane."""
    return {
        "t": "new", "n": r.get("nickname","?"), "fp": r.get("fingerprint","")[:16],
        "c": r.get("country",""), "cn": r.get("country_name",""),
        "asn": f"{r.get('as','')} {r.get('as_name','')}".strip(),
        "bw": round(r.get("advertised_bandwidth",0)/1e6, 2),
        "f": " ".join(r.get("flags",[])), "per": period,
    }


def build_flat_rows(all_changes: list[dict]) -> list[dict]:
    """Spłaszcza wszystkie zmiany do listy kompaktowych wierszy."""
    rows = []
    for c in all_changes:
        per = c["to_date"][:10]
        for r in c.get("new", []):
            rows.append({**_relay_mini(r, per), "t": "new"})
        for r in c.get("gone", []):
            rows.append({**_relay_mini(r, per), "t": "gone"})
        for x in c.get("flag_changes", []):
            rows.append({"t":"flags","n":x["nickname"],"fp":x["fingerprint"][:16],
                         "add":",".join(x["added"]),"rm":",".join(x["removed"]),"per":per})
        for x in c.get("version_changes", []):
            rows.append({"t":"version","n":x["nickname"],"fp":x["fingerprint"][:16],
                         "fr":x.get("from","?"),"to":x.get("to","?"),"st":x.get("status",""),"per":per})
        for x in c.get("bw_changes", []):
            rows.append({"t":"bw","n":x["nickname"],"fp":x["fingerprint"][:16],
                         "fr":x["from_MB"],"to":x["to_MB"],"dp":x["delta_pct"],"per":per})
        for x in c.get("as_changes", []):
            rows.append({"t":"as","n":x["nickname"],"fp":x["fingerprint"][:16],
                         "fr":x["from_as"],"to":x["to_as"],"per":per})
        for x in c.get("country_changes", []):
            rows.append({"t":"country","n":x["nickname"],"fp":x["fingerprint"][:16],
                         "fr":x.get("from","?"),"to":x.get("to","?"),"per":per})
    return rows


def export_report(all_changes: list[dict], trend: list[dict], report_dir: str) -> None:
    """Generuje folder raportu z lekkimi plikami danych i index.html."""
    import shutil
    rp = Path(report_dir)
    dp = rp / "data"
    dp.mkdir(parents=True, exist_ok=True)

    # ── Trend (mały) ──────────────────────────────────────────────
    trend_compact = []
    for t in trend:
        tc = {k: t[k] for k in ("date","total","exit","guard","total_bw_MB") if k in t}
        tc["top_countries"] = t.get("top_countries", [])[:10]
        tc["top_as"]        = t.get("top_as", [])[:10]
        trend_compact.append(tc)

    with open(dp / "trend.json", "w", encoding="utf-8") as f:
        json.dump(trend_compact, f, ensure_ascii=False, separators=(",",":"))

    # ── Sumy per snapshot (dla osi czasu) ────────────────────────
    period_stats = []
    for c in all_changes:
        period_stats.append({
            "per":  c["to_date"][:10],
            "new":  len(c.get("new",[])),
            "gone": len(c.get("gone",[])),
            "fl":   len(c.get("flag_changes",[])),
            "bw":   len(c.get("bw_changes",[])),
            "ver":  len(c.get("version_changes",[])),
            "as":   len(c.get("as_changes",[])),
        })

    # ── Płaskie wiersze → chunki ──────────────────────────────────
    all_rows  = build_flat_rows(all_changes)
    n_chunks  = max(1, (len(all_rows) + CHUNK_SIZE - 1) // CHUNK_SIZE)
    all_periods = sorted({r["per"] for r in all_rows})

    print(f"  Wierszy zmian łącznie: {len(all_rows):,}  →  {n_chunks} chunków po {CHUNK_SIZE}")

    for i in range(n_chunks):
        chunk = all_rows[i*CHUNK_SIZE:(i+1)*CHUNK_SIZE]
        with open(dp / f"ch{i:04d}.json", "w", encoding="utf-8") as f:
            json.dump(chunk, f, ensure_ascii=False, separators=(",",":"))

    # ── Manifest index.json ───────────────────────────────────────
    manifest = {
        "total_rows": len(all_rows),
        "chunks":     n_chunks,
        "chunk_size": CHUNK_SIZE,
        "periods":    all_periods,
        "period_stats": period_stats,
        "generated":  datetime.now().isoformat(timespec="seconds"),
    }
    with open(dp / "index.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, separators=(",",":"))

    # ── index.html ────────────────────────────────────────────────
    html = _build_html()
    with open(rp / "index.html", "w", encoding="utf-8") as f:
        f.write(html)

    size_mb = sum(fp.stat().st_size for fp in dp.glob("*")) / 1e6
    print(f"✅  Raport: {report_dir}/  ({size_mb:.1f} MB danych)")
    print(f"\n  Uruchom serwer:\n    cd \"{report_dir}\" && python -m http.server 8123")
    print(f"  Otwórz:  http://localhost:8123\n")


def _build_html() -> str:
    return """<!DOCTYPE html>
<html lang="pl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Tor Relay – Raport historyczny</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
:root{
  --bg:#0a0d12;--surface:#111620;--surface2:#181f2e;--border:#1e2a3a;
  --text:#cdd9e5;--muted:#7a8ba0;--dim:#3d5068;
  --green:#3fb950;--green-bg:#0d2218;
  --red:#f85149;  --red-bg:#2b1010;
  --blue:#58a6ff; --blue-bg:#0d1f36;
  --yellow:#e3b341;--yellow-bg:#2b2005;
  --purple:#bc8cff;--purple-bg:#1e1030;
  --cyan:#39c5cf; --cyan-bg:#051e20;
  --radius:10px;--radius-sm:6px;
  --font:'Segoe UI',system-ui,sans-serif;
  --mono:'Cascadia Code','Fira Code','Consolas',monospace;
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--font);font-size:14px;line-height:1.6;min-height:100vh}
.layout{display:flex;min-height:100vh}
/* Sidebar */
.sidebar{width:220px;flex-shrink:0;background:var(--surface);border-right:1px solid var(--border);
  display:flex;flex-direction:column;position:sticky;top:0;height:100vh;overflow-y:auto}
.sidebar-logo{padding:20px 16px 12px;border-bottom:1px solid var(--border)}
.sidebar-logo .lg-icon{font-size:22px;margin-bottom:4px}
.sidebar-logo h1{font-size:13px;font-weight:700}
.sidebar-logo p{font-size:11px;color:var(--muted);margin-top:2px}
.sidebar-nav{padding:12px 8px;flex:1}
.nav-label{font-size:10px;font-weight:700;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;padding:8px 8px 4px}
.nav-btn{display:flex;align-items:center;gap:8px;width:100%;padding:7px 10px;border:none;
  background:none;color:var(--muted);font-size:13px;font-family:var(--font);border-radius:var(--radius-sm);
  cursor:pointer;text-align:left;transition:.15s}
.nav-btn:hover{background:var(--surface2);color:var(--text)}
.nav-btn.active{background:var(--blue-bg);color:var(--blue)}
.snap-list{padding:12px 8px;border-top:1px solid var(--border)}
.snap-label{font-size:10px;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;padding:4px 8px 6px}
.snap-item{display:flex;justify-content:space-between;align-items:center;padding:5px 10px;border-radius:var(--radius-sm)}
.snap-date{font-size:11px;color:var(--text)}
.snap-cnt{font-size:10px;color:var(--muted);background:var(--border);padding:1px 6px;border-radius:10px}
/* Main */
.main{flex:1;min-width:0}
.topbar{position:sticky;top:0;z-index:100;background:rgba(10,13,18,.93);backdrop-filter:blur(12px);
  border-bottom:1px solid var(--border);padding:10px 24px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.topbar-title{font-size:13px;font-weight:600;color:var(--muted);flex:1}
.search-wrap{position:relative;flex:1;max-width:280px}
.search-wrap input{width:100%;background:var(--surface);border:1px solid var(--border);color:var(--text);
  padding:7px 10px 7px 32px;border-radius:var(--radius-sm);font-size:13px;font-family:var(--font);outline:none;transition:.15s}
.search-wrap input:focus{border-color:var(--blue);box-shadow:0 0 0 3px rgba(88,166,255,.12)}
.search-ico{position:absolute;left:10px;top:50%;transform:translateY(-50%);color:var(--muted)}
.row-count{font-size:12px;color:var(--muted);white-space:nowrap}
/* Loading bar */
#loadBar{position:fixed;top:0;left:0;height:3px;background:var(--blue);width:0;transition:width .3s;z-index:9999}
/* Pages */
.page{display:none;padding:24px}
.page.active{display:block}
/* Stats */
.stat-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:14px;margin-bottom:24px}
.stat-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);
  padding:18px 16px;position:relative;overflow:hidden}
.stat-card::before{content:'';position:absolute;inset:0;opacity:.06;pointer-events:none}
.stat-card.blue::before{background:var(--blue)}.stat-card.blue .sv{color:var(--blue)}
.stat-card.green::before{background:var(--green)}.stat-card.green .sv{color:var(--green)}
.stat-card.red::before{background:var(--red)}.stat-card.red .sv{color:var(--red)}
.stat-card.yellow::before{background:var(--yellow)}.stat-card.yellow .sv{color:var(--yellow)}
.stat-card.purple::before{background:var(--purple)}.stat-card.purple .sv{color:var(--purple)}
.stat-card.cyan::before{background:var(--cyan)}.stat-card.cyan .sv{color:var(--cyan)}
.s-icon{font-size:18px;margin-bottom:8px}
.sv{font-size:26px;font-weight:800;line-height:1;margin-bottom:4px}
.sn{font-size:11px;color:var(--muted);font-weight:500;text-transform:uppercase;letter-spacing:.06em}
.ss{font-size:11px;color:var(--dim);margin-top:4px}
/* Charts */
.chart-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(360px,1fr));gap:16px;margin-bottom:24px}
.chart-card{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);padding:20px}
.chart-card h3{font-size:11px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:16px}
/* Pills */
.type-filters{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:16px}
.pill{padding:5px 12px;border-radius:20px;border:1px solid var(--border);font-size:12px;font-weight:600;
  cursor:pointer;transition:.15s;background:var(--surface);color:var(--muted);user-select:none}
.pill:hover{border-color:var(--blue);color:var(--text)}
.pill.on{color:#fff;border-color:transparent}
.pill[data-t=new].on{background:var(--green);box-shadow:0 0 12px rgba(63,185,80,.3)}
.pill[data-t=gone].on{background:var(--red);box-shadow:0 0 12px rgba(248,81,73,.3)}
.pill[data-t=flags].on{background:var(--purple);box-shadow:0 0 12px rgba(188,140,255,.3)}
.pill[data-t=version].on{background:var(--blue);box-shadow:0 0 12px rgba(88,166,255,.3)}
.pill[data-t=bw].on{background:var(--yellow);color:#000;box-shadow:0 0 12px rgba(227,179,65,.3)}
.pill[data-t=as].on{background:var(--cyan);color:#000}
.pill[data-t=country].on{background:#8957e5}
/* Filter row */
.frow{display:flex;gap:10px;align-items:center;margin-bottom:16px;flex-wrap:wrap}
.frow select{background:var(--surface);border:1px solid var(--border);color:var(--text);
  padding:6px 10px;border-radius:var(--radius-sm);font-size:13px;font-family:var(--font);outline:none}
.frow select:focus{border-color:var(--blue)}
.btn-clear{padding:5px 12px;border:1px solid var(--border);border-radius:var(--radius-sm);
  background:none;color:var(--muted);font-size:12px;cursor:pointer;transition:.12s}
.btn-clear:hover{border-color:var(--red);color:var(--red)}
/* Progress */
.progress-wrap{display:flex;align-items:center;gap:10px;padding:8px 0;font-size:12px;color:var(--muted)}
.progress-bar{flex:1;height:4px;background:var(--border);border-radius:2px;overflow:hidden}
.progress-fill{height:100%;background:var(--blue);transition:width .2s}
/* Virtual table */
.tbl-wrap{background:var(--surface);border:1px solid var(--border);border-radius:var(--radius);overflow:hidden}
.tbl-head{display:grid;grid-template-columns:80px 90px 160px 1fr;padding:10px 12px;
  border-bottom:1px solid var(--border);font-size:11px;font-weight:700;color:var(--dim);
  text-transform:uppercase;letter-spacing:.07em;background:var(--surface);z-index:10}
.tbl-head span{cursor:pointer;user-select:none}
.tbl-head span:hover{color:var(--text)}
.vp{height:560px;overflow-y:auto;scroll-behavior:smooth}
.vp::-webkit-scrollbar{width:6px}
.vp::-webkit-scrollbar-thumb{background:var(--border);border-radius:3px}
.tbl-row{display:grid;grid-template-columns:80px 90px 160px 1fr;padding:8px 12px;
  border-bottom:1px solid rgba(30,42,58,.7);transition:.1s;align-items:start}
.tbl-row:hover{background:var(--surface2)}
.c-per{font-size:11px;color:var(--muted);font-family:var(--mono)}
.c-nick{font-weight:600;font-size:13px}
.c-fp{font-size:10px;color:var(--dim);font-family:var(--mono)}
.c-det{font-size:12px;color:var(--muted);word-break:break-word;line-height:1.5}
.c-det b{font-weight:600}
.tag{display:inline-block;padding:0 5px;border-radius:3px;font-size:10px;font-weight:700;background:var(--border);color:var(--muted);margin:1px}
/* Badges */
.badge{display:inline-flex;align-items:center;gap:3px;padding:3px 8px;border-radius:20px;font-size:11px;font-weight:700;white-space:nowrap}
.b-new{background:var(--green-bg);color:var(--green);border:1px solid rgba(63,185,80,.25)}
.b-gone{background:var(--red-bg);color:var(--red);border:1px solid rgba(248,81,73,.25)}
.b-flags{background:var(--purple-bg);color:var(--purple);border:1px solid rgba(188,140,255,.25)}
.b-version{background:var(--blue-bg);color:var(--blue);border:1px solid rgba(88,166,255,.25)}
.b-bw{background:var(--yellow-bg);color:var(--yellow);border:1px solid rgba(227,179,65,.25)}
.b-as{background:var(--cyan-bg);color:var(--cyan);border:1px solid rgba(57,197,207,.25)}
.b-country{background:#1e0f36;color:#8957e5;border:1px solid rgba(137,87,229,.25)}
/* Timeline */
.tl{display:flex;flex-direction:column;gap:2px}
.tl-hdr{display:grid;grid-template-columns:22px 110px 70px 60px 60px 1fr;gap:8px;
  font-size:10px;font-weight:700;color:var(--dim);text-transform:uppercase;letter-spacing:.07em;
  padding:6px 12px;background:var(--surface);border-bottom:1px solid var(--border)}
.tl-row{display:grid;grid-template-columns:22px 110px 70px 60px 60px 1fr;gap:8px;
  align-items:center;padding:8px 12px;border-radius:var(--radius-sm);font-size:12px;transition:.12s}
.tl-row:hover{background:var(--surface2)}
.dot{width:8px;height:8px;border-radius:50%;background:var(--border)}
.dot.has{background:var(--green)}
.up{color:var(--green)}.dn{color:var(--red)}.nc{color:var(--muted)}
/* No data */
.no-data{display:flex;flex-direction:column;align-items:center;justify-content:center;
  height:180px;color:var(--dim);gap:8px}
.no-data .nd-ico{font-size:36px;opacity:.35}
@media(max-width:860px){
  .sidebar{display:none}
  .tbl-head,.tbl-row{grid-template-columns:70px 80px 120px 1fr;font-size:12px}
}
</style>
</head>
<body>
<div id="loadBar"></div>
<div class="layout">

<!-- SIDEBAR -->
<nav class="sidebar">
  <div class="sidebar-logo">
    <div class="lg-icon">📡</div>
    <h1>Tor Relay</h1>
    <p id="genDate">Raport historyczny</p>
  </div>
  <div class="sidebar-nav">
    <div class="nav-label">Widoki</div>
    <button class="nav-btn active" onclick="showPage('overview')" id="nb-overview"><span>📊</span> Przegląd</button>
    <button class="nav-btn" onclick="showPage('changes')"  id="nb-changes"><span>🔄</span> Dziennik zmian</button>
    <button class="nav-btn" onclick="showPage('timeline')" id="nb-timeline"><span>📅</span> Oś czasu</button>
  </div>
  <div class="snap-list">
    <div class="snap-label">Snapshoty</div>
    <div id="snapList"></div>
  </div>
</nav>

<!-- MAIN -->
<div class="main">
  <div class="topbar">
    <span class="topbar-title" id="pageTitle">Przegląd</span>
    <div class="search-wrap">
      <span class="search-ico">🔍</span>
      <input id="fNick" placeholder="Szukaj nickname / fingerprint…" oninput="onSearch()">
    </div>
    <span class="row-count" id="rowCount"></span>
  </div>

  <!-- Przegląd -->
  <div class="page active" id="page-overview">
    <div class="stat-grid" id="statGrid"><div style="color:var(--muted);padding:20px">Ładowanie…</div></div>
    <div class="chart-grid">
      <div class="chart-card"><h3>Liczba węzłów w czasie</h3><canvas id="cNodes"></canvas></div>
      <div class="chart-card"><h3>Przepustowość sieci [MB/s]</h3><canvas id="cBW"></canvas></div>
      <div class="chart-card"><h3>Aktywność zmian per snapshot</h3><canvas id="cActivity"></canvas></div>
      <div class="chart-card"><h3>Top 10 krajów (ostatni snapshot)</h3><canvas id="cCountry"></canvas></div>
      <div class="chart-card"><h3>Top 10 sieci AS (ostatni snapshot)</h3><canvas id="cAS"></canvas></div>
    </div>
  </div>

  <!-- Dziennik zmian -->
  <div class="page" id="page-changes">
    <div class="type-filters" id="pills"></div>
    <div class="frow">
      <select id="fPeriod" onchange="applyFilters()"><option value="">Wszystkie okresy</option></select>
      <button class="btn-clear" onclick="clearFilters()">✕ Wyczyść</button>
    </div>
    <div class="progress-wrap" id="loadProgress" style="display:none">
      <span id="loadTxt">Ładowanie danych…</span>
      <div class="progress-bar"><div class="progress-fill" id="loadFill"></div></div>
      <span id="loadPct">0%</span>
    </div>
    <div class="tbl-wrap" id="tblWrap">
      <div class="tbl-head">
        <span onclick="sortBy('per')">Data <span id="si-per"></span></span>
        <span onclick="sortBy('t')">Typ <span id="si-t"></span></span>
        <span onclick="sortBy('n')">Nickname <span id="si-n"></span></span>
        <span>Szczegóły</span>
      </div>
      <div class="vp" id="vp" onscroll="onScroll()">
        <div id="stTop"></div>
        <div id="rowCont"></div>
        <div id="stBot"></div>
      </div>
    </div>
    <div class="no-data" id="noData" style="display:none">
      <div class="nd-ico">🔎</div><div>Brak wyników</div>
    </div>
  </div>

  <!-- Oś czasu -->
  <div class="page" id="page-timeline">
    <div class="tbl-wrap">
      <div class="tl" id="tl">
        <div class="tl-hdr">
          <span></span><span>Data</span><span>Węzłów</span>
          <span class="up">Nowe</span><span class="dn">Znik.</span><span>Zmiany</span>
        </div>
      </div>
    </div>
  </div>
</div><!-- /.main -->
</div><!-- /.layout -->

<script>
/* ═══ STAN GLOBALNY ═══════════════════════════════════════════════ */
let MANIFEST = null, TREND = [], ALL_ROWS = [];
let loadedChunks = 0;
let activeTypes = new Set(['new','gone','flags','version','bw','as','country']);
let activePeriod = '', searchQ = '', sortCol = 'per', sortDir = 1;
let filteredRows = [], visRows = [];
const ROW_H = 48, OVERSCAN = 8;
let chartsBuilt = false;

const PAGE_TITLES = {overview:'Przegląd', changes:'Dziennik zmian', timeline:'Oś czasu'};
const ALL_TYPES   = ['new','gone','flags','version','bw','as','country'];
const TYPE_LABELS = {new:'🟢 Nowe',gone:'🔴 Zniknięte',flags:'🏳 Flagi',version:'🔄 Wersje',
                     bw:'📶 Przepust.',as:'🌐 AS',country:'🗺 Kraj'};
const BADGE_CLS   = {new:'b-new',gone:'b-gone',flags:'b-flags',version:'b-version',
                     bw:'b-bw',as:'b-as',country:'b-country'};

/* ═══ ŁADOWANIE DANYCH ════════════════════════════════════════════ */
async function fetchJSON(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
  return r.json();
}

function setBar(pct) {
  document.getElementById('loadBar').style.width = pct + '%';
}

async function loadAll() {
  setBar(10);
  MANIFEST = await fetchJSON('data/index.json');
  TREND    = await fetchJSON('data/trend.json');
  setBar(30);

  document.getElementById('genDate').textContent = 'Wygenerowano: ' + (MANIFEST.generated||'').slice(0,10);

  buildOverview();
  buildTimeline();
  buildSnapList();

  // Ładuj chunki sekwencyjnie w tle
  showLoadProgress(true);
  for (let i = 0; i < MANIFEST.chunks; i++) {
    const chunk = await fetchJSON(`data/ch${String(i).padStart(4,'0')}.json`);
    ALL_ROWS.push(...chunk);
    loadedChunks = i + 1;
    const pct = Math.round(loadedChunks / MANIFEST.chunks * 100);
    document.getElementById('loadFill').style.width = pct + '%';
    document.getElementById('loadPct').textContent  = pct + '%';
    document.getElementById('loadTxt').textContent  = `Ładowanie ${loadedChunks}/${MANIFEST.chunks} chunków…`;
    setBar(30 + pct * 0.7);
    applyFilters();   // odświeżaj tabelę na bieżąco
    await new Promise(r => setTimeout(r, 0));  // nie blokuj UI
  }
  showLoadProgress(false);
  setBar(100);
  setTimeout(() => setBar(0), 600);
}

function showLoadProgress(on) {
  document.getElementById('loadProgress').style.display = on ? 'flex' : 'none';
}

/* ═══ NAWIGACJA ═══════════════════════════════════════════════════ */
function showPage(id) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
  document.getElementById('page-' + id).classList.add('active');
  document.getElementById('nb-' + id).classList.add('active');
  document.getElementById('pageTitle').textContent = PAGE_TITLES[id] || id;
  if (id === 'changes') applyFilters();
}

/* ═══ FILTRY ══════════════════════════════════════════════════════ */
// Buduj pills
const pillsEl = document.getElementById('pills');
ALL_TYPES.forEach(t => {
  const p = document.createElement('span');
  p.className = 'pill on'; p.dataset.t = t;
  p.textContent = TYPE_LABELS[t];
  p.onclick = () => { activeTypes.has(t) ? activeTypes.delete(t) : activeTypes.add(t);
    p.classList.toggle('on', activeTypes.has(t)); applyFilters(); };
  pillsEl.appendChild(p);
});

// Buduj select okresu po załadowaniu manifestu
function buildPeriodSelect() {
  const sel = document.getElementById('fPeriod');
  (MANIFEST.periods || []).forEach(p => {
    const o = document.createElement('option'); o.value = p; o.textContent = p; sel.appendChild(o);
  });
  sel.onchange = () => { activePeriod = sel.value; applyFilters(); };
}

function clearFilters() {
  activeTypes = new Set(ALL_TYPES);
  document.querySelectorAll('.pill').forEach(p => p.classList.add('on'));
  activePeriod = ''; document.getElementById('fPeriod').value = '';
  searchQ = ''; document.getElementById('fNick').value = '';
  applyFilters();
}

let searchTimer;
function onSearch() {
  clearTimeout(searchTimer);
  searchTimer = setTimeout(() => { searchQ = document.getElementById('fNick').value.toLowerCase(); applyFilters(); }, 150);
}

function applyFilters() {
  filteredRows = sortedRows(ALL_ROWS.filter(r =>
    activeTypes.has(r.t) &&
    (!activePeriod || r.per === activePeriod) &&
    (!searchQ || (r.n||'').toLowerCase().includes(searchQ) || (r.fp||'').toLowerCase().includes(searchQ))
  ));
  visRows = filteredRows;
  const cnt = filteredRows.length;
  document.getElementById('rowCount').textContent = cnt.toLocaleString() + ' wierszy';
  document.getElementById('noData').style.display = cnt ? 'none' : '';
  document.getElementById('tblWrap').style.display = cnt ? '' : 'none';
  paintVirtual(true);
}

/* ═══ SORTOWANIE ══════════════════════════════════════════════════ */
function sortBy(col) {
  sortDir = sortCol === col ? -sortDir : 1; sortCol = col;
  document.querySelectorAll('[id^="si-"]').forEach(e => e.textContent = '');
  const el = document.getElementById('si-' + col);
  if (el) el.textContent = sortDir > 0 ? ' ↑' : ' ↓';
  applyFilters();
}
function sortedRows(rows) {
  return [...rows].sort((a, b) => {
    const va = a[sortCol]||'', vb = b[sortCol]||'';
    return va < vb ? -sortDir : va > vb ? sortDir : 0;
  });
}

/* ═══ VIRTUAL SCROLL ══════════════════════════════════════════════ */
function paintVirtual(resetScroll) {
  const vp = document.getElementById('vp');
  if (!vp) return;
  if (resetScroll) vp.scrollTop = 0;
  const total = visRows.length;
  const vpH   = vp.clientHeight || 560;
  const scroll = vp.scrollTop;
  const s = Math.max(0, Math.floor(scroll / ROW_H) - OVERSCAN);
  const e = Math.min(total, Math.ceil((scroll + vpH) / ROW_H) + OVERSCAN);
  document.getElementById('stTop').style.height = (s * ROW_H) + 'px';
  document.getElementById('stBot').style.height = ((total - e) * ROW_H) + 'px';
  document.getElementById('rowCont').innerHTML = visRows.slice(s, e).map(rowHTML).join('');
}
function onScroll() { requestAnimationFrame(() => paintVirtual(false)); }

function rowHTML(r) {
  let det = '';
  if (r.t === 'new' || r.t === 'gone') {
    det = `${r.cn||r.c||''} · <b>${r.asn||''}</b> · ${r.bw||0} MB/s · <span style="color:var(--dim)">${r.f||''}</span>`;
  } else if (r.t === 'flags') {
    const add = (r.add||'').split(',').filter(Boolean).map(f=>`<span class="tag" style="color:var(--green)">+${f}</span>`).join('');
    const rm  = (r.rm||'').split(',').filter(Boolean).map(f=>`<span class="tag" style="color:var(--red)">−${f}</span>`).join('');
    det = add + rm;
  } else if (r.t === 'version') {
    det = `${r.fr||'?'} → ${r.to||'?'} <span class="tag">${r.st||''}</span>`;
  } else if (r.t === 'bw') {
    const up = r.dp > 0;
    det = `${r.fr} → ${r.to} MB/s <b style="color:${up?'var(--green)':'var(--red)'}">${up?'▲':'▼'}${Math.abs(r.dp)}%</b>`;
  } else if (r.t === 'as' || r.t === 'country') {
    det = `${r.fr||'?'} → ${r.to||'?'}`;
  }
  return `<div class="tbl-row">
    <div class="c-per">${r.per||''}</div>
    <div><span class="badge ${BADGE_CLS[r.t]||''}">${TYPE_LABELS[r.t]||r.t}</span></div>
    <div><div class="c-nick">${esc(r.n||'?')}</div><div class="c-fp">${r.fp||''}</div></div>
    <div class="c-det">${det}</div>
  </div>`;
}
function esc(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

/* ═══ STATYSTYKI ══════════════════════════════════════════════════ */
function buildOverview() {
  const last  = TREND[TREND.length-1] || {};
  const first = TREND[0] || {};
  const ps    = MANIFEST.period_stats || [];
  const totalNew  = ps.reduce((s,p)=>s+p.new,0);
  const totalGone = ps.reduce((s,p)=>s+p.gone,0);
  const totalFlag = ps.reduce((s,p)=>s+p.fl,0);
  const dTotal = (last.total||0) - (first.total||0);
  const dBW    = ((last.total_bw_MB||0) - (first.total_bw_MB||0)).toFixed(1);

  function dlt(d) {
    if (!d || d==='0.0') return `<span style="color:var(--dim)">bez zmian · </span>`;
    return d > 0
      ? `<span style="color:var(--green)">▲ +${d} · </span>`
      : `<span style="color:var(--red)">▼ ${d} · </span>`;
  }
  const cards = [
    {cl:'blue',  ico:'📦', v:TREND.length,              nm:'Snapshoty',     sb:'pliki JSON'},
    {cl:'blue',  ico:'🖥',  v:last.total||'?',           nm:'Węzłów (teraz)',sb:dlt(dTotal)+'vs. start'},
    {cl:'green', ico:'🟢', v:totalNew,                   nm:'Nowe łącznie',  sb:'węzły w okresie'},
    {cl:'red',   ico:'🔴', v:totalGone,                  nm:'Zniknięte',     sb:'łącznie w okresie'},
    {cl:'yellow',ico:'📶', v:(last.total_bw_MB||0)+' MB/s',nm:'BW (teraz)', sb:dlt(+dBW)+'vs. start'},
    {cl:'cyan',  ico:'🚪', v:last.exit||'?',             nm:'Exit (teraz)',  sb:'węzły Exit'},
    {cl:'green', ico:'🛡',  v:last.guard||'?',           nm:'Guard (teraz)', sb:'węzły Guard'},
    {cl:'purple',ico:'🏳', v:totalFlag,                  nm:'Zmiany flag',   sb:'łącznie w okresie'},
  ];
  document.getElementById('statGrid').innerHTML = cards.map(c =>
    `<div class="stat-card ${c.cl}"><div class="s-icon">${c.ico}</div>
     <div class="sv">${c.v}</div><div class="sn">${c.nm}</div><div class="ss">${c.sb}</div></div>`
  ).join('');

  buildPeriodSelect();
  buildCharts(ps);
}

/* ═══ WYKRESY ═════════════════════════════════════════════════════ */
function buildCharts(ps) {
  if (chartsBuilt) return; chartsBuilt = true;
  const CLRS = {blue:'#58a6ff',green:'#3fb950',red:'#f85149',yellow:'#e3b341',purple:'#bc8cff',cyan:'#39c5cf'};
  const labels = TREND.map(t => t.date.slice(0,10));
  const base = {
    responsive:true, maintainAspectRatio:true,
    plugins:{legend:{labels:{color:'#7a8ba0',boxWidth:12,font:{size:11}}},
      tooltip:{backgroundColor:'#181f2e',borderColor:'#1e2a3a',borderWidth:1,titleColor:'#cdd9e5',bodyColor:'#7a8ba0'}},
    scales:{
      x:{ticks:{color:'#3d5068',font:{size:10},maxRotation:45},grid:{color:'rgba(30,42,58,.6)'}},
      y:{ticks:{color:'#3d5068',font:{size:11}},grid:{color:'rgba(30,42,58,.6)'}}
    }
  };

  new Chart('cNodes',{type:'line',data:{labels,datasets:[
    {label:'Łącznie',data:TREND.map(t=>t.total), borderColor:CLRS.blue,  backgroundColor:'rgba(88,166,255,.07)',tension:.35,fill:true,pointRadius:2},
    {label:'Exit',   data:TREND.map(t=>t.exit),  borderColor:CLRS.red,   backgroundColor:'rgba(248,81,73,.06)',tension:.35,fill:true,pointRadius:2},
    {label:'Guard',  data:TREND.map(t=>t.guard), borderColor:CLRS.green, backgroundColor:'rgba(63,185,80,.06)',tension:.35,fill:true,pointRadius:2},
  ]},options:base});

  new Chart('cBW',{type:'line',data:{labels,datasets:[
    {label:'MB/s',data:TREND.map(t=>t.total_bw_MB),borderColor:CLRS.purple,backgroundColor:'rgba(188,140,255,.07)',tension:.35,fill:true,pointRadius:2},
  ]},options:base});

  const pLabels = ps.map(p=>p.per);
  new Chart('cActivity',{type:'bar',data:{labels:pLabels,datasets:[
    {label:'Nowe',     data:ps.map(p=>p.new),  backgroundColor:'rgba(63,185,80,.75)'},
    {label:'Zniknęły', data:ps.map(p=>p.gone), backgroundColor:'rgba(248,81,73,.75)'},
    {label:'Flagi',    data:ps.map(p=>p.fl),   backgroundColor:'rgba(188,140,255,.75)'},
    {label:'BW',       data:ps.map(p=>p.bw),   backgroundColor:'rgba(227,179,65,.75)'},
    {label:'AS',       data:ps.map(p=>p.as),   backgroundColor:'rgba(57,197,207,.75)'},
  ]},options:{...base,scales:{
    x:{...base.scales.x,stacked:true},
    y:{...base.scales.y,stacked:true}
  }}});

  // Kraje
  const lc = (TREND[TREND.length-1]||{}).top_countries||[];
  if (lc.length) {
    const pal=['#58a6ff','#3fb950','#f85149','#e3b341','#bc8cff','#39c5cf','#ff8c42','#a8dadc','#d62828','#f7b2ad'];
    new Chart('cCountry',{type:'doughnut',data:{
      labels:lc.map(c=>c[0]),
      datasets:[{data:lc.map(c=>c[1]),backgroundColor:pal,borderColor:'#111620',borderWidth:2}]
    },options:{responsive:true,maintainAspectRatio:true,
      plugins:{legend:{position:'right',labels:{color:'#7a8ba0',boxWidth:10,font:{size:11}}},
               tooltip:{backgroundColor:'#181f2e',borderColor:'#1e2a3a',borderWidth:1,titleColor:'#cdd9e5',bodyColor:'#7a8ba0'}}}});
  }

  // AS
  const la = (TREND[TREND.length-1]||{}).top_as||[];
  if (la.length) {
    new Chart('cAS',{type:'bar',data:{
      labels:la.map(a=>a[0].trim().slice(0,30)),
      datasets:[{label:'Węzłów',data:la.map(a=>a[1]),
        backgroundColor:'rgba(57,197,207,.75)',borderColor:'#39c5cf',borderWidth:1,borderRadius:4}]
    },options:{...base,indexAxis:'y',plugins:{...base.plugins,legend:{display:false}}}});
  }
}

/* ═══ OŚ CZASU ════════════════════════════════════════════════════ */
function buildTimeline() {
  const ps = MANIFEST.period_stats || [];
  const tl = document.getElementById('tl');
  ps.forEach((p, i) => {
    const t   = TREND[i+1] || TREND[i] || {};
    const prev = TREND[i] || {};
    const dT  = t.total - prev.total;
    const div = document.createElement('div');
    div.className = 'tl-row';
    div.innerHTML = `
      <div><div class="dot ${p.new?'has':''}"></div></div>
      <div>${p.per}</div>
      <div class="${dT>0?'up':dT<0?'dn':'nc'}">${t.total||'?'} ${dT?`(${dT>0?'+':''}${dT})`:''}</div>
      <div class="up">${p.new||''}</div>
      <div class="dn">${p.gone||''}</div>
      <div style="font-size:11px">${[
        p.new  ?`<span class="badge b-new">+${p.new} nowych</span>` :'',
        p.gone ?`<span class="badge b-gone">−${p.gone} znikło</span>`:'',
        p.fl   ?`<span class="badge b-flags">${p.fl} flag</span>`:'',
        p.bw   ?`<span class="badge b-bw">${p.bw} BW</span>`:'',
        p.as   ?`<span class="badge b-as">${p.as} AS</span>`:'',
      ].filter(Boolean).join(' ')}</div>
    `;
    tl.appendChild(div);
  });
}

/* ═══ SNAPSHOTY W SIDEBARZE ═══════════════════════════════════════ */
function buildSnapList() {
  const el = document.getElementById('snapList');
  [...TREND].reverse().forEach(t => {
    const d = document.createElement('div');
    d.className = 'snap-item';
    d.innerHTML = `<span class="snap-date">${t.date.slice(0,16)}</span><span class="snap-cnt">${t.total}</span>`;
    el.appendChild(d);
  });
}

/* ═══ START ═══════════════════════════════════════════════════════ */
loadAll().catch(e => {
  document.getElementById('statGrid').innerHTML =
    `<div style="color:var(--red);padding:20px">❌ Błąd ładowania danych: ${e.message}<br><br>
     Uruchom serwer HTTP: <code>python -m http.server 8123</code></div>`;
});
</script>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Historyczny analizator węzłów Tor (Onionoo JSON)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Przykłady:
  python tor_history_analyzer.py ./snapshots
  python tor_history_analyzer.py ./snapshots --country de --filter new,gone
  python tor_history_analyzer.py ./snapshots --from 2025-06-01 --to 2025-12-31
  python tor_history_analyzer.py ./snapshots --report ./raport --export zmiany.csv --no-print
""")
    parser.add_argument("folder", help="Folder z plikami JSON")
    parser.add_argument("--country",  metavar="KOD",   help="Filtruj wg kodu kraju, np. de, us, pl")
    parser.add_argument("--filter",   metavar="TYPY",  help="Typy zmian (po polsku lub angielsku): nowe/new, zamknięte/gone, flagi/flags, wersje/version, przepustowość/bw, as, kraj/country")
    parser.add_argument("--from",     dest="date_from", metavar="YYYY-MM-DD", help="Data poczatkowa")
    parser.add_argument("--to",       dest="date_to",   metavar="YYYY-MM-DD", help="Data koncowa")
    parser.add_argument("--export",   metavar="PLIK.csv",   help="Eksport zmian do CSV")
    parser.add_argument("--json",     metavar="PLIK.json",  help="Eksport podsumowania do JSON")
    parser.add_argument("--report",   metavar="FOLDER",     help="Generuj interaktywny raport HTML (folder z chunkami)")
    parser.add_argument("--html",     metavar="PLIK.html",  help="[UWAGA: wolne przy dużych danych] użyj --report zamiast tego")
    parser.add_argument("--group",     metavar="TRYB",  help="Grupuj węzły: as, rodzina (lub: family). Można łączyć: as,rodzina")
    parser.add_argument("--no-print", action="store_true",  help="Pomin wydruk terminalowy")
    args = parser.parse_args()

    if args.html:
        print()
        print("⚠️  Flaga --html wbudowuje WSZYSTKIE dane w jeden plik HTML.")
        print("   Przy dużych danych (>100 MB) przeglądarka może się nie otworzyć.")
        print()
        print("   Użyj zamiast tego:")
        print(f"     --report ./raport")
        print()
        print("   Potem uruchom serwer i otwórz w przeglądarce:")
        print("     cd raport")
        print("     python -m http.server 8123")
        print("     # otwórz: http://localhost:8123")
        print()
        ans = input("   Kontynuować mimo to z --html? [t/N]: ").strip().lower()
        if ans not in ("t", "tak", "y", "yes"):
            print("   Przerywam. Uruchom ponownie z --report ./raport")
            sys.exit(0)

    print(f"Wczytywanie snapshotow z: {args.folder}")
    snapshots = load_snapshots(args.folder)
    if len(snapshots) < 2:
        print(f"Potrzeba co najmniej 2 plikow JSON, znaleziono: {len(snapshots)}")
        sys.exit(1)

    print(f"Znaleziono {len(snapshots)} snapshotow: {snapshots[0]['published'][:10]} -> {snapshots[-1]['published'][:10]}")

    all_changes_raw = [compare(snapshots[i], snapshots[i+1]) for i in range(len(snapshots)-1)]
    trend           = build_aggregate_trend(snapshots)
    all_changes     = apply_filters(all_changes_raw, args)

    total_new   = sum(len(c["new"])             for c in all_changes)
    total_gone  = sum(len(c["gone"])            for c in all_changes)
    total_flags = sum(len(c["flag_changes"])    for c in all_changes)
    total_ver   = sum(len(c["version_changes"]) for c in all_changes)
    total_bw    = sum(len(c["bw_changes"])      for c in all_changes)
    print(f"  Nowe: {total_new}  | Znikniete: {total_gone}  | Zmiany flag: {total_flags}  | Wersje: {total_ver}  | BW>=10%: {total_bw}")

    if not args.no_print:
        print_changes(all_changes, trend)

    # Grupowania
    if args.group:
        modes = {m.strip().lower() for m in args.group.split(",")}
        if modes & {"as"}:
            group_by_as(snapshots)
        if modes & {"rodzina", "family", "fam"}:
            group_by_family(snapshots)

    if args.export:
        export_csv(all_changes, args.export)
    if args.json:
        export_json(all_changes, trend, args.json)
    if args.report:
        export_report(all_changes_raw, trend, args.report)

    print()


if __name__ == "__main__":
    main()
