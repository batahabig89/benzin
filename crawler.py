"""benzin.mn-ээс ШТС-ын системийн (oil.mrpam.gov.mn) мэдээг татаж, хоёр жагсаалт гаргана.

1. «Орлого буусан» ШТС-ууд — сүүлд орлого буусан цагаар нь (шинэ нь эхэндээ).
2. 92 бензин өгч байгаа, дуусаагүй, дундраагүй (state == "normal") ШТС-ууд —
   ШТС мэдээгээ шинэчилсэн цагаар нь (шинэ нь эхэндээ).

Мөр бүр `in_city` (CITY_BBOX) талбартай; хот/хотын гадна, Аи-92/Евро-92-оор UI шүүнэ.

    python3 crawler.py              # терминал дээр хэвлэнэ
    python3 crawler.py --out _site  # GitHub Pages-д зориулж data.json + stations/*.json бичнэ
"""

import argparse
import hashlib
import json
import os
import shutil
import threading
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

BASE = "https://benzin.mn"
UA = "benzin-crawl/1.0 (+personal use)"

# benzin.mn өөрөө feed-ээ 30 секунд cache хийдэг тул түүнээс олон асуух хэрэггүй.
FEED_TTL = 30
# Жагсаалтад хэрэгтэй хаяг, компани, камер бараг өөрчлөгддөггүй.
REGISTER_TTL = 6 * 60 * 60
# Станц дээр дарахад харуулах төлөв шинэ байх ёстой.
DETAIL_TTL = 30

# Хот доторх хүрээ (өмнөд, баруун, хойд, зүүн). benzin.mn-ийн дүүргийн нэр заримдаа
# буруу байдаг (Налайхын ШТС «Баянзүрх» гэх мэт) тул координатаар шүүнэ. Налайх,
# Багануур, Өлзийт, Гачуурт зэрэг хотын гаднах ШТС-ууд орохгүй.
CITY_BBOX = (47.82, 106.60, 48.02, 107.12)
UB_DISTRICTS = {"Баянгол", "Баянзүрх", "Сонгинохайрхан", "Сүхбаатар", "Хан-Уул", "Чингэлтэй",
                "Налайх", "Багануур", "Багахангай"}

GRADE_92 = {"ai92": "Аи-92", "euro92": "Евро-92"}
FUEL_LABELS = {
    "ai80": "Аи-80", "ai92": "Аи-92", "euro92": "Евро-92", "ai95": "Аи-95", "ai98": "Аи-98",
    "diesel": "ДТ", "dt": "ДТ", "eurodt": "Евро ДТ", "any": "Ерөнхий",
}
LEVELS = {
    "low": "Бага", "medium": "Дундарсан", "high": "Хангалттай", "normal": "Хэвийн",
    "full": "Дүүрэн", "empty": "Хоосон", "stale": "Хэмжилт хуучирсан",
}
GOV_STATES = {
    "normal": "Хэвийн түгээж байна", "medium": "Савны түвшин дундарсан", "low": "Түвшин бага",
    "stopped": "Түгээлт зогссон", "stale": "Мэдээ баталгаажаагүй",
}
SALE_WORDS = {"sell": "Зарж байна", "out": "Дууссан", "stop": "Зарахгүй", "unknown": "Мэдээгүй"}
SALE_ORDER = {"sell": 0, "unknown": 1, "out": 2, "stop": 3}
CROWD_STATUS = {"available": "Өгч байна", "queue": "Дараалалтай", "out": "Дууссан",
                "unknown": "Мэдээлэлгүй"}
WAITS = {"lt1h": "1 цаг хүртэл", "1_2h": "1-2 цаг", "2_3h": "2-3 цаг", "gt3h": "3+ цаг"}

_lock = threading.Lock()
_feed_cache = {"at": 0.0, "items": None}
_detail_cache = {}


def _get_json(path):
    req = urllib.request.Request(BASE + path, headers={"User-Agent": UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_feed():
    with _lock:
        if _feed_cache["items"] is not None and time.time() - _feed_cache["at"] < FEED_TTL:
            return _feed_cache["items"]
    items = _get_json("/api/gov/feed").get("items", [])
    with _lock:
        _feed_cache.update(at=time.time(), items=items)
    return items


def fetch_station(station_id, ttl=REGISTER_TTL):
    """/api/stations/<id> — бүртгэл, камер, хүмүүсийн мэдээ, ШТС-ын төлөв."""
    hit = _detail_cache.get(station_id)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    st = _get_json("/api/stations/" + urllib.parse.quote(station_id, safe="") + "?web=1")
    _detail_cache[station_id] = (time.time(), st)
    return st


def _fetch_quietly(station_id):
    try:
        return fetch_station(station_id)
    except Exception:
        return None


def fetch_stations(ids):
    with ThreadPoolExecutor(max_workers=6) as pool:
        return dict(zip(ids, pool.map(_fetch_quietly, ids)))


def detail_path(station_id):
    """ШТС-ын дэлгэрэнгүй JSON-ы харьцангуй зам. ID-д кирилл, тусгай тэмдэгт байдаг тул hash."""
    return "stations/" + hashlib.sha1(station_id.encode("utf-8")).hexdigest()[:16] + ".json"


def in_city(item):
    lat, lon = item.get("lat"), item.get("lon")
    if lat is None or lon is None:
        return False
    s, w, n, e = CITY_BBOX
    return s <= lat <= n and w <= lon <= e


def _ts(iso):
    try:
        return datetime.fromisoformat(iso).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _said_at(item):
    """ШТС өөрөө сүүлд мэдээ оруулсан цаг (benzin.mn-ийн govSaidAt-тэй адил)."""
    if item.get("said_at"):
        return item["said_at"]
    g = item["gov"]
    times = [g.get("confirmed_at"), g.get("started_at"), g.get("stopped_at"), g.get("ended_at")]
    times += [x.get("measured_at") for x in g.get("stock") or [] if x]
    times += [x.get("effective_at") for x in g.get("prices") or [] if x]
    times = [t for t in times if t and _ts(t)]
    return max(times, key=_ts) if times else g.get("updated_at")


def _location(item, st):
    st = st or {}
    reg = (st.get("gov") or {}).get("register") or {}
    place = ", ".join(x for x in [reg.get("district") or item.get("district"), reg.get("khoroo")] if x)
    addr = reg.get("address") or st.get("address") or ""
    if addr and addr in place:
        addr = ""
    return {
        # Бүртгэлийн дүүрэг benzin.mn-ийнхээс найдвартай (тэнд Налайх «Баянзүрх» болсон байдаг),
        # гэхдээ заримд нь хороо бичигдсэн байдаг тул зөвхөн жинхэнэ дүүргийн нэрийг авна.
        "district": (reg.get("district") if reg.get("district") in UB_DISTRICTS else None)
        or item.get("district") or "",
        "address": " · ".join(x for x in [place, addr] if x),
        "lat": item.get("lat"),
        "lon": item.get("lon"),
        "map_url": f"https://www.google.com/maps?q={item.get('lat')},{item.get('lon')}"
        if item.get("lat") is not None else "",
    }


def _base(item, st):
    st = st or {}
    reg = (st.get("gov") or {}).get("register") or {}
    return {
        "id": item["id"],
        "name": item.get("name") or "ШТС",
        "brand": item.get("brand_label") or "",
        "operator": reg.get("operator") or "",
        "work_hours": reg.get("work_hours") or "",
        "cams": len([c for c in st.get("cams") or [] if c and c.get("url")]),
        "in_city": in_city(item),
        "detail": detail_path(item["id"]),
        "benzin_url": f"{BASE}/?station={urllib.parse.quote(item['id'])}",
        "said_at": _said_at(item),
        **_location(item, st),
    }


def refill_list(items, stations):
    rows = []
    for it in items:
        refills = (it.get("gov") or {}).get("refills") or []
        if not refills:
            continue
        last = max(refills, key=lambda r: _ts(r.get("at")))
        same = [r for r in refills if r.get("at") == last.get("at")]
        rows.append({
            **_base(it, stations.get(it["id"])),
            "refill_at": last.get("at"),
            "refill_from": last.get("from_at"),
            "fuels": [r.get("label") for r in same],
            "changes": [f"{r.get('label')}: {LEVELS.get(r.get('from'), r.get('from'))} → "
                        f"{LEVELS.get(r.get('to'), r.get('to'))}" for r in same],
        })
    rows.sort(key=lambda r: _ts(r["refill_at"]), reverse=True)
    return rows


def _dispensing_off(g):
    if "dispensing" in g:
        return g["dispensing"] is False
    return g.get("state") == "stopped"


def selling92_list(items, stations, grades=("ai92",)):
    rows = []
    for it in items:
        g = it.get("gov") or {}
        if not g.get("reported") or _dispensing_off(g) or g.get("state") == "stale":
            continue
        stock = {s.get("product"): s for s in g.get("stock") or [] if s}
        prices = {p.get("product"): p for p in g.get("prices") or [] if p}
        fuels = []
        for p in g.get("products") or []:
            code = p.get("product")
            if code not in grades:
                continue
            # Хэвийн түгээж байгаа = дуусаагүй, дундраагүй. Савны түвшин бол хэмжсэн
            # үеийн утга; одоогийн төлөв нь product.state (benzin.mn-ийн govFuelSale).
            if p.get("state") != "normal" or p.get("cars_left") == 0:
                continue
            sk = stock.get(code) or {}
            fuels.append({
                "code": code,
                "label": p.get("label") or GRADE_92.get(code, code),
                "cars_left": p.get("cars_left"),
                "minutes_left": p.get("minutes_left"),
                "estimated": p.get("estimated") is True,
                "nozzles": p.get("nozzles"),
                "price": (prices.get(code) or {}).get("price"),
                "tank_level": LEVELS.get(sk.get("level"), sk.get("level") or ""),
                "tank_measured_at": sk.get("measured_at"),
            })
        if fuels:
            rows.append({**_base(it, stations.get(it["id"])), "fuels": fuels,
                         "started_at": g.get("started_at")})
    rows.sort(key=lambda r: _ts(r["said_at"]), reverse=True)
    return rows


def build():
    items = fetch_feed()
    grades = tuple(GRADE_92)
    # Эхлээд хаяггүйгээр шүүж, зөвхөн жагсаалтад орсон ШТС-уудын бүртгэлийг татна.
    wanted = {r["id"] for r in refill_list(items, {}) + selling92_list(items, {}, grades)}
    stations = fetch_stations(sorted(wanted))
    return {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": BASE + "/api/gov/feed",
        "total_stations": len(items),
        "city_stations": len([it for it in items if in_city(it)]),
        "refilled": refill_list(items, stations),
        "selling92": selling92_list(items, stations, grades),
    }


def write_site(out):
    """GitHub Pages-д зориулсан статик сайт: index.html, data.json, stations/<hash>.json."""
    data = build()
    os.makedirs(os.path.join(out, "stations"), exist_ok=True)
    here = os.path.dirname(os.path.abspath(__file__))
    shutil.copy(os.path.join(here, "static", "index.html"), os.path.join(out, "index.html"))
    ids = {r["id"] for r in data["refilled"] + data["selling92"]}
    failed = 0
    for sid in sorted(ids):
        try:
            detail = station_detail(sid)
        except Exception:
            failed += 1
            continue
        _write_json(os.path.join(out, detail_path(sid)), detail)
    _write_json(os.path.join(out, "data.json"), data)
    return data, len(ids) - failed, failed


def _write_json(path, obj):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, separators=(",", ":"))


def _gov_fuel_sale(p, stock, run_off):
    """benzin.mn-ийн govFuelSale: нэг түлш зарагдаж байгаа эсэх."""
    if run_off or (p and p.get("state") == "stopped"):
        return "stop"
    if p and p.get("state") == "stale":
        return "unknown"
    if p and p.get("cars_left") == 0:
        return "out"
    if p and p.get("state") in GOV_STATES:
        return "sell"
    if stock and stock.get("level") == "empty":
        return "out"
    return "unknown"


def _gov_detail(g):
    if not g or not g.get("reported"):
        return None
    run_off = _dispensing_off(g)
    rows, order = {}, []
    for key, field in (("products", "p"), ("prices", "price"), ("stock", "stock")):
        for x in g.get(key) or []:
            code = x.get("product") or x.get("label")
            if code not in rows:
                rows[code] = {"label": x.get("label") or FUEL_LABELS.get(code, code)}
                order.append(code)
            rows[code][field] = x
    fuels = []
    for code in order:
        r = rows[code]
        p, sk, price = r.get("p"), r.get("stock"), r.get("price")
        sale = _gov_fuel_sale(p, sk, run_off)
        fuels.append({
            "label": r["label"],
            "sale": sale,
            "sale_word": SALE_WORDS[sale],
            "state": GOV_STATES.get((p or {}).get("state"), ""),
            "estimated": (p or {}).get("estimated") is True,
            "cars_left": (p or {}).get("cars_left"),
            "minutes_left": (p or {}).get("minutes_left"),
            "nozzles": (p or {}).get("nozzles"),
            "price": (price or {}).get("price"),
            "tank_level": LEVELS.get((sk or {}).get("level"), (sk or {}).get("level") or ""),
            "tank_color": (sk or {}).get("color"),
            "tank_measured_at": (sk or {}).get("measured_at"),
        })
    fuels.sort(key=lambda f: SALE_ORDER[f["sale"]])
    refills = sorted(g.get("refills") or [], key=lambda r: _ts(r.get("at")), reverse=True)
    return {
        "state": GOV_STATES.get("stopped" if run_off else g.get("state"), ""),
        "dispensing": not run_off,
        "nozzles": g.get("nozzles"),
        "started_at": g.get("started_at"),
        "stopped_at": g.get("stopped_at") or g.get("ended_at"),
        "confirmed_at": g.get("confirmed_at") or g.get("updated_at"),
        "fuels": fuels,
        "refills": [{"at": r.get("at"), "from_at": r.get("from_at"), "label": r.get("label"),
                     "change": f"{LEVELS.get(r.get('from'), r.get('from'))} → "
                               f"{LEVELS.get(r.get('to'), r.get('to'))}"} for r in refills[:5]],
    }


def station_detail(station_id):
    st = fetch_station(station_id, ttl=DETAIL_TTL)
    g = st.get("gov") or {}
    reg = g.get("register") or {}
    crowd = []
    for code, f in (st.get("fuels") or {}).items():
        crowd.append({"label": FUEL_LABELS.get(code, code), **_crowd(f)})
    if st.get("any"):
        crowd.append({"label": "Ерөнхий", **_crowd(st["any"])})
    history = [{
        "label": FUEL_LABELS.get(h.get("fuel"), h.get("fuel")),
        "status": CROWD_STATUS.get(h.get("status"), h.get("status")),
        "status_code": h.get("status"),
        "wait": WAITS.get(h.get("wait"), ""),
        "note": h.get("note") or "",
        "at": h.get("created_at"),
        "replies": [x.get("text") for x in h.get("replies") or [] if x.get("text")][:3],
    } for h in (st.get("history") or [])[:10]]
    item = {"id": st["id"], "name": st.get("name"), "district": st.get("district"),
            "brand_label": st.get("brand_label"), "lat": st.get("lat"), "lon": st.get("lon"),
            "gov": g}
    return {
        **_base(item, st),
        "operator": reg.get("operator") or "",
        "cams": [{"url": c["url"], "label": c.get("label") or "Камер"}
                 for c in st.get("cams") or [] if c and c.get("url")],
        "gov": _gov_detail(g),
        "crowd": crowd,
        "last_queue": st.get("last_queue"),
        "history": history,
    }


def _crowd(f):
    return {
        "status": CROWD_STATUS.get(f.get("status"), f.get("status")),
        "status_code": f.get("status"),
        "wait": WAITS.get(f.get("wait"), ""),
        "reported_at": f.get("reported_at"),
        "reports": f.get("reports_counted"),
        "note": f.get("note") or "",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", help="статик сайтыг энэ хавтас руу бичнэ (GitHub Pages)")
    args = ap.parse_args()
    if args.out:
        data, ok, failed = write_site(args.out)
        print(f"{args.out}: орлого {len(data['refilled'])}, 92 {len(data['selling92'])}, "
              f"дэлгэрэнгүй {ok} ШТС" + (f" ({failed} татагдсангүй)" if failed else ""))
        raise SystemExit(0)

    data = build()
    city = lambda rows: [r for r in rows if r["in_city"]]
    print(f"Хот доторх {data['city_stations']} ШТС (нийт {data['total_stations']})")
    print(f"\nОрлого буусан: {len(city(data['refilled']))}")
    for r in city(data["refilled"]):
        print(f"  {r['refill_at']}  {r['name']}  [{r['address'] or r['district']}]  {', '.join(r['fuels'])}")
    print(f"\n92 хэвийн өгч байгаа: {len(city(data['selling92']))}")
    for r in city(data["selling92"]):
        print(f"  {r['said_at']}  {r['name']}  [{r['address'] or r['district']}]  "
              f"{', '.join(f['label'] for f in r['fuels'])}")
