#!/usr/bin/env python3
"""
轻量成交数据抓取脚本。

目标不是替代人工核验，而是每天做一次公开新闻发现：
- 能从标题/摘要里识别出 城市 + 月份 + 新房/二手房 + 套数 时，写入 transactions.json
- 识别不到新增数据时，也刷新 updated，表示当天已经检查过
"""

import json
import re
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

DATA_FILE = Path(__file__).parent.parent / "data" / "transactions.json"
TODAY = date.today()
TODAY_STR = TODAY.isoformat()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; RealtyTxBot/1.0)",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

CITY_KEYWORDS = [
    "北京", "上海", "广州", "深圳", "成都", "杭州", "重庆", "武汉", "西安",
    "南京", "苏州", "长沙", "郑州", "天津", "宁波", "合肥", "济南", "青岛",
]
TRACKED_CITIES = ["北京", "上海", "广州", "深圳"]

METRIC_PATTERNS = [
    ("second", ["二手住宅", "二手房", "存量房"]),
    ("new", ["新房", "新建商品住宅", "新建商品房", "一手住宅", "一手房"]),
]


def month_candidates():
    months = []
    y, m = TODAY.year, TODAY.month
    for offset in range(0, 1):
        mm = m - offset
        yy = y
        while mm <= 0:
            mm += 12
            yy -= 1
        months.append(f"{yy}-{mm:02d}")
    return months


def display_month(month):
    y, m = month.split("-")
    return f"{int(y)}年{int(m)}月"


def normalize_num(raw, unit):
    num = float(raw.replace(",", ""))
    if unit == "万":
        num *= 10000
    return int(round(num))


def extract_value(text):
    patterns = [
        r"(?:成交|网签|备案|录得)[^，。；、]{0,12}?([0-9][0-9,]*(?:\.[0-9]+)?)(万?)套",
        r"([0-9][0-9,]*(?:\.[0-9]+)?)(万?)套[^，。；、]{0,12}?(?:成交|网签|备案|录得)",
    ]
    for pattern in patterns:
        m = re.search(pattern, text)
        if m:
            return normalize_num(m.group(1), m.group(2))
    return None


def detect_city(text):
    for city in CITY_KEYWORDS:
        if city in text:
            return city
    return None


def detect_month(text, fallback_month):
    full = re.search(r"(20\d{2})年\s*(\d{1,2})月", text)
    if full:
        return f"{int(full.group(1)):04d}-{int(full.group(2)):02d}"
    short = re.search(r"(?<!\d)(\d{1,2})月", text)
    if short:
        fallback_year = int(fallback_month[:4])
        month = int(short.group(1))
        return f"{fallback_year:04d}-{month:02d}"
    return fallback_month


def detect_metric(text):
    hits = []
    for metric, keywords in METRIC_PATTERNS:
        if any(kw in text for kw in keywords):
            hits.append(metric)
    return hits[0] if len(hits) == 1 else None


def fetch_url(url):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=(3, 4))
        resp.raise_for_status()
        return resp.text
    except Exception as exc:
        print(f"[fetch] {url} error: {exc}")
        return ""


def text_from_html(html):
    if not html:
        return ""
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(" ", strip=True)


def search_news(city, month):
    query = f"{display_month(month)} {city} 楼市 成交 套 二手房 新房"
    url = (
        "https://news.google.com/rss/search?q="
        + quote_plus(query)
        + "&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    )
    xml = fetch_url(url)
    if not xml:
        return []
    try:
        root = ET.fromstring(xml)
    except ET.ParseError as exc:
        print(f"[rss] parse error: {exc}")
        return []
    items = []
    for item in root.findall(".//item")[:5]:
        title = item.findtext("title") or ""
        link = item.findtext("link") or ""
        desc = item.findtext("description") or ""
        items.append({"title": title, "url": link, "description": desc})
    return items


def ensure_month(data, month):
    for row in data["monthly"]:
        if row.get("month") == month:
            row.setdefault("data", {})
            return row
    row = {"month": month, "data": {}}
    data["monthly"].append(row)
    data["monthly"].sort(key=lambda x: x["month"])
    return row


def merge_candidate(data, candidate):
    month_row = ensure_month(data, candidate["month"])
    city_data = month_row["data"].setdefault(candidate["city"], {
        "new": None,
        "second": None,
        "new_area": None,
        "second_avg_price": None,
        "metric_scope": "",
        "as_of_date": candidate["month"] + "-28",
        "confidence": "low",
        "conflicts": [],
        "source": "",
        "source_url": "",
    })

    metric = candidate["metric"]
    old_value = city_data.get(metric)
    new_value = candidate["value"]
    if old_value is None:
        city_data[metric] = new_value
        city_data["metric_scope"] = candidate["scope"]
        city_data["as_of_date"] = candidate["as_of_date"]
        city_data["confidence"] = "low"
        city_data["source"] = candidate["source"]
        city_data["source_url"] = candidate["source_url"]
        return True

    if old_value != new_value:
        conflicts = city_data.setdefault("conflicts", [])
        conflict_key = (candidate["source"], str(new_value), candidate["source_url"])
        existing = {(c.get("source"), str(c.get("value")), c.get("url", "")) for c in conflicts}
        if conflict_key not in existing:
            conflicts.append({
                "source": candidate["source"],
                "value": f"{new_value}套",
                "note": f"自动发现，当前主值为{old_value}套",
                "url": candidate["source_url"],
            })
            return True
    return False


def discover_candidates():
    candidates = []
    for month in month_candidates():
        for city in TRACKED_CITIES:
            for item in search_news(city, month):
                snippet = BeautifulSoup(item["description"], "lxml").get_text(" ", strip=True)
                combined = f"{item['title']} {snippet}"
                detected_city = detect_city(combined)
                detected_month = detect_month(combined, month)
                metric = detect_metric(combined)
                value = extract_value(combined)
                if detected_city != city or detected_month != month or not metric or not value:
                    continue
                candidates.append({
                    "city": city,
                    "month": month,
                    "metric": metric,
                    "value": value,
                    "scope": "自动抓取公开新闻；待人工复核口径",
                    "as_of_date": TODAY_STR,
                    "source": item["title"][:80],
                    "source_url": item["url"],
                })
    return candidates


def main():
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    candidates = discover_candidates()
    print(f"自动发现候选 {len(candidates)} 条")

    changed = 0
    for candidate in candidates:
        if merge_candidate(data, candidate):
            changed += 1
            print(
                f"写入候选：{candidate['month']} {candidate['city']} "
                f"{candidate['metric']}={candidate['value']}"
            )

    data["updated"] = TODAY_STR
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"成交数据检查完成，更新/冲突 {changed} 条，更新时间 {TODAY_STR}")


if __name__ == "__main__":
    main()
