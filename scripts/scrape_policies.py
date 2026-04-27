#!/usr/bin/env python3
"""
房产政策自动抓取脚本
数据来源：搜房网政策频道、新浪乐居、各城市住建委RSS
每天由 GitHub Actions 执行，将新政策写入 data/policies.json
"""

import json
import hashlib
import re
import time
from datetime import date, datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

DATA_FILE = Path(__file__).parent.parent / 'data' / 'policies.json'
TODAY = date.today().isoformat()
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (compatible; PolicyBot/1.0)',
    'Accept-Language': 'zh-CN,zh;q=0.9'
}

CITY_KEYWORDS = {
    '北京': ['北京', '京'],
    '上海': ['上海', '沪'],
    '广州': ['广州', '穗'],
    '深圳': ['深圳'],
    '成都': ['成都'],
    '杭州': ['杭州'],
    '重庆': ['重庆'],
    '武汉': ['武汉'],
    '西安': ['西安'],
    '南京': ['南京'],
    '苏州': ['苏州'],
    '长沙': ['长沙'],
    '郑州': ['郑州'],
    '天津': ['天津'],
    '宁波': ['宁波'],
    '合肥': ['合肥'],
    '济南': ['济南'],
    '青岛': ['青岛'],
}

TYPE_KEYWORDS = {
    '限购': ['限购', '购房限制', '购房资格'],
    '限贷': ['限贷', '首付', '贷款比例', '房贷'],
    '限售': ['限售', '持有年限'],
    '金融': ['利率', 'LPR', '贷款利率', '公积金'],
    '税费': ['契税', '增值税', '个税', '豪宅税', '税费'],
    '补贴': ['补贴', '奖励', '优惠', '人才'],
    '土地': ['土拍', '供地', '地价', '容积率'],
}

IMPACT_KEYWORDS = {
    '宽松': ['取消', '放开', '降低', '下调', '优化', '支持', '鼓励', '补贴', '放松', '放宽', '减免'],
    '收紧': ['收紧', '限制', '提高', '加强', '严格', '禁止', '上调'],
}


def detect_city(text):
    for city, kws in CITY_KEYWORDS.items():
        if any(kw in text for kw in kws):
            return city
    return None


def detect_type(text):
    for ptype, kws in TYPE_KEYWORDS.items():
        if any(kw in text for kw in kws):
            return ptype
    return '其他'


def detect_impact(text):
    loose_score = sum(1 for kw in IMPACT_KEYWORDS['宽松'] if kw in text)
    tight_score = sum(1 for kw in IMPACT_KEYWORDS['收紧'] if kw in text)
    if loose_score > tight_score:
        return '宽松'
    elif tight_score > loose_score:
        return '收紧'
    return '中性'


def make_id(city, date_str, title):
    h = hashlib.md5(title.encode()).hexdigest()[:6]
    city_abbr = city[:2].lower()
    return f"{city_abbr}-{date_str.replace('-','')}-{h}"


def fetch_soufun_policy():
    """搜房网政策频道"""
    policies = []
    try:
        url = 'https://news.fang.com/policy/'
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, 'lxml')
        items = soup.select('.list-con li, .news-list li')[:20]
        for item in items:
            a = item.find('a')
            if not a:
                continue
            title = a.get_text(strip=True)
            href = a.get('href', '')
            date_el = item.find(class_=re.compile(r'date|time'))
            date_str = date_el.get_text(strip=True)[:10] if date_el else TODAY
            city = detect_city(title)
            if not city:
                continue
            policies.append({
                'title': title, 'date': date_str, 'city': city,
                'type': detect_type(title), 'impact': detect_impact(title),
                'summary': title, 'source': '搜房网', 'url': href, 'tags': []
            })
    except Exception as e:
        print(f'[soufun] error: {e}')
    return policies


def fetch_leju_policy():
    """乐居政策"""
    policies = []
    try:
        url = 'https://policy.leju.com/'
        resp = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(resp.text, 'lxml')
        items = soup.select('ul.list li, .list-item')[:20]
        for item in items:
            a = item.find('a')
            if not a:
                continue
            title = a.get_text(strip=True)
            city = detect_city(title)
            if not city:
                continue
            policies.append({
                'title': title, 'date': TODAY, 'city': city,
                'type': detect_type(title), 'impact': detect_impact(title),
                'summary': title, 'source': '乐居', 'url': '', 'tags': []
            })
    except Exception as e:
        print(f'[leju] error: {e}')
    return policies


def deduplicate(existing, new_policies):
    existing_ids = {p['id'] for p in existing}
    existing_titles = {p['title'] for p in existing}
    result = []
    for p in new_policies:
        pid = make_id(p['city'], p['date'], p['title'])
        if pid not in existing_ids and p['title'] not in existing_titles:
            p['id'] = pid
            result.append(p)
    return result


def main():
    with open(DATA_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    existing = data.get('policies', [])
    print(f'已有政策 {len(existing)} 条')

    new_raw = []
    new_raw.extend(fetch_soufun_policy())
    time.sleep(2)
    new_raw.extend(fetch_leju_policy())
    print(f'抓取到原始条目 {len(new_raw)} 条')

    new_dedup = deduplicate(existing, new_raw)
    print(f'去重后新增 {len(new_dedup)} 条')

    if new_dedup:
        data['policies'] = new_dedup + existing
        print(f'新增政策 {len(new_dedup)} 条')
    else:
        print('无新政策，仅刷新更新时间')

    data['updated'] = TODAY
    with open(DATA_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f'写入完成，总计 {len(data["policies"])} 条，更新时间 {TODAY}')


if __name__ == '__main__':
    main()
