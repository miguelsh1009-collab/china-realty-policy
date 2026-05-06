#!/usr/bin/env python3
"""
上海新房月度成交数据抓取 — 房天下
来源：https://sh.newhouse.fang.com/xfbusiness/deal.htm

每次运行：
  1. 读取现有 data/shanghai_fang.json（若不存在则新建）
  2. 抓取最近 N 个月数据（默认 12 个月）
  3. 合并写入文件

用法：
  python scrape_fang_sh.py            # 默认抓取近 12 个月
  python scrape_fang_sh.py --months=3 # 抓取近 3 个月
"""

import asyncio
import json
import math
import re
import sys
from datetime import date
from pathlib import Path
from typing import Optional

from playwright.async_api import async_playwright

DATA_FILE = Path(__file__).parent.parent / "data" / "shanghai_fang.json"
URL = "https://sh.newhouse.fang.com/xfbusiness/deal.htm"
PAGE_SIZE = 10  # fang.com 每页条数


def month_list(n: int) -> list:
    """返回最近 n 个月的字符串列表，如 ['2026-05', '2026-04', ...]"""
    today = date.today()
    result = []
    y, m = today.year, today.month
    for _ in range(n):
        result.append(f"{y}-{m:02d}")
        m -= 1
        if m == 0:
            m = 12
            y -= 1
    return result


async def select_month(page, target_month: str) -> bool:
    """
    1. 通过 mousedown/mouseup/click 事件点击对应月份 li，将 #monthSelect input 值设为目标月份
    2. 点击 #searchBtn 查询按钮触发数据刷新
    返回 True 表示操作成功（input 值已更新）。
    """
    # Step 1: 触发 li 上的鼠标事件，让 JS 把 input 值设为目标月份
    clicked = await page.evaluate(
        """(month) => {
            const lis = document.querySelectorAll('.selectUl li');
            for (const li of lis) {
                const p = li.querySelector('p');
                if (p && p.textContent.trim() === month) {
                    ['mousedown', 'mouseup', 'click'].forEach(evt =>
                        li.dispatchEvent(new MouseEvent(evt, {bubbles: true, cancelable: true})));
                    return true;
                }
            }
            return false;
        }""",
        target_month,
    )

    if not clicked:
        print(f"  [!] 找不到月份选项: {target_month}")
        return False

    await page.wait_for_timeout(400)

    # 确认 input 值已更新
    input_val = await page.locator("#monthSelect").input_value()
    if target_month not in input_val:
        print(f"  [!] input 值未变 ({input_val!r})，跳过")
        return False

    # Step 2: 点击查询按钮触发数据加载
    await page.click("#searchBtn", timeout=5000)
    await page.wait_for_timeout(1500)

    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass

    await page.wait_for_timeout(500)
    return True


async def get_total_pages(page) -> int:
    """从 #pageDiv 的"尾页" span 的 data-page 属性读取总页数。"""
    try:
        # 优先：读"尾页" span 的 data-page（最精确）
        last_pg = await page.locator("#pageDiv span:has-text('尾页')").first.get_attribute("data-page", timeout=3000)
        if last_pg and last_pg.isdigit():
            n = int(last_pg)
            print(f"    共 {n} 页（来自尾页span）")
            return n
    except Exception:
        pass

    try:
        # 备用：从"共XX条"计算
        text = await page.locator("#pageDiv").first.inner_text(timeout=3000)
        m = re.search(r"共\s*(\d+)\s*条", text)
        if m:
            total_records = int(m.group(1))
            pages = math.ceil(total_records / PAGE_SIZE)
            print(f"    共 {total_records} 条 → {pages} 页")
            return pages
    except Exception:
        pass

    return 1


async def parse_table(page) -> list:
    """解析当前页 #deallist 表格，返回行列表。"""
    rows = []
    try:
        await page.wait_for_selector("#deallist tr", timeout=6000)
        trs = await page.locator("#deallist tr").all()
        for tr in trs:
            tds = await tr.locator("td").all_inner_texts()
            if len(tds) < 5:
                continue
            # 列：成交月份 | 楼盘名称 | 区域 | 开发商 | 成交套数
            units_raw = tds[4].strip()
            m = re.search(r"(\d+)", units_raw)
            units = int(m.group(1)) if m else 0
            rows.append({
                "month": tds[0].strip(),
                "project": tds[1].strip(),
                "district": tds[2].strip(),
                "developer": tds[3].strip(),
                "units": units,
            })
    except Exception as e:
        print(f"    [!] 表格解析异常: {e}")
    return rows


async def go_to_next_page(page) -> bool:
    """点击 #pageDiv 中的"下一页" span，返回 False 表示已是最后一页。"""
    try:
        # fang.com 用 <span data-page="N">下一页</span>
        nxt = page.locator("#pageDiv span:has-text('下一页')")
        if await nxt.count() == 0:
            return False

        # 检查 data-page：如果等于当前已选中页（class="on"），说明已到最后
        next_pg = await nxt.first.get_attribute("data-page", timeout=2000)
        on_span = page.locator("#pageDiv span.on")
        current_pg = None
        if await on_span.count() > 0:
            current_pg = await on_span.first.get_attribute("data-page", timeout=2000)

        if next_pg and current_pg and next_pg == current_pg:
            return False  # 下一页 == 当前页，已到最后

        await nxt.first.click()
        await page.wait_for_timeout(1200)
        try:
            await page.wait_for_load_state("networkidle", timeout=8000)
        except Exception:
            pass
        await page.wait_for_timeout(400)
        return True
    except Exception as e:
        print(f"    [!] 翻页异常: {e}")
        return False


async def scrape_month(page, target_month: str) -> Optional[dict]:
    """抓取指定月份所有分页数据，返回汇总 dict 或 None。"""
    print(f"  切换到 {target_month} …")
    ok = await select_month(page, target_month)
    if not ok:
        return None

    # 验证第一行确实是目标月份
    try:
        first_cell = await page.locator("#deallist tr td:first-child p").first.inner_text(timeout=3000)
        print(f"    第一行月份: {first_cell.strip()!r}")
        if target_month not in first_cell:
            print(f"    [!] 月份未切换，跳过")
            return None
    except Exception as e:
        print(f"    [!] 无法验证月份: {e}")
        return None

    total_pages = await get_total_pages(page)

    all_rows = []
    for current_page in range(1, total_pages + 1):
        rows = await parse_table(page)
        all_rows.extend(rows)
        print(f"    第 {current_page}/{total_pages} 页，获取 {len(rows)} 条")
        if current_page >= total_pages:
            break
        ok = await go_to_next_page(page)
        if not ok:
            print(f"    没有下一页，停止")
            break

    if not all_rows:
        print(f"    [!] {target_month} 无数据")
        return None

    # 按区域汇总，并用 项目名|区域 去重
    seen = set()
    deduped = []
    for r in all_rows:
        key = r["project"] + "|" + r["district"]
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    by_district = {}
    total = 0
    for r in deduped:
        d = r["district"] or "未知"
        by_district[d] = by_district.get(d, 0) + r["units"]
        total += r["units"]

    return {
        "month": target_month,
        "total": total,
        "by_district": dict(sorted(by_district.items(), key=lambda x: -x[1])),
        "project_count": len(deduped),
    }


async def main():
    n_months = 12
    for arg in sys.argv[1:]:
        if arg.startswith("--months="):
            n_months = int(arg.split("=")[1])

    targets = month_list(n_months)
    print(f"计划抓取月份: {targets}")

    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = {
            "updated": "",
            "source": "房天下-上海新房成交备案",
            "source_url": URL,
            "note": "数据来源房天下平台备案记录，按楼盘去重后汇总成交套数。",
            "monthly": [],
        }

    existing_months = {m["month"]: i for i, m in enumerate(existing["monthly"])}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            executable_path="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            headless=False,
            args=["--no-first-run", "--no-default-browser-check"],
        )
        page = await browser.new_page()

        print(f"打开页面: {URL}")
        await page.goto(URL, wait_until="load", timeout=30000)
        await page.wait_for_selector("#deallist tr", timeout=15000)
        await page.wait_for_timeout(1000)

        for month in targets:
            print(f"\n[月份] {month}")
            result = await scrape_month(page, month)
            if result is None:
                continue

            if month in existing_months:
                existing["monthly"][existing_months[month]] = result
                print(f"  更新: {month} 合计 {result['total']} 套")
            else:
                existing["monthly"].append(result)
                existing_months[month] = len(existing["monthly"]) - 1
                print(f"  新增: {month} 合计 {result['total']} 套")

        await browser.close()

    existing["monthly"].sort(key=lambda x: x["month"])
    existing["updated"] = date.today().isoformat()

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"\n完成，共 {len(existing['monthly'])} 个月，已写入 {DATA_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
