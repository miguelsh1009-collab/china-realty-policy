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
import sys
from datetime import date, datetime
from pathlib import Path

from playwright.async_api import async_playwright

DATA_FILE = Path(__file__).parent.parent / "data" / "shanghai_fang.json"
URL = "https://sh.newhouse.fang.com/xfbusiness/deal.htm"
CHROME_PROFILE = Path.home() / "Library/Application Support/Google/Chrome"

# 要抓取的月份数（从当前月往前）
DEFAULT_MONTHS = 12


def month_list(n: int) -> list[str]:
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
    打开月份下拉，点击目标月份 li，等待数据刷新。
    返回 True 表示成功切换。
    """
    # 1. 点击下拉按钮打开
    await page.click("#monthSelect", timeout=8000)
    await page.wait_for_timeout(600)

    # 2. 找到包含目标月份文字的 li（精确文本匹配）
    li_loc = page.locator(".selectUl li").filter(has_text=target_month)
    count = await li_loc.count()
    if count == 0:
        print(f"  [!] 找不到月份选项: {target_month}")
        # 关闭下拉（按 Escape）
        await page.keyboard.press("Escape")
        return False

    # 3. 点击 li（不是内部的 p），触发月份切换
    await li_loc.first.click()
    await page.wait_for_timeout(800)

    # 4. 等待网络稳定（数据刷新）
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass

    await page.wait_for_timeout(500)
    return True


async def get_total_pages(page) -> int:
    """读取分页总页数"""
    try:
        # 常见分页结构：共 X 页
        text = await page.locator(".pageBox, .pagination, [class*='page']").first.inner_text(timeout=4000)
        import re
        m = re.search(r"共\s*(\d+)\s*页", text)
        if m:
            return int(m.group(1))
    except Exception:
        pass

    # 备用：找最大页码按钮
    try:
        btns = await page.locator(".pageBox a, .pagination a").all_inner_texts()
        nums = []
        for t in btns:
            t = t.strip()
            if t.isdigit():
                nums.append(int(t))
        if nums:
            return max(nums)
    except Exception:
        pass

    return 1


async def parse_table(page) -> list[dict]:
    """解析当前页成交数据表格，返回行列表"""
    rows = []
    try:
        # 等待表格出现
        await page.wait_for_selector("table tbody tr, .dealList tr", timeout=6000)
        trs = await page.locator("table tbody tr").all()
        for tr in trs:
            tds = await tr.locator("td").all_inner_texts()
            if len(tds) < 5:
                continue
            # 列顺序：成交月份、楼盘名称、区域、开发商、成交套数
            try:
                units_str = tds[4].strip().replace(",", "")
                units = int(units_str) if units_str.isdigit() else 0
            except Exception:
                units = 0
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


async def go_to_next_page(page, current: int) -> bool:
    """点击下一页，返回 False 表示已是最后一页"""
    try:
        # 查找"下一页"按钮或 current+1 数字
        next_btn = page.locator(f".pageBox a:has-text('{current + 1}'), .pagination a:has-text('{current + 1}')")
        if await next_btn.count() > 0:
            await next_btn.first.click()
            await page.wait_for_load_state("networkidle", timeout=6000)
            await page.wait_for_timeout(400)
            return True

        # 备用：找"下一页"文字按钮
        nxt = page.locator(".pageBox a:has-text('下一页'), a:has-text('下一页')")
        if await nxt.count() > 0:
            cls = await nxt.first.get_attribute("class") or ""
            if "disable" in cls or "disabled" in cls:
                return False
            await nxt.first.click()
            await page.wait_for_load_state("networkidle", timeout=6000)
            await page.wait_for_timeout(400)
            return True
    except Exception:
        pass
    return False


async def scrape_month(page, target_month: str) -> dict | None:
    """
    抓取指定月份所有分页数据，返回汇总 dict 或 None。
    """
    print(f"  切换到 {target_month} …")
    ok = await select_month(page, target_month)
    if not ok:
        return None

    # 验证页面确实切换到了目标月份
    # （部分情况下切换失败仍显示旧月份）
    try:
        visible_month = await page.locator("#monthSelect .text_select, #monthSelect").inner_text(timeout=3000)
        visible_month = visible_month.strip()
        if target_month not in visible_month:
            print(f"    [!] 月份切换可能失败，当前显示: {visible_month!r}")
    except Exception:
        pass

    all_rows: list[dict] = []
    current_page = 1
    total_pages = await get_total_pages(page)
    print(f"    共 {total_pages} 页")

    while True:
        rows = await parse_table(page)
        all_rows.extend(rows)
        print(f"    第 {current_page} 页，获取 {len(rows)} 条")
        if current_page >= total_pages:
            break
        ok = await go_to_next_page(page, current_page)
        if not ok:
            break
        current_page += 1

    if not all_rows:
        print(f"    [!] {target_month} 无数据")
        return None

    # 按区域汇总
    by_district: dict[str, int] = {}
    total = 0
    for r in all_rows:
        d = r["district"] or "未知"
        by_district[d] = by_district.get(d, 0) + r["units"]
        total += r["units"]

    # 去重（同月项目可能重复出现在多页）
    # 用 项目名+区域 做 key
    seen: set[str] = set()
    deduped: list[dict] = []
    for r in all_rows:
        key = r["project"] + "|" + r["district"]
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    by_district_dedup: dict[str, int] = {}
    total_dedup = 0
    for r in deduped:
        d = r["district"] or "未知"
        by_district_dedup[d] = by_district_dedup.get(d, 0) + r["units"]
        total_dedup += r["units"]

    return {
        "month": target_month,
        "total": total_dedup,
        "by_district": dict(sorted(by_district_dedup.items(), key=lambda x: -x[1])),
        "project_count": len(deduped),
    }


async def main():
    # 解析参数
    n_months = DEFAULT_MONTHS
    for arg in sys.argv[1:]:
        if arg.startswith("--months="):
            n_months = int(arg.split("=")[1])

    targets = month_list(n_months)
    print(f"计划抓取月份: {targets}")

    # 读取现有数据
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            existing = json.load(f)
    else:
        existing = {"updated": "", "source": "房天下-上海新房成交备案", "source_url": URL, "monthly": []}

    existing_months = {m["month"]: i for i, m in enumerate(existing["monthly"])}

    async with async_playwright() as pw:
        browser = await pw.chromium.launch_persistent_context(
            user_data_dir=str(CHROME_PROFILE),
            headless=False,
            args=["--no-first-run", "--no-default-browser-check"],
        )
        page = browser.pages[0] if browser.pages else await browser.new_page()

        print(f"打开页面: {URL}")
        await page.goto(URL, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(1500)

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

    # 按月份排序
    existing["monthly"].sort(key=lambda x: x["month"])
    existing["updated"] = date.today().isoformat()

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
        f.write("\n")

    print(f"\n完成，共 {len(existing['monthly'])} 个月，已写入 {DATA_FILE}")


if __name__ == "__main__":
    asyncio.run(main())
