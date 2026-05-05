#!/usr/bin/env python3
"""闲鱼套利监控 — 主入口。

用法:
  python monitor.py                  # 常驻模式 (APScheduler 定时轮询)
  python monitor.py --once           # 单次抓取后退出
  python monitor.py --config path    # 指定配置文件

每次运行：抓取 → 过滤 → 真实性检测 → 跨平台比价 → 聚合推送（1条微信消息）
"""

import argparse
import logging
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import yaml

from data_sources.smzdm_json import SMZDMJsonFetcher
from data_sources.smzdm_rss import SMZDMRssFetcher
from filters.keyword_filter import KeywordFilter
from storage.deal_history import DealHistory
from alerting.serverchan import ServerChanAlerter
from alerting.win_toast import DesktopNotifier
from price_checker import PriceChecker, compute_platform_stats

logger = logging.getLogger("monitor")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def setup_logging(config: dict):
    lc = config.get("logging", {})
    level = getattr(logging, lc.get("level", "INFO").upper(), logging.INFO)
    log_file = lc.get("file", "")
    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        Path(log_file).parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


def format_aggregated_alert(
    matched: list[dict],
    platform_stats: dict,
    total_fetched: int,
    filter_name: str,
) -> str:
    """Format ALL matched deals into a single WeChat message body.

    Each entry in `matched` is a dict with:
        deal, reason, authenticity, cross_platform_ok, cross_platform_info, price_insight
    """
    now = datetime.now().strftime("%m-%d %H:%M")
    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"🛒 套利监控 · {now}",
        f"共抓取 {total_fetched} 条，命中 {len(matched)} 条大额优惠",
        "",
    ]

    # Platform distribution summary
    if platform_stats:
        plat_summary = " | ".join(
            f"{p}({s['count']})" for p, s in
            sorted(platform_stats.items(), key=lambda x: -x[1]["count"])
        )
        lines.append(f"📦 平台分布：{plat_summary}")
        lines.append("")

    lines.append("━" * 20)

    # Sort: best discount first, then lowest price
    def sort_key(m):
        d = m["deal"]
        disc = 0
        if d.original_price > 0 and d.original_price > d.price:
            disc = (d.original_price - d.price) / d.original_price * 100
        return (-disc, d.price)

    matched_sorted = sorted(matched, key=sort_key)

    for i, m in enumerate(matched_sorted, 1):
        deal = m["deal"]
        discount_pct = 0
        if deal.original_price > 0 and deal.original_price > deal.price:
            discount_pct = round(
                (deal.original_price - deal.price) / deal.original_price * 100, 1
            )

        # Deal card
        lines.append(f"\n{i}. {deal.title}")
        price_line = f"   💰 ¥{deal.price:.1f}"
        if deal.original_price > 0:
            price_line += f"  |  原价 ¥{deal.original_price:.1f}"
            price_line += f"  |  {discount_pct:.0f}% OFF"
            price_line += f"  |  省 ¥{deal.original_price - deal.price:.1f}"
        lines.append(price_line)

        if deal.description:
            lines.append(f"   🏷️ {deal.description}")

        # Platform + platform-specific indicator
        platform_indicator = ""
        if m.get("cross_platform_ok") is False:
            platform_indicator = " ⚠️ 非全平台最低"
        elif m.get("cross_platform_ok") is True and m.get("cross_platform_info"):
            platform_indicator = " ✅ 全平台最低"
        lines.append(f"   📦 {deal.platform or '未知'}{platform_indicator}")

        # Votes
        if deal.worthy_count:
            lines.append(f"   👍 {deal.worthy_ratio:.0f}%值得 ({deal.worthy_count}票)")

        # Authenticity
        auth_label = ""
        if m.get("authenticity") is False:
            auth_label = " ⚠️ " + m.get("auth_reason", "折扣真实性存疑")
        elif m.get("price_insight"):
            auth_label = " " + m["price_insight"]
        if auth_label:
            lines.append(f"   🔍{auth_label}")

        # Cross-platform comparison
        if m.get("cross_platform_info"):
            lines.append(f"   🔗 {m['cross_platform_info']}")

        # URL
        lines.append(f"   🔗 {deal.url}")

        # Match reason
        lines.append(f"   🎯 {m['reason']}")

    lines.append(f"\n━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"📊 本次统计：抓取{total_fetched} | 命中{len(matched)} | 推送1条")
    lines.append(f"⏰ {now}")

    return "\n".join(lines)


def run_once(config: dict) -> dict:
    """Execute a single monitoring cycle. Returns summary dict."""
    sources_cfg = config.get("sources", {}).get("smzdm", {})

    # Initialize components
    json_fetcher = SMZDMJsonFetcher(sources_cfg)
    rss_fetcher = SMZDMRssFetcher(sources_cfg)
    filter_engine = KeywordFilter(config)
    history = DealHistory(config)
    wechat = ServerChanAlerter(config)
    desktop = DesktopNotifier(config)
    checker = PriceChecker(history, config)

    # ── Fetch ──
    deals = []
    if json_fetcher.is_available():
        json_fetcher.open_browser()
        deals = json_fetcher.fetch()
    else:
        logger.warning("Playwright unavailable, trying RSS fallback")
        if rss_fetcher.is_available():
            deals = rss_fetcher.fetch()
        else:
            logger.error("All sources unavailable")

    if not deals:
        logger.info("No deals fetched")
        if json_fetcher.is_available():
            json_fetcher.close_browser()
        return {"fetched": 0, "matched": 0, "sent": 0}

    # ── Platform diversity: actively search under-represented platforms ──
    target_platforms = sources_cfg.get(
        "target_platforms",
        ["拼多多", "淘宝", "抖音", "美团", "得物"],
    )
    existing_ids = {d.source_id for d in deals}
    for platform in target_platforms:
        if json_fetcher.is_available():
            platform_deals = json_fetcher.fetch_platform_deals(platform)
            for deal in platform_deals:
                if deal.source_id not in existing_ids:
                    existing_ids.add(deal.source_id)
                    deals.append(deal)
            time.sleep(0.5)
    logger.info("After platform diversity: %d total deals", len(deals))

    # ── Filter ──
    candidates = []
    for deal in deals:
        ok, reason = filter_engine.should_alert(deal)
        if not ok:
            history.record_seen(deal)
            continue
        # Check dedup
        alert_ok, dedup_reason = history.should_alert(deal)
        if not alert_ok:
            history.record_seen(deal)
            continue
        candidates.append({"deal": deal, "reason": f"{reason} / {dedup_reason}"})

    if not candidates:
        logger.info("No deals matched filters (fetched %d)", len(deals))
        if json_fetcher.is_available():
            json_fetcher.close_browser()
        return {"fetched": len(deals), "matched": 0, "sent": 0}

    # ── Authenticity + Cross-platform check ──
    for c in candidates:
        deal = c["deal"]

        # Price authenticity
        is_auth, auth_reason = checker.check_authenticity(deal)
        c["authenticity"] = is_auth
        c["auth_reason"] = auth_reason

        # Cross-platform check
        cp_ok, cp_reason = checker.cross_platform_check(deal, deals)
        c["cross_platform_ok"] = cp_ok or None  # None if no comparison data
        c["cross_platform_info"] = cp_reason if not cp_ok else (
            "全平台最低" if cp_ok else ""
        )

        # Price history insight
        c["price_insight"] = checker.get_price_history_insight(deal)

    # ── Actively find cross-platform comparisons for matched deals ──
    comparisons = checker.find_platform_comparisons(
        [c["deal"] for c in candidates], deals
    )
    # Merge comparison data back
    comp_map = {comp["deal"].source_id: comp for comp in comparisons}
    for c in candidates:
        comp = comp_map.get(c["deal"].source_id)
        if comp:
            c["cross_platform_data"] = comp
            if comp["is_truly_cheapest"]:
                c["cross_platform_ok"] = True
                c["cross_platform_info"] = "✅ 全平台最低"
            else:
                cheapest = comp["cheapest_other"]
                c["cross_platform_ok"] = False
                c["cross_platform_info"] = (
                    f"⚠️ {cheapest['platform']}同款仅¥{cheapest['price']:.1f} — 非真优惠"
                )

    # ── Active cross-platform search for top deals ──
    # For the best 5 discounts, search SMZDM to find other-platform listings
    import re as _re

    def _extract_keywords(title: str) -> str:
        """Extract brand + product type for search."""
        prefixes = [
            "移动端、", "京东百亿补贴：", "百亿补贴：", "PLUS会员：",
            "88VIP：", "移动端：", "有券的上：", "国家补贴：",
            "今日必买：", "值友专享、", "值友专享：", "限地区：",
            "京东百亿补贴、限地区：",
        ]
        t = title
        for p in prefixes:
            t = t.replace(p, "")
        # Keep first 15 chars (brand + product)
        return t.strip()[:20]

    # Identify top 5 deals for cross-platform search
    def _discount(d):
        deal = d["deal"]
        if deal.original_price > 0 and deal.original_price > deal.price:
            return (deal.original_price - deal.price) / deal.original_price * 100
        return 0
    top5 = sorted(candidates, key=_discount, reverse=True)[:5]

    all_cross_platform = []  # (keyword, list_of_deals)
    for c in top5:
        kw = _extract_keywords(c["deal"].title)
        if len(kw) < 5:
            continue
        cp_deals = json_fetcher.fetch_cross_platform(kw)
        if cp_deals:
            all_cross_platform.append((kw, cp_deals))

    # Merge cross-platform results
    for c in candidates:
        deal_title = _extract_keywords(c["deal"].title)
        for kw, cp_deals in all_cross_platform:
            if _re.search(_re.escape(kw[:8]), deal_title):
                # Group by platform
                platform_prices: dict[str, float] = {}
                for cp in cp_deals:
                    p = cp.platform or "未知"
                    if p not in platform_prices or cp.price < platform_prices[p]:
                        platform_prices[p] = cp.price

                my_price = c["deal"].price
                my_platform = c["deal"].platform or "未知"
                other_min = min(
                    (pr for pl, pr in platform_prices.items() if pl != my_platform),
                    default=None,
                )

                if other_min is not None and other_min < my_price * 0.90:
                    c["cross_platform_ok"] = False
                    # Find which platform
                    cheaper_plats = [
                        f"{pl}¥{pr:.1f}"
                        for pl, pr in sorted(platform_prices.items(), key=lambda x: x[1])
                        if pl != my_platform and pr < my_price * 0.95
                    ]
                    c["cross_platform_info"] = "⚠️ 比价：" + " | ".join(cheaper_plats[:3])
                elif other_min is not None and other_min >= my_price * 0.90:
                    c["cross_platform_ok"] = True
                    c["cross_platform_info"] = "✅ 全平台最低（经搜索验证）"
                break

    # ── Platform stats ──
    platform_stats = compute_platform_stats(deals)

    # ── Aggregate into ONE message ──
    body = format_aggregated_alert(
        candidates, platform_stats, len(deals),
        "折扣≥" + str(config.get("filters", {}).get("min_discount_pct", 45)) + "%"
    )

    title = f"🛒 套利监控 {len(candidates)}条好价 ({datetime.now().strftime('%H:%M')})"

    # ── Send ──
    wc_ok = wechat.send(title, body)
    dt_ok = desktop.send(
        title,
        f"抓取{len(deals)}条，命中{len(candidates)}条大额优惠\n点击微信查看详情"
    )

    # ── Record alerts ──
    sent = 0
    if wc_ok or dt_ok:
        for c in candidates:
            history.record_alert(c["deal"])
            sent += 1
        logger.info("✅ Aggregated push: %d deals in 1 message", sent)

    logger.info(
        "Cycle done: fetched=%d matched=%d sent=%d",
        len(deals), len(candidates), 1 if (wc_ok or dt_ok) else 0,
    )

    # ── Cleanup ──
    if json_fetcher.is_available():
        json_fetcher.close_browser()

    # Print summary to console
    print(f"\n{'='*50}")
    print(f"  套利监控 — {datetime.now().strftime('%m-%d %H:%M')}")
    print(f"  抓取: {len(deals)}条 | 命中: {len(candidates)}条大额优惠")
    print(f"  推送: {'已发送微信' if wc_ok else '发送失败'}")
    print(f"{'='*50}")
    for i, c in enumerate(candidates, 1):
        d = c["deal"]
        disc = 0
        if d.original_price > 0:
            disc = round((d.original_price - d.price) / d.original_price * 100)
        warnings = []
        if c.get("authenticity") is False:
            warnings.append("⚠️假折扣")
        if c.get("cross_platform_ok") is False:
            warnings.append("⚠️非最低")
        warn_str = " " + " ".join(warnings) if warnings else ""
        print(f"  {i}. [{d.platform}] ¥{d.price:.1f} ({disc}%OFF) {d.title[:40]}{warn_str}")
    print()


def main():
    # Fix Windows console encoding (default GBK can't handle emoji/¥)
    try:
        sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        sys.stderr.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass

    parser = argparse.ArgumentParser(description="闲鱼套利监控")
    parser.add_argument("--once", action="store_true", help="单次运行后退出")
    parser.add_argument("--config", default="config.yaml", help="配置文件路径")
    args = parser.parse_args()

    config = load_config(args.config)
    setup_logging(config)

    logger.info("Arbitrage Monitor starting (mode=%s)", "once" if args.once else "continuous")

    if args.once:
        run_once(config)
    else:
        from scheduler import run_scheduler
        run_scheduler(config)


if __name__ == "__main__":
    main()
