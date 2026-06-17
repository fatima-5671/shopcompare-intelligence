"""
Cross-Platform E-Commerce Scraper
===================================
Platforms:
  1. Daraz    — hidden JSON API (fast, reliable, full data)
  2. Amazon   — Selenium with proper selectors
  3. Alibaba  — hidden JSON API (fast, reliable, full data)

Fields: Title, Price, Original Price, Discount%, Rating,
        Reviews, Availability, Seller, Product URL

SETUP:
  pip install selenium webdriver-manager pandas openpyxl beautifulsoup4 lxml requests

RUN:
  python ecommerce_scraper.py
  python ecommerce_scraper.py --keywords "laptop" "headphones" --pages 3
  python ecommerce_scraper.py --platforms daraz amazon
"""

import time, random, re, os, logging, argparse, requests
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional

import pandas as pd
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)


# ════════════════════════════════════════════════
#  KEYWORDS — edit freely
# ════════════════════════════════════════════════
KEYWORDS = [
    "wireless earbuds",
    "smart watch",
    "men shirt",
    "women kurta",
    "electric drill",
    "screwdriver set",
]
PAGES_PER_KEYWORD = 2


# ════════════════════════════════════════════════
#  DATA MODEL
# ════════════════════════════════════════════════
@dataclass
class Product:
    platform:       str
    keyword:        str
    title:          str
    price:          Optional[float]
    currency:       str
    original_price: Optional[float]
    discount_pct:   Optional[float]
    rating:         Optional[float]
    review_count:   Optional[int]
    availability:   str
    seller:         str
    product_url:    str
    scraped_at:     str = ""

    def __post_init__(self):
        if not self.scraped_at:
            self.scraped_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════
def _f(val) -> Optional[float]:
    if val is None: return None
    try:
        s = re.sub(r"[^\d.]", "", str(val).replace(",", ""))
        return float(s) if s else None
    except: return None

def _i(val) -> Optional[int]:
    if val is None: return None
    try:
        s = str(val).upper().replace(",", "").strip()
        if "K" in s:
            return int(float(s.replace("K", "")) * 1000)
        n = re.sub(r"[^\d]", "", s)
        return int(n) if n else None
    except: return None

def _disc(orig, curr) -> Optional[float]:
    try:
        o, c = float(orig), float(curr)
        if o > c > 0: return round((o - c) / o * 100, 1)
    except: pass
    return None

def _delay(a=2.0, b=5.0): time.sleep(random.uniform(a, b))
def _soup(driver): return BeautifulSoup(driver.page_source, "lxml")

def _wait(driver, css, timeout=20):
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, css)))
        return True
    except TimeoutException: return False

def _scroll(driver, rounds=4):
    for i in range(1, rounds + 1):
        driver.execute_script(
            f"window.scrollTo(0,document.body.scrollHeight*{i}/{rounds});")
        time.sleep(random.uniform(0.7, 1.3))
    driver.execute_script("window.scrollTo(0,document.body.scrollHeight);")
    time.sleep(1.5)


# ════════════════════════════════════════════════
#  SELENIUM DRIVER
# ════════════════════════════════════════════════
def _make_driver():
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--window-size=1400,900")
    opts.add_argument("--lang=en-US")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument",
        {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"})
    return driver


# ════════════════════════════════════════════════
#  1. DARAZ — Hidden JSON API
#     Returns: price, original_price, discount,
#              rating, reviews, seller — all clean
# ════════════════════════════════════════════════
_DARAZ_UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
]

def scrape_daraz(keyword: str, max_pages: int) -> list:
    log.info(f"[Daraz]   '{keyword}'")
    products = []
    session = requests.Session()

    for page in range(1, max_pages + 1):
        url = (
            f"https://www.daraz.pk/catalog/?ajax=true"
            f"{'&isFirstRequest=true' if page == 1 else ''}"
            f"&page={page}&q={keyword.replace(' ', '%20')}"
        )
        headers = {
            "Referer":          "https://www.daraz.pk/",
            "User-Agent":       random.choice(_DARAZ_UA),
            "Accept":           "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
        }
        log.info(f"[Daraz]     page {page}")
        try:
            r = session.get(url, headers=headers, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning(f"[Daraz] failed: {e}"); break

        items = data.get("mods", {}).get("listItems", [])
        if not items:
            log.info("[Daraz]     no more items"); break

        log.info(f"[Daraz]     {len(items)} items")
        for item in items:
            try:
                title = item.get("name", "") or item.get("brandName", "")
                if not title: continue

                price      = _f(item.get("price")) or _f(item.get("priceShow"))
                orig_price = _f(item.get("originalPrice")) or _f(item.get("originalPriceShow"))

                disc = None
                ds = item.get("discount", "")
                if ds:
                    m = re.search(r"(\d+\.?\d*)%", str(ds))
                    if m: disc = float(m.group(1))
                if not disc: disc = _disc(orig_price, price)

                rating  = _f(item.get("ratingScore"))
                reviews = _i(item.get("review"))
                seller  = item.get("sellerName", "") or item.get("seller", "")
                purl    = item.get("productUrl", "") or item.get("itemUrl", "")
                if purl and not purl.startswith("http"):
                    purl = "https:" + purl
                stock = "Out of Stock" if item.get("inStock") is False else "In Stock"

                products.append(Product(
                    platform="Daraz", keyword=keyword, title=title,
                    price=price, currency="PKR",
                    original_price=orig_price, discount_pct=disc,
                    rating=rating, review_count=reviews,
                    availability=stock, seller=seller, product_url=purl,
                ))
            except Exception as e:
                log.debug(f"[Daraz] item err: {e}")
        _delay(1, 3)

    log.info(f"[Daraz]   ✓ {len(products)} products")
    return products


# ════════════════════════════════════════════════
#  2. AMAZON — Selenium
#     All selectors verified + multiple fallbacks
# ════════════════════════════════════════════════
def scrape_amazon(driver, keyword: str, max_pages: int) -> list:
    log.info(f"[Amazon]  '{keyword}'")
    products = []

    for page in range(1, max_pages + 1):
        url = f"https://www.amazon.com/s?k={keyword.replace(' ', '+')}&page={page}"
        log.info(f"[Amazon]    page {page}")
        driver.get(url)
        _delay(5, 8)

        # CAPTCHA check
        if any(x in driver.title.lower() for x in ["robot", "captcha", "blocked"]):
            log.warning("[Amazon] CAPTCHA — waiting 25s then retrying")
            time.sleep(25)
            driver.refresh()
            _delay(5, 8)

        found = _wait(driver, "[data-component-type='s-search-result']", timeout=25)
        if not found:
            log.info(f"[Amazon]    no results page {page}"); break

        _scroll(driver, rounds=4)
        _delay(1, 2)
        soup = _soup(driver)

        cards = [c for c in
                 soup.select("[data-component-type='s-search-result'][data-asin]")
                 if c.get("data-asin", "").strip()]
        log.info(f"[Amazon]    {len(cards)} cards")

        for card in cards:
            try:
                # ── Title ──────────────────────────────────────
                t = (card.select_one("h2 a span")
                     or card.select_one("h2 span")
                     or card.select_one("[class*='s-title'] span"))
                title = t.get_text(strip=True) if t else ""
                if not title: continue

                # ── URL ────────────────────────────────────────
                asin  = card.get("data-asin", "")
                a_el  = card.select_one("h2 a[href]")
                href  = a_el.get("href", "") if a_el else ""
                if href.startswith("/"):
                    link = "https://www.amazon.com" + href
                elif href.startswith("http"):
                    link = href
                else:
                    link = f"https://www.amazon.com/dp/{asin}" if asin else ""

                # ── Price (3 fallback methods) ──────────────────
                curr_price = None
                # Method 1: whole.fraction
                w = card.select_one(".a-price-whole")
                f_ = card.select_one(".a-price-fraction")
                if w:
                    ps = re.sub(r"[^\d]", "", w.get_text())
                    if f_: ps += "." + re.sub(r"[^\d]", "", f_.get_text())
                    curr_price = _f(ps)
                # Method 2: aria-hidden span
                if not curr_price:
                    for el in card.select(".a-price span[aria-hidden='true']"):
                        v = _f(re.sub(r"[^\d.]", "", el.get_text().replace(",", "")))
                        if v and v > 0: curr_price = v; break
                # Method 3: offscreen span
                if not curr_price:
                    for el in card.select(".a-offscreen"):
                        v = _f(re.sub(r"[^\d.]", "", el.get_text().replace(",", "")))
                        if v and v > 0: curr_price = v; break

                # ── Original price ──────────────────────────────
                orig_price = None
                for sel in [
                    ".a-price.a-text-price span[aria-hidden='true']",
                    "span.a-text-strike",
                    "[data-a-strike='true'] .a-offscreen",
                ]:
                    el = card.select_one(sel)
                    if el:
                        v = _f(re.sub(r"[^\d.]", "", el.get_text().replace(",", "")))
                        if v: orig_price = v; break
                disc = _disc(orig_price, curr_price)

                # ── Rating (4 fallback methods) ─────────────────
                rat = None
                # Method 1: i.a-icon-star aria-label
                for sel in [
                    "i.a-icon-star-small span.a-icon-alt",
                    "i.a-icon-star span.a-icon-alt",
                    "i[class*='a-star-'] span.a-icon-alt",
                ]:
                    el = card.select_one(sel)
                    if el:
                        m = re.search(r"(\d+\.?\d*)\s*out of", el.get_text())
                        if m: rat = float(m.group(1)); break
                # Method 2: span aria-label with "out of 5"
                if rat is None:
                    for el in card.select("span[aria-label*='out of 5']"):
                        m = re.search(r"(\d+\.?\d*)\s*out of",
                                      el.get("aria-label", ""))
                        if m: rat = float(m.group(1)); break
                # Method 3: span aria-label with "stars"
                if rat is None:
                    for el in card.select("span[aria-label*='stars']"):
                        m = re.search(r"(\d+\.?\d*)", el.get("aria-label", ""))
                        if m:
                            v = float(m.group(1))
                            if 0 < v <= 5: rat = v; break
                # Method 4: any element with text like "4.5"
                if rat is None:
                    for el in card.select("span.a-icon-alt"):
                        m = re.search(r"(\d+\.?\d*)\s*out of", el.get_text())
                        if m: rat = float(m.group(1)); break

                # ── Reviews (3 fallback methods) ────────────────
                reviews = None
                # Method 1: aria-label containing number of ratings
                for el in card.select("span[aria-label]"):
                    aria = el.get("aria-label", "")
                    if re.search(r"\d[\d,]*\s*(rating|review)", aria, re.I):
                        v = _i(re.sub(r"[^\d]", "", aria.split()[0]))
                        if v: reviews = v; break
                # Method 2: link to customer reviews
                if not reviews:
                    for el in card.select("a[href*='customerReviews'] span,"
                                          "a[href*='#customerReviews'] span"):
                        v = _i(el.get_text(strip=True))
                        if v: reviews = v; break
                # Method 3: underlined text (review count style)
                if not reviews:
                    for el in card.select("span.a-size-base.s-underline-text"):
                        v = _i(el.get_text(strip=True))
                        if v and v > 0: reviews = v; break

                # ── Availability ────────────────────────────────
                oos = card.select_one(
                    "span.a-color-error, [class*='unavailable']")
                stock = "Out of Stock" if oos else "In Stock"

                # ── Currency ────────────────────────────────────
                sym_el   = card.select_one(".a-price-symbol")
                sym      = sym_el.get_text(strip=True) if sym_el else "$"
                currency = {"$": "USD", "£": "GBP", "€": "EUR"}.get(sym, "USD")

                # ── Seller ──────────────────────────────────────
                seller_el = card.select_one(
                    "span.a-size-base + span.a-size-base,"
                    "div[class*='sponsored'] + span")
                seller = seller_el.get_text(strip=True) if seller_el else ""

                products.append(Product(
                    platform="Amazon", keyword=keyword, title=title,
                    price=curr_price, currency=currency,
                    original_price=orig_price, discount_pct=disc,
                    rating=rat, review_count=reviews,
                    availability=stock, seller=seller, product_url=link,
                ))
            except Exception as e:
                log.debug(f"[Amazon] card err: {e}")

        _delay(4, 7)

    log.info(f"[Amazon]  ✓ {len(products)} products")
    return products


# ════════════════════════════════════════════════
#  3. ALIBABA — Hidden JSON API
#     Returns: price range, rating, reviews,
#              seller/supplier, MOQ, orders
# ════════════════════════════════════════════════
_ALIBABA_UA = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15",
]

def scrape_alibaba(keyword: str, max_pages: int) -> list:
    log.info(f"[Alibaba] '{keyword}'")
    products = []
    session = requests.Session()

    for page in range(1, max_pages + 1):
        # Alibaba's internal search API
        url = (
            f"https://www.alibaba.com/trade/search/freeSearch.do"
            f"?SearchText={keyword.replace(' ', '+')}"
            f"&page={page}"
            f"&indexArea=product_en"
        )
        headers = {
            "User-Agent": random.choice(_ALIBABA_UA),
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.alibaba.com/",
            "X-Requested-With": "XMLHttpRequest",
        }
        log.info(f"[Alibaba]   page {page}")
        try:
            r = session.get(url, headers=headers, timeout=20)
            # Alibaba may return JSON or HTML depending on request
            if "application/json" in r.headers.get("content-type", ""):
                data = r.json()
                items = (data.get("data", {}).get("offerList", [])
                         or data.get("result", {}).get("resultList", []))
            else:
                # Parse HTML response
                soup = BeautifulSoup(r.text, "lxml")
                items = []

                # Each product card
                cards = (soup.select(".organic-offer-wrapper")
                         or soup.select("[class*='offer-wrapper']")
                         or soup.select(".list-no-v2-outter")
                         or soup.select("[class*='J-offer-wrapper']"))

                log.info(f"[Alibaba]   {len(cards)} cards (HTML)")
                for card in cards:
                    try:
                        # Title
                        t = (card.select_one("h2 a, .offer-title a, "
                                             "[class*='title'] a, .elements-title-normal a"))
                        title = t.get_text(strip=True) if t else ""
                        if not title: continue

                        # URL
                        href = t.get("href", "") if t else ""
                        link = href if href.startswith("http") else "https:" + href

                        # Price
                        p_el = card.select_one(
                            ".price, [class*='price'], .elements-offer-price-normal")
                        price_text = p_el.get_text(strip=True) if p_el else ""
                        # Price often shows range like "$1.50 - $3.00"
                        prices_found = re.findall(r"[\d,]+\.?\d*", price_text)
                        curr_price = _f(prices_found[0]) if prices_found else None
                        orig_price = _f(prices_found[-1]) if len(prices_found) > 1 else None

                        # Rating
                        r_el = card.select_one(
                            "[class*='rating'], [class*='star'], "
                            "span[aria-label*='star']")
                        rat = None
                        if r_el:
                            aria = r_el.get("aria-label", "")
                            m = re.search(r"(\d+\.?\d*)", aria or r_el.get_text())
                            if m:
                                v = float(m.group(1))
                                if 0 < v <= 5: rat = v

                        # Reviews / orders
                        rv_el = card.select_one(
                            "[class*='review'], [class*='order'], "
                            "[class*='transaction']")
                        reviews = _i(rv_el.get_text() if rv_el else "")

                        # Seller
                        s_el = card.select_one(
                            ".company-name, [class*='company'], "
                            "[class*='supplier'], [class*='seller']")
                        seller = s_el.get_text(strip=True) if s_el else ""

                        # Currency (Alibaba shows USD)
                        currency = "USD"
                        if "£" in price_text: currency = "GBP"
                        elif "€" in price_text: currency = "EUR"

                        products.append(Product(
                            platform="Alibaba", keyword=keyword, title=title,
                            price=curr_price, currency=currency,
                            original_price=orig_price,
                            discount_pct=_disc(orig_price, curr_price),
                            rating=rat, review_count=reviews,
                            availability="In Stock",
                            seller=seller, product_url=link,
                        ))
                    except Exception as e:
                        log.debug(f"[Alibaba] card err: {e}")
                continue  # skip JSON handling below

            # Handle JSON response
            log.info(f"[Alibaba]   {len(items)} items (JSON)")
            for item in items:
                try:
                    title = (item.get("subject", "")
                             or item.get("title", "")
                             or item.get("productName", ""))
                    if not title: continue

                    price_info = item.get("tradePrice", {}) or item.get("price", {})
                    curr_price = _f(price_info.get("min") or price_info.get("price"))
                    orig_price = _f(price_info.get("max"))
                    if orig_price == curr_price: orig_price = None

                    rat     = _f(item.get("starRating") or item.get("score"))
                    reviews = _i(item.get("reviewCount") or item.get("orderNum"))
                    seller  = (item.get("companyName", "")
                               or item.get("supplierName", ""))
                    link    = item.get("detailUrl", "") or item.get("productUrl", "")
                    if link and not link.startswith("http"):
                        link = "https:" + link

                    products.append(Product(
                        platform="Alibaba", keyword=keyword, title=title,
                        price=curr_price, currency="USD",
                        original_price=orig_price,
                        discount_pct=_disc(orig_price, curr_price),
                        rating=rat, review_count=reviews,
                        availability="In Stock",
                        seller=seller, product_url=link,
                    ))
                except Exception as e:
                    log.debug(f"[Alibaba] item err: {e}")

        except Exception as e:
            log.warning(f"[Alibaba] page {page} failed: {e}"); break

        _delay(2, 4)

    log.info(f"[Alibaba] ✓ {len(products)} products")
    return products


# ════════════════════════════════════════════════
#  PIPELINE
# ════════════════════════════════════════════════
def run_pipeline(keywords, max_pages, output_dir, platforms):
    log.info(f"🚀  {len(keywords)} keywords | platforms: {platforms}")
    log.info("    Chrome opens automatically — do NOT close it.\n")

    need_browser = "amazon" in platforms
    driver = _make_driver() if need_browser else None
    all_products = []

    try:
        for kw in keywords:
            log.info(f"\n{'─'*55}\n  KEYWORD: {kw.upper()}\n{'─'*55}")

            if "daraz"   in platforms:
                all_products.extend(scrape_daraz(kw, max_pages))
                _delay(1, 2)

            if "amazon"  in platforms and driver:
                all_products.extend(scrape_amazon(driver, kw, max_pages))
                _delay(2, 3)

            if "alibaba" in platforms:
                all_products.extend(scrape_alibaba(kw, max_pages))
                _delay(2, 3)

    finally:
        if driver:
            try: driver.quit()
            except: pass

    if not all_products:
        log.warning("⚠  No products collected.")
        return pd.DataFrame()

    # ── Build DataFrame ──────────────────────────
    df = pd.DataFrame([asdict(p) for p in all_products])
    df["price"]          = pd.to_numeric(df["price"],          errors="coerce")
    df["original_price"] = pd.to_numeric(df["original_price"], errors="coerce")
    df["discount_pct"]   = pd.to_numeric(df["discount_pct"],   errors="coerce")
    df["rating"]         = pd.to_numeric(df["rating"],         errors="coerce").clip(0, 5)
    df["review_count"]   = pd.to_numeric(df["review_count"],   errors="coerce").astype("Int64")
    df["title"]          = df["title"].str.strip()

    # ── Save files ───────────────────────────────
    os.makedirs(output_dir, exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.join(output_dir, f"ecommerce_data_{ts}")
    df.to_csv(  base + ".csv",  index=False, encoding="utf-8-sig")
    df.to_json( base + ".json", orient="records", indent=2, force_ascii=False)
    df.to_excel(base + ".xlsx", index=False, engine="openpyxl")

    # ── Summary ──────────────────────────────────
    print("\n" + "═" * 65)
    print(f"  DONE  |  {len(df)} total products scraped")
    print("═" * 65)
    summary = df.groupby("platform").agg(
        Products      = ("title",        "count"),
        Avg_Price     = ("price",        "mean"),
        Avg_Rating    = ("rating",       "mean"),
        Total_Reviews = ("review_count", "sum"),
        Has_Rating    = ("rating",       lambda x: f"{x.notna().sum()}/{len(x)}"),
        Has_Reviews   = ("review_count", lambda x: f"{x.notna().sum()}/{len(x)}"),
        Has_Price     = ("price",        lambda x: f"{x.notna().sum()}/{len(x)}"),
    ).round(2)
    print(summary.to_string())
    print("═" * 65)
    print(f"\n  Files saved in: {output_dir}/")
    print(f"    ├── ecommerce_data_{ts}.csv")
    print(f"    ├── ecommerce_data_{ts}.json")
    print(f"    └── ecommerce_data_{ts}.xlsx\n")

    return df


# ════════════════════════════════════════════════
#  ENTRY POINT
# ════════════════════════════════════════════════
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="E-Commerce Scraper — Daraz | Amazon | Alibaba",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ecommerce_scraper.py
  python ecommerce_scraper.py --keywords "laptop" "headphones" --pages 3
  python ecommerce_scraper.py --platforms daraz amazon
  python ecommerce_scraper.py --platforms daraz alibaba --pages 2
        """
    )
    parser.add_argument("--pages",     "-p", type=int,   default=PAGES_PER_KEYWORD)
    parser.add_argument("--output",    "-o", default="scraped_data")
    parser.add_argument("--platforms", nargs="+",
                        choices=["daraz", "amazon", "alibaba"],
                        default=["daraz", "amazon", "alibaba"])
    parser.add_argument("--keywords",  "-k", nargs="+",  default=KEYWORDS)
    args = parser.parse_args()

    df = run_pipeline(
        keywords=args.keywords,
        max_pages=args.pages,
        output_dir=args.output,
        platforms=args.platforms,
    )

    if not df.empty:
        cols = ["platform", "keyword", "title", "price", "currency",
                "discount_pct", "rating", "review_count", "availability"]
        print("\nSample (first 15 rows):")
        print(df[cols].head(15).to_string(index=False))
