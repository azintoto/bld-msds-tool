"""
Multi-vendor Reagent MSDS 정보 추출기

검색 순서: Alfa Aesar → TCI → Sigma-Aldrich → BLD Pharm
- 시약이 발견된 첫 번째 공급업체에서 정보를 반환합니다.
- 단, 1g 가격이 $300 이상이면 다음 공급업체도 검색하여 결과를 함께 표시합니다.

출력 컬럼:
  공급업체 | Product Name | Catalog Number | CAS Number
  성상 | 보관 조건 | 가격 (용량/$)
"""

import math
import re
import time
import base64
import json
from io import BytesIO
from urllib.parse import quote

import pandas as pd
import pdfplumber
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="시약 정보 추출기 (Multi-Vendor)",
    page_icon="🧪",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PRICE_THRESHOLD_1G = 300.0  # USD — 1g 가격이 이 이상이면 다음 공급업체도 검색

# ---------------------------------------------------------------------------
# Common utilities
# ---------------------------------------------------------------------------

def make_session(warm_up_url: str | None = None) -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/webp,*/*;q=0.8"
            ),
        }
    )
    if warm_up_url:
        try:
            s.get(warm_up_url, timeout=10)
        except Exception:
            pass
    return s


def _make_base_result(vendor: str, cas: str) -> dict:
    return {
        "vendor": vendor,
        "input_cas": cas,
        "product_name": "",
        "catalog_number": "",
        "cas_number": cas,
        "appearance": "",
        "storage": "",
        "prices": "",
        "status": "제품 없음",
    }


def _clean_html(text: str) -> str:
    """Strip HTML tags and decode basic entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = (
        text.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&nbsp;", " ")
        .replace("&#176;", "°")
        .replace("&deg;", "°")
    )
    return re.sub(r"\s+", " ", text).strip()


def _dedupe_chars(text: str) -> str:
    """BLD Pharm PDFs sometimes duplicate characters; collapse them."""
    result: list[str] = []
    i = 0
    while i < len(text):
        c = text[i]
        if i + 1 < len(text) and text[i + 1] == c and c not in " \n\t":
            result.append(c)
            i += 2
        else:
            result.append(c)
            i += 1
    return "".join(result)


def _clean_text(raw: str) -> str:
    deduped = _dedupe_chars(raw)
    lines = [re.sub(r"[ \t]{2,}", " ", ln).strip() for ln in deduped.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def _classify_storage(raw: str) -> str:
    """Classify storage text into 상온 / 냉장 / 냉동."""
    if not raw:
        return raw
    t = raw.lower()

    freeze_patterns = [
        r"-\s*20\s*[°℃c]", r"-\s*80\s*[°℃c]", r"-\s*70\s*[°℃c]",
        r"below\s*0", r"freezer", r"frozen", r"\bfreeze\b",
    ]
    is_freeze = any(re.search(p, t) for p in freeze_patterns)

    fridge_patterns = [
        r"2\s*[-~]\s*8\s*[°℃c]", r"4\s*[°℃c]", r"0\s*[-~]\s*5\s*[°℃c]",
        r"refrigerat", r"\bcool\b", r"\bcold\b", r"cold.chain",
    ]
    is_fridge = any(re.search(p, t) for p in fridge_patterns)

    if is_freeze and is_fridge:
        return raw
    if is_freeze:
        return "냉동"
    if is_fridge:
        return "냉장"

    room_patterns = [
        r"room\s*temp", r"ambient", r"15\s*[-~]\s*25\s*[°℃c]",
        r"25\s*[°℃c]", r"\bdry\b",
    ]
    if any(re.search(p, t) for p in room_patterns):
        return "상온"

    return raw


def _extract_sds_fields(raw_text: str) -> dict:
    """
    Parse an SDS PDF text string (any GHS-compliant vendor) and return:
      product_name, catalog_number, cas_number, appearance, storage
    """
    text = _clean_text(raw_text)
    out = {k: "" for k in ("product_name", "catalog_number", "cas_number",
                            "appearance", "storage")}

    # Product name
    for pat in [
        r"Product Description:\s*(.+?)(?:\nCat No\.|\nUFI:|\nCAS-No|\nSynonym)",
        r"Product name[:\s]+(.+?)(?:\nCatalog|\nCAS|\nSynonym|\nCompany)",
        r"Substance name[:\s]+(.+?)(?:\n[A-Z])",
    ]:
        m = re.search(pat, text, re.DOTALL | re.IGNORECASE)
        if m:
            out["product_name"] = " ".join(m.group(1).split())
            break

    # Catalog / Product number
    for pat in [
        r"Cat No\.\s*:\s*(\S+)",
        r"Catalog(?:ue)? [Nn]umber[:\s]+(\S+)",
        r"Product [Nn]umber[:\s]+(\S+)",
        r"Cat\.?\s*No\.?\s*[:\s]+(\S+)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            out["catalog_number"] = m.group(1).strip()
            break

    # CAS number
    for pat in [
        r"CAS-No\.?\s+([\d][\d-]+)",
        r"CAS\s+[Nn]o\.?\s*[:\s]+([\d][\d-]+)",
        r"CAS\s*#?\s*[:\s]+([\d][\d-]+)",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            out["cas_number"] = m.group(1).strip()
            break

    # Appearance / Physical state
    for pat in [
        r"Physical state\s*\n(.+?)\n(?:Colour|Color|Odour|Odor|Form)",
        r"Physical state\s+([^\n]+)",
        r"Form\s*:\s*([^\n]+)",
        r"Colour\s+([^\n]+)",
        r"Color\s+([^\n]+)",
        r"Appearance\s*[:\s]+([^\n<]{3,80})",
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val.lower() not in ("no data available", "n/a", "", "not determined"):
                out["appearance"] = val
                break

    # Storage conditions
    for pat in [
        r"Conditions for safe storage[^\n]*\n(.+?)(?=\n7\.3\b|\nSpecific end use)",
        r"Storage[^\n]*conditions[^\n]*[:\s]+([^\n]{5,150})",
        r"Recommended storage[^\n]*[:\s]+([^\n]{5,100})",
        r"Storage temperature[^\n]*[:\s]+([^\n]{3,60})",
    ]:
        m = re.search(pat, text, re.IGNORECASE | re.DOTALL)
        if m:
            raw_storage = " ".join(m.group(1).split())
            if raw_storage:
                out["storage"] = _classify_storage(raw_storage)
                break

    return out


def _download_and_parse_pdf(url: str, session: requests.Session) -> dict | None:
    """Download a SDS PDF and extract fields. Returns None on any failure."""
    try:
        r = session.get(url, timeout=30)
        if r.status_code != 200:
            return None
        content_type = r.headers.get("content-type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            return None
        with pdfplumber.open(BytesIO(r.content)) as pdf:
            raw = "\n".join(page.extract_text() or "" for page in pdf.pages)
        return _extract_sds_fields(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Price utilities
# ---------------------------------------------------------------------------

def _parse_price_entries(prices_str: str) -> list:
    """
    Parse price string like "100mg/$45, 1g/$230, 5g/$850".
    Returns list of (qty_in_grams, size_label, price_usd).
    """
    entries = []
    for m in re.finditer(r"([\d.]+)\s*(mg|g|kg)/\$([\d,.]+)", prices_str, re.IGNORECASE):
        try:
            qty = float(m.group(1))
            unit = m.group(2).lower()
            price = float(m.group(3).replace(",", ""))
            if unit == "mg":
                qty_g = qty / 1000.0
            elif unit == "kg":
                qty_g = qty * 1000.0
            else:
                qty_g = qty
            if qty_g > 0:
                entries.append((qty_g, f"{m.group(1)}{m.group(2)}", price))
        except Exception:
            pass
    return sorted(entries)


def get_effective_1g_price(prices_str: str):
    """
    Returns the effective cost to acquire 1 g of reagent.
    - Uses 1 g pack price if listed.
    - Otherwise estimates per-gram from the smallest listed pack.
    Returns None if no gram-based pricing found.
    """
    entries = _parse_price_entries(prices_str)
    if not entries:
        return None
    # Exact 1g entry
    for qty_g, _label, price in entries:
        if abs(qty_g - 1.0) < 0.01:
            return price
    # Estimate per-gram from smallest pack
    min_qty_g, _label, min_price = entries[0]
    return min_price / min_qty_g


def exceeds_price_threshold(prices_str: str) -> bool:
    """Returns True when effective 1g price >= PRICE_THRESHOLD_1G."""
    price = get_effective_1g_price(prices_str)
    if price is None:
        return False
    return price >= PRICE_THRESHOLD_1G


# ---------------------------------------------------------------------------
# Shared price-parsing helper (HTML)
# ---------------------------------------------------------------------------

def _extract_gram_prices_from_html(html: str) -> str:
    """
    Generic helper: scan HTML for gram-based price patterns.
    Returns formatted price string, empty string if nothing found.
    """
    entries = []

    # Pattern A: "1g/$11"
    for m in re.finditer(r"([\d.]+)\s*(mg|g|kg)/\$([\d,.]+)", html, re.IGNORECASE):
        try:
            qty = float(m.group(1))
            unit = m.group(2).lower()
            price = float(m.group(3).replace(",", ""))
            qty_g = qty / 1000 if unit == "mg" else (qty * 1000 if unit == "kg" else qty)
            entries.append((qty_g, f"{m.group(1)}{m.group(2)}", price))
        except Exception:
            pass

    # Pattern B: "1 g ... $11.50" within ~80 chars
    for m in re.finditer(r"([\d.]+)\s+(mg|g|kg)\b(.{0,80}?)\$([\d,.]+)", html, re.IGNORECASE):
        try:
            qty = float(m.group(1))
            unit = m.group(2).lower()
            price = float(m.group(4).replace(",", ""))
            qty_g = qty / 1000 if unit == "mg" else (qty * 1000 if unit == "kg" else qty)
            entries.append((qty_g, f"{m.group(1)}{m.group(2)}", price))
        except Exception:
            pass

    if not entries:
        return ""
    seen: set = set()
    unique = []
    for qty_g, label, price in sorted(entries):
        if label not in seen:
            seen.add(label)
            unique.append((qty_g, label, price))
    return ", ".join(f"{lb}/${pr:.0f}" for _, lb, pr in unique[:8])


# ===========================================================================
# VENDOR: BLD Pharm
# ===========================================================================

def _bld_try_product_page(url: str, session: requests.Session):
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200:
            return None
        html = r.text
        bd_m = re.search(r'id="nowBD"\s+value="([^"]+)"', html)
        if not bd_m:
            return None
        sds_m = re.search(r'id="sdsstatus"\s+value="([^"]+)"', html)
        proid_m = re.search(r'id="proid"\s+value="([^"]+)"', html)
        name_m = re.search(r'<h1[^>]*>\s*([^<]{3,300})\s*</h1>', html)
        return {
            "bd_number": bd_m.group(1).strip(),
            "sds_available": bool(sds_m and sds_m.group(1).lower() == "true"),
            "product_name": name_m.group(1).strip() if name_m else None,
            "proid": proid_m.group(1).strip() if proid_m else None,
        }
    except Exception:
        return None


def _bld_search_api(cas: str, session: requests.Session):
    try:
        xsrf = session.cookies.get("_xsrf", "")
        params_b64 = base64.b64encode(
            json.dumps({"keyword": cas, "pageindex": 1, "country": ""}).encode()
        ).decode()
        api_url = (
            "https://www.bldpharm.com/webapi/v1/productlistbykeyword"
            f"?params={params_b64}&_xsrf={xsrf}"
        )
        r = session.get(api_url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            products = data.get("results", [])
            if products:
                s_url = products[0].get("s_url", "")
                if s_url:
                    return f"https://www.bldpharm.com/products/{s_url}"
    except Exception:
        pass
    return None


def _bld_get_price_info(proid: str, session: requests.Session) -> str:
    try:
        xsrf = session.cookies.get("_xsrf", "")
        encoded = base64.b64encode(json.dumps({}).encode()).decode()
        num = hex(int(time.time()))[2:] + "x"
        url = (
            f"https://www.bldpharm.com/webapi/v1/product/productPriceInfo"
            f"/{proid}?num={num}"
        )
        r = session.get(url, params={"params": encoded, "_xsrf": xsrf}, timeout=15)
        if r.status_code != 200:
            return ""
        data = r.json()
        value = data.get("value", {})
        entries = []
        seen_sizes: set = set()
        for key, items in value.items():
            if key == "proInfo" or not isinstance(items, list):
                continue
            for item in items:
                size = item.get("pr_size", "").strip()
                usd = item.get("price_dict", {}).get("pr_usd", 0)
                if size and usd and usd > 0 and size not in seen_sizes:
                    entries.append((size, usd))
                    seen_sizes.add(size)
        if not entries:
            return ""

        def _sort_key(e):
            m = re.match(r"([\d.]+)", e[0])
            return float(m.group(1)) if m else 0

        entries.sort(key=_sort_key)
        return ", ".join(f"{sz}/${usd:.0f}" for sz, usd in entries)
    except Exception:
        return ""


def _bld_build_sds_url(bd_number: str, region: str = "bldsds-ger") -> str:
    digits = re.sub(r"[^0-9]", "", bd_number)
    fileak = math.ceil(int(digits) / 1000)
    return (
        f"https://file.bldpharm.com/static/upload/prosds"
        f"/{region}/{fileak}/SDS-{bd_number}.pdf"
    )


def search_bld_pharm(cas: str, session: requests.Session) -> dict:
    result = _make_base_result("BLD Pharm", cas)
    try:
        session.get("https://www.bldpharm.com/", timeout=10)
    except Exception:
        pass

    info = _bld_try_product_page(
        f"https://www.bldpharm.com/products/{cas}.html", session
    )
    if not info:
        alt_url = _bld_search_api(cas, session)
        if alt_url:
            info = _bld_try_product_page(alt_url, session)
    if not info or not info.get("bd_number"):
        return result

    prices = ""
    if info.get("proid"):
        prices = _bld_get_price_info(info["proid"], session)

    result.update(
        {
            "product_name": info.get("product_name") or "",
            "catalog_number": info["bd_number"],
            "prices": prices,
            "status": "SDS 없음",
        }
    )

    if not info.get("sds_available"):
        return result

    fields = _download_and_parse_pdf(_bld_build_sds_url(info["bd_number"]), session)
    if fields is None:
        result["status"] = "SDS 파싱 실패"
        return result

    result.update(
        {
            "product_name": fields["product_name"] or result["product_name"],
            "catalog_number": fields["catalog_number"] or result["catalog_number"],
            "cas_number": fields["cas_number"] or cas,
            "appearance": fields["appearance"],
            "storage": fields["storage"],
            "status": "성공",
        }
    )
    return result


# ===========================================================================
# VENDOR: Alfa Aesar  (via ThermoFisher typeahead — Alfa Aesar products now
#                      on thermofisher.com after acquisition)
# ===========================================================================

def search_alfa_aesar(cas: str, session: requests.Session) -> dict:
    """Search via chemicals.thermofisher.kr (Alfa Aesar KR chemicals portal)."""
    result = _make_base_result("Alfa Aesar", cas)
    try:
        # POST to the internal Next.js search API
        r = session.post(
            "https://chemicals.thermofisher.kr/apac/api/search/catalog/keyword",
            json={
                "countryCode": "kr",
                "language": "ko",
                "filter": "",
                "pageNo": 1,
                "pageSize": 10,
                "persona": "catalog",
                "query": cas,
            },
            headers={
                "Content-Type": "application/json",
                "Referer": "https://chemicals.thermofisher.kr/kr/ko/home.html",
            },
            timeout=15,
        )
        if r.status_code != 200:
            return result

        data = r.json()
        if data.get("code") != "200":
            return result

        items = data.get("data", {}).get("catalogResultDTOs", [])
        if not items:
            return result

        # Prefer items whose cas field exactly matches the query
        cas_items = [i for i in items if i.get("cas") == cas]
        product = (cas_items or items)[0]

        product_name = _clean_html(product.get("catalogName") or "")
        prod_code = (
            product.get("childCatalogNumber")
            or product.get("rootCatalogNumber")
            or ""
        )
        cas_found = product.get("cas") or cas

        if not product_name:
            return result

        result.update(
            {
                "product_name": product_name,
                "catalog_number": prod_code or cas,
                "cas_number": cas_found,
                "status": "성공",
            }
        )
    except Exception:
        result["status"] = "오류"

    return result

# ===========================================================================
# VENDOR: TCI Chemicals  (via sejinci.co.kr — Korean TCI distributor)
# ===========================================================================


def _sejinci_parse_prices(html: str) -> str:
    """Parse sejinci.co.kr opt_lst table for KRW pricing."""
    entries = []
    # Each row has data-th="포장단위" for size and data-th="단가(원)" for unit price
    for row in re.findall(r"<tr[^>]*class=\"m_chk\"[^>]*>.*?</tr>", html, re.DOTALL | re.IGNORECASE):
        size_m = re.search(r'data-th="포장단위"[^>]*>\s*<div[^>]*>\s*([^<\s][^<]*?)\s*</div>', row, re.IGNORECASE | re.DOTALL)
        price_m = re.search(r'data-th="단가\(원\)"[^>]*>\s*<div[^>]*>\s*([\d,]+)\s*</div>', row, re.IGNORECASE | re.DOTALL)
        if size_m and price_m:
            size_label = size_m.group(1).strip()
            price_str = price_m.group(1).replace(",", "").strip()
            if size_label and price_str:
                entries.append((size_label, int(price_str)))
    return ", ".join(f"{sz}/₩{pr:,}" for sz, pr in entries[:8])


def search_tci(cas: str, session: requests.Session) -> dict:
    result = _make_base_result("TCI", cas)
    try:
        # 1. Search via sejinci.co.kr (Korean TCI distributor — server-renders results)
        r = session.get(
            f"https://www.sejinci.co.kr/productsearch/?keyword={quote(cas)}",
            timeout=15,
        )
        if r.status_code != 200:
            return result

        html = r.text

        # Not found indicator
        if "해당하는 제품이 검색되지 않았습니다" in html or "검색 결과가 없습니다" in html:
            return result

        # 2. Parse product info from server-rendered HTML
        product_name = ""
        m = re.search(r'<p class="name">\s*([^<]+)\s*</p>', html, re.IGNORECASE)
        if m:
            product_name = _clean_html(m.group(1)).strip()

        if not product_name:
            return result

        prod_code = ""
        m = re.search(r'<th>\s*제품번호\s*</th>\s*<td[^>]*>(?:<[^>]+>)?([A-Z]\d{4,5})(?:</[^>]+>)?</td>',
                      html, re.IGNORECASE | re.DOTALL)
        if not m:
            m = re.search(r'<th>\s*제품번호\s*</th>\s*<td[^>]*>\s*([A-Z]\d{4,5})\s*</td>',
                          html, re.IGNORECASE | re.DOTALL)
        if m:
            prod_code = m.group(1).strip()

        cas_found = ""
        m = re.search(r'<th>\s*CAS\s*NO\s*</th>\s*<td[^>]*>\s*([^<\s][^<]*?)\s*</td>',
                      html, re.IGNORECASE | re.DOTALL)
        if m:
            cas_found = _clean_html(m.group(1)).strip()

        # Purity used as proxy for appearance when no SDS is available
        purity = ""
        m = re.search(r'<th>\s*순도/시험방법\s*</th>\s*<td[^>]*>\s*([^<]+)\s*</td>',
                      html, re.IGNORECASE | re.DOTALL)
        if m:
            purity = _clean_html(m.group(1)).strip()

        prices = _sejinci_parse_prices(html)
        appearance = purity  # fallback; replaced by SDS data if available
        storage = ""

        # 3. SDS PDF (tcichemicals.com KR — may work from Streamlit Cloud)
        if prod_code:
            sds_url = (
                f"https://www.tcichemicals.com/KR/ko/documentSearch/productSDSSearchDoc"
                f"?productCode={prod_code}&langSelector=ko&selectedCountry=KR"
            )
            fields = _download_and_parse_pdf(sds_url, session)
            if fields:
                if fields.get("appearance"):
                    appearance = fields["appearance"]
                if fields.get("storage"):
                    storage = fields["storage"]
                if fields.get("product_name") and not product_name:
                    product_name = fields["product_name"]

        result.update(
            {
                "product_name": product_name,
                "catalog_number": prod_code or cas,
                "cas_number": cas_found or cas,
                "appearance": appearance,
                "storage": storage,
                "prices": prices,
                "status": "성공" if product_name else "SDS 없음",
            }
        )
    except Exception:
        result["status"] = "오류"

    return result


# ===========================================================================
# VENDOR: Sigma-Aldrich
# ===========================================================================

def _sigma_extract_next_data(html: str) -> dict:
    m = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(\{.+?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            pass
    return {}


def _sigma_get_prices(product_number: str, brand: str, session: requests.Session) -> str:
    entries = []
    try:
        url = (
            f"https://www.sigmaaldrich.com/api/2022/pricing/products"
            f"?productNumber={product_number}&brand={brand}"
            f"&region=US&currency=USD"
        )
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            data = r.json()
            skus = data if isinstance(data, list) else data.get("skus", [])
            for sku in skus:
                desc = sku.get("description", "") or sku.get("packSize", "")
                price_val = (
                    sku.get("price")
                    or sku.get("listPrice")
                    or sku.get("unitPrice")
                    or 0
                )
                if desc and price_val:
                    gm = re.match(r"([\d.]+)\s*(mg|g|kg)", str(desc), re.IGNORECASE)
                    if gm:
                        qty = float(gm.group(1))
                        unit = gm.group(2).lower()
                        price_f = float(str(price_val).replace(",", ""))
                        qty_g = (
                            qty / 1000 if unit == "mg" else
                            (qty * 1000 if unit == "kg" else qty)
                        )
                        entries.append((qty_g, f"{gm.group(1)}{gm.group(2)}", price_f))
    except Exception:
        pass

    if not entries:
        return ""

    seen: set = set()
    unique = []
    for qty_g, label, price in sorted(entries):
        if label not in seen:
            seen.add(label)
            unique.append((qty_g, label, price))
    return ", ".join(f"{lb}/${pr:.0f}" for _, lb, pr in unique[:8])


def _sigma_specs_from_html(html: str):
    appearance = ""
    storage = ""

    next_data = _sigma_extract_next_data(html)
    if next_data:
        try:
            props = next_data.get("props", {}).get("pageProps", {})
            product = props.get("product") or props.get("productDetails") or {}
            if isinstance(product, dict):
                for attr in product.get("attributes", []):
                    label = str(attr.get("label", "")).lower()
                    value = str(attr.get("value", ""))
                    if any(k in label for k in ("appearance", "physical state", "form")):
                        appearance = appearance or value
                    elif "storage" in label:
                        storage = storage or _classify_storage(value)
        except Exception:
            pass

    if not appearance:
        for pat in [
            r"Physical state[^:]*:\s*([^\n<]{3,80})",
            r"Appearance[^:]*:\s*([^\n<]{3,80})",
            r'"physicalState"\s*:\s*"([^"]{3,80})"',
            r'"appearance"\s*:\s*"([^"]{3,80})"',
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                val = _clean_html(m.group(1)).strip()
                if val.lower() not in ("n/a", "no data", ""):
                    appearance = val
                    break

    if not storage:
        for pat in [
            r"Storage[^T\n:]*:\s*([^\n<]{5,120})",
            r'"storageInformation"\s*:\s*"([^"]{5,120})"',
            r'"storageTemp"\s*:\s*"([^"]{3,60})"',
        ]:
            m = re.search(pat, html, re.IGNORECASE)
            if m:
                val = _clean_html(m.group(1)).strip()
                if val:
                    storage = _classify_storage(val)
                    break

    return appearance, storage


def search_sigma_aldrich(cas: str, session: requests.Session) -> dict:
    result = _make_base_result("Sigma-Aldrich", cas)
    try:
        # 1. Warm up for Akamai locale cookies (non-fatal if geo-blocked)
        try:
            session.get("https://www.sigmaaldrich.com/US/en/", timeout=10)
        except Exception:
            pass

        # 2. GraphQL POST search by CAS number
        product_info: dict = {}
        gql_query = (
            '{ getProductSearchResults(input: {searchTerm: "'
            + cas.replace('"', "")
            + '", type: CAS_NUMBER}) { items { ... on Product {'
            + " productNumber brand { key name } casNumber"
            + " } } } }"
        )
        r = session.post(
            "https://www.sigmaaldrich.com/api/2022/products/search",
            json={"query": gql_query},
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            timeout=15,
        )
        if r.status_code == 200:
            try:
                data = r.json()
                items = (
                    data.get("data", {})
                    .get("getProductSearchResults", {})
                    .get("items", [])
                )
                if isinstance(items, list) and items:
                    product_info = items[0]
            except Exception:
                pass

        if not product_info:
            return result

        # 3. Identifiers
        product_number = (
            product_info.get("productNumber")
            or product_info.get("product_number")
            or product_info.get("catalogNumber")
            or product_info.get("sku")
            or ""
        )
        product_name = (
            product_info.get("name")
            or product_info.get("title")
            or ""
        )
        brand_raw = product_info.get("brand", "sigma")
        brand = (
            brand_raw.get("key", "sigma") if isinstance(brand_raw, dict) else str(brand_raw)
        ).lower()

        if not product_number:
            return result

        # 4. Pricing
        prices = _sigma_get_prices(product_number, brand, session)

        # 5. Product page
        appearance = ""
        storage = ""
        prod_url = (
            f"https://www.sigmaaldrich.com/US/en/product/{brand}/{product_number}"
        ).lower()
        r3 = session.get(prod_url, timeout=15)
        if r3.status_code == 200:
            appearance, storage = _sigma_specs_from_html(r3.text)
            if not prices:
                prices = _extract_gram_prices_from_html(r3.text)
            if not product_name:
                m = re.search(r"<h1[^>]*>([^<]{5,200})</h1>", r3.text)
                if m:
                    product_name = _clean_html(m.group(1)).strip()

        # 6. SDS PDF
        sds_fields = _download_and_parse_pdf(
            f"https://www.sigmaaldrich.com/US/en/sds/{brand}/{product_number}",
            session,
        )
        if sds_fields:
            if sds_fields.get("appearance"):
                appearance = sds_fields["appearance"]
            if sds_fields.get("storage"):
                storage = sds_fields["storage"]
            if sds_fields.get("product_name") and not product_name:
                product_name = sds_fields["product_name"]

        result.update(
            {
                "product_name": _clean_html(product_name),
                "catalog_number": product_number.upper(),
                "appearance": appearance,
                "storage": storage,
                "prices": prices,
                "status": "성공" if product_number else "SDS 없음",
            }
        )
    except Exception:
        result["status"] = "오류"

    return result


# ===========================================================================
# Multi-vendor pipeline
# ===========================================================================

_VENDOR_PIPELINE = [
    ("Alfa Aesar",    search_alfa_aesar),
    ("TCI",           search_tci),
    ("Sigma-Aldrich", search_sigma_aldrich),
    ("BLD Pharm",     search_bld_pharm),
]

# Statuses that mean "product was found" (even if some info is incomplete)
_FOUND_STATUSES = {"성공", "SDS 없음", "SDS 파싱 실패"}


def process_cas(cas: str, session: requests.Session) -> list:
    """
    Search vendors in the fixed order.

    Rules:
      - Not found / error at a vendor  → skip to next vendor
      - Found, effective 1g price < $300 (or price unknown)  → return [this result]
      - Found, effective 1g price >= $300  → add result, continue to next vendor

    Returns a list of result dicts (usually 1; multiple when price is high).
    """
    results = []

    for _vendor_name, search_fn in _VENDOR_PIPELINE:
        res = search_fn(cas, session)

        if res["status"] not in _FOUND_STATUSES:
            continue  # not found or error → try next vendor

        results.append(res)

        # Stop when price is acceptable or unknown
        if not exceeds_price_threshold(res.get("prices", "")):
            break

        # Price ≥ $300/g → keep going to next vendor

    if not results:
        not_found = _make_base_result("N/A", cas)
        not_found["status"] = "모든 공급업체에서 제품 없음"
        return [not_found]

    return results


# ===========================================================================
# Streamlit UI
# ===========================================================================

st.title("🧪 시약 정보 추출기 (Multi-Vendor)")
st.markdown(
    "CAS 번호를 입력하면 **Alfa Aesar → TCI → Sigma-Aldrich → BLD Pharm** 순서로 "
    "시약 정보를 자동 검색합니다.  \n"
    "시약이 발견된 공급업체에서 바로 결과를 반환하며, "
    "**1g 가격이 \\$300 이상이면** 다음 공급업체도 함께 검색합니다."
)

st.divider()

# ---------------------------------------------------------------------------
# CAS 번호 입력
# ---------------------------------------------------------------------------

st.subheader("CAS 번호 입력")
st.caption(
    "아래 표의 CAS No. 열에 CAS 번호를 입력하세요 (예: 3952-78-1). "
    "행은 자유롭게 추가할 수 있습니다."
)

default_rows = st.number_input(
    "입력할 시약 수", min_value=1, max_value=100, value=5, step=1
)

if "cas_table" not in st.session_state or len(st.session_state.cas_table) != default_rows:
    st.session_state.cas_table = pd.DataFrame({"CAS No.": [""] * int(default_rows)})

edited_df = st.data_editor(
    st.session_state.cas_table,
    use_container_width=True,
    num_rows="dynamic",
    column_config={
        "CAS No.": st.column_config.TextColumn(
            "CAS No.",
            help="CAS 번호를 입력하세요 (예: 3952-78-1)",
        )
    },
    hide_index=True,
    key="cas_editor",
)

cas_re = re.compile(r"^\d{1,7}-\d{2}-\d$")
cas_list = [
    str(v).strip()
    for v in edited_df["CAS No."].fillna("")
    if cas_re.match(str(v).strip())
]

if cas_list:
    st.info(f"유효한 CAS 번호 **{len(cas_list)}**개 인식됨: {', '.join(cas_list)}")
else:
    st.warning("유효한 CAS 번호가 없습니다. 위 표에 CAS 번호를 입력해 주세요.")

st.divider()

if cas_list and st.button("🔍 시약 정보 추출 시작", type="primary"):
    session = make_session()
    all_results = []

    prog_bar = st.progress(0, text="시작 중...")
    status_msg = st.empty()

    for i, cas in enumerate(cas_list):
        status_msg.info(
            f"[{i + 1}/{len(cas_list)}] CAS **{cas}** 처리 중 "
            f"(Alfa Aesar → TCI → Sigma-Aldrich → BLD Pharm 순서 검색)…"
        )
        vendor_results = process_cas(cas, session)
        all_results.extend(vendor_results)

        found = [r["vendor"] for r in vendor_results if r["status"] in _FOUND_STATUSES]
        summary = ", ".join(found) if found else "제품 없음"
        prog_bar.progress(
            (i + 1) / len(cas_list),
            text=f"{i + 1}/{len(cas_list)} — CAS {cas}: {summary}",
        )
        time.sleep(0.3)

    n_ok = sum(1 for r in all_results if r["status"] == "성공")
    status_msg.success(
        f"완료!  성공 {n_ok} / 결과 행 {len(all_results)}개 (CAS {len(cas_list)}개)"
    )

    # ── 결과 테이블 ──────────────────────────────────────────────────
    display_df = pd.DataFrame(
        [
            {
                "공급업체": r["vendor"],
                "CAS No. (입력)": r["input_cas"],
                "제품명": r["product_name"],
                "Cat. No.": r["catalog_number"],
                "CAS No. (확인)": r["cas_number"],
                "성상": r["appearance"],
                "보관 조건": r["storage"],
                "가격 (용량/$)": r["prices"],
                "상태": r["status"],
            }
            for r in all_results
        ]
    )
    st.dataframe(display_df, use_container_width=True)

    # ── Excel 다운로드 ────────────────────────────────────────────────
    header = [
        "공급업체", "CAS No.", "제품명 (Product Name)", "Cat. No.",
        "CAS No. (확인)", "성상 (Appearance)", "보관 조건", "가격 (용량/$)",
    ]
    rows_out = [
        [
            r["vendor"],
            r["input_cas"],
            r["product_name"],
            r["catalog_number"],
            r["cas_number"],
            r["appearance"],
            r["storage"],
            r["prices"],
        ]
        for r in all_results
    ]
    df_out = pd.DataFrame(rows_out, columns=header)

    out_buf = BytesIO()
    with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, header=True)
    out_buf.seek(0)

    st.download_button(
        label="📥 결과 Excel 다운로드",
        data=out_buf.getvalue(),
        file_name="reagent_msds_results.xlsx",
        mime=(
            "application/vnd.openxmlformats-officedocument"
            ".spreadsheetml.sheet"
        ),
    )
