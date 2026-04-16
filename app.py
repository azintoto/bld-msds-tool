"""
BLD Pharm MSDS 정보 추출기
Excel A열의 CAS 번호로 BLD Pharm SDS PDF를 다운로드하고
필요한 정보를 추출하여 B~F열에 저장합니다.

  B: Product Name
  C: Catalog Number
  D: CAS Number
  E: Appearance
  F: Conditions for safe storage
  G: Price by volume (e.g. 1g/$11, 5g/$31)
"""

import math
import re
import time
import base64
import json
from io import BytesIO

import pandas as pd
import pdfplumber
import requests
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="BLD Pharm MSDS 추출기",
    page_icon="🧪",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Helper: HTTP session
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/webp,*/*;q=0.8"
            ),
        }
    )
    # Warm-up request to acquire cookies (e.g. _xsrf)
    try:
        s.get("https://www.bldpharm.com/", timeout=10)
    except Exception:
        pass
    return s


# ---------------------------------------------------------------------------
# Helper: product page → BD catalog number + SDS status
# ---------------------------------------------------------------------------

def _try_get_product_page(url: str, session: requests.Session) -> dict | None:
    """Return parsed product info dict, or None on failure."""
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
        # Try to extract product common name from the page <h1>
        name_m = re.search(r'<h1[^>]*>\s*([^<]{3,300})\s*</h1>', html)
        return {
            "bd_number": bd_m.group(1).strip(),
            "sds_available": bool(sds_m and sds_m.group(1).lower() == "true"),
            "product_name": name_m.group(1).strip() if name_m else None,
            "proid": proid_m.group(1).strip() if proid_m else None,
        }
    except Exception:
        return None


def _search_api_url(cas: str, session: requests.Session) -> str | None:
    """
    Use BLD Pharm's search API as a fallback when the direct product URL fails.
    Returns a product page URL string, or None.
    """
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


def get_product_info(cas: str, session: requests.Session) -> dict | None:
    """
    Return {bd_number, sds_available, product_name} for a given CAS.
    Tries direct URL first; falls back to search API.
    """
    # 1. Direct URL
    info = _try_get_product_page(
        f"https://www.bldpharm.com/products/{cas}.html", session
    )
    if info:
        return info

    # 2. Search API fallback
    alt_url = _search_api_url(cas, session)
    if alt_url:
        info = _try_get_product_page(alt_url, session)
        if info:
            return info

    return None


# ---------------------------------------------------------------------------
# Helper: price info via productPriceInfo API
# ---------------------------------------------------------------------------

def get_price_info(proid: str, session: requests.Session) -> str:
    """
    Call /webapi/v1/product/productPriceInfo/{proid} and return a formatted
    string of size→USD price entries, e.g. "1g/$11, 5g/$31, 25g/$97".
    Returns an empty string on any failure.
    """
    try:
        xsrf = session.cookies.get("_xsrf", "")
        encoded = base64.b64encode(json.dumps({}).encode()).decode()
        num = hex(int(time.time()))[2:] + "x"
        url = f"https://www.bldpharm.com/webapi/v1/product/productPriceInfo/{proid}?num={num}"
        r = session.get(url, params={"params": encoded, "_xsrf": xsrf}, timeout=15)
        if r.status_code != 200:
            return ""
        data = r.json()
        value = data.get("value", {})
        # Collect all size/price entries across all SKU keys (skip 'proInfo')
        entries: list[tuple[str, float]] = []
        seen_sizes: set[str] = set()
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
        # Sort by numeric weight if possible
        def _sort_key(e):
            m = re.match(r"([\d.]+)", e[0])
            return float(m.group(1)) if m else 0
        entries.sort(key=_sort_key)
        return ", ".join(f"{sz}/${usd:.0f}" for sz, usd in entries)
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Helper: SDS PDF URL
# ---------------------------------------------------------------------------

def build_sds_url(bd_number: str, region: str = "bldsds-ger") -> str:
    """
    Construct the SDS PDF URL from the BLD catalog number.

    Formula (from product-detail.js):
        fileak = ceil(int(digits_only(bd)) / 1000)
        url = f".../prosds/{region}/{fileak}/SDS-{bd}.pdf"
    """
    digits = re.sub(r"[^0-9]", "", bd_number)
    fileak = math.ceil(int(digits) / 1000)
    return (
        f"https://file.bldpharm.com/static/upload/prosds"
        f"/{region}/{fileak}/SDS-{bd_number}.pdf"
    )


# ---------------------------------------------------------------------------
# Helper: PDF text extraction + field parsing
# ---------------------------------------------------------------------------

def _dedupe_chars(text: str) -> str:
    """
    BLD Pharm SDS PDFs from certain font sets render every character twice
    (e.g. 'PPrroodduucctt'). This function collapses consecutive identical
    characters pairs back to a single character.
    """
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
    """Deduplicate characters, collapse inline whitespace, drop blank lines."""
    deduped = _dedupe_chars(raw)
    lines = [re.sub(r"[ \t]{2,}", " ", ln).strip() for ln in deduped.split("\n")]
    return "\n".join(ln for ln in lines if ln)


def extract_sds_fields(raw_text: str) -> dict:
    """
    Parse an SDS PDF text string and return a dict with keys:
      product_name, catalog_number, cas_number, appearance, storage
    """
    text = _clean_text(raw_text)
    out = {k: "" for k in ("product_name", "catalog_number", "cas_number",
                            "appearance", "storage")}

    # --- Product name (Section 1.1) ------------------------------------
    m = re.search(
        r"Product Description:\s*(.+?)(?:\nCat No\.|\nUFI:|\nCAS-No)",
        text,
        re.DOTALL,
    )
    if m:
        out["product_name"] = " ".join(m.group(1).split())

    # --- Catalog number ------------------------------------------------
    m = re.search(r"Cat No\.\s*:\s*(\S+)", text)
    if m:
        out["catalog_number"] = m.group(1).strip()

    # --- CAS number ----------------------------------------------------
    m = re.search(r"CAS-No\s+([\d][\d-]+)", text)
    if m:
        out["cas_number"] = m.group(1).strip()

    # --- Appearance (Section 9 – Physical state) -----------------------
    # Format 1: label on its own line, value on next line, next field below
    m = re.search(
        r"Physical state\s*\n(.+?)\n(?:Colour|Color|Odour|Odor)",
        text,
        re.IGNORECASE,
    )
    if m:
        val = m.group(1).strip()
        if val.lower() not in ("no data available", "n/a", ""):
            out["appearance"] = val

    if not out["appearance"]:
        # Format 2: label and value on the same line
        m = re.search(r"Physical state\s+([^\n]+)", text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val.lower() not in ("no data available", "n/a", ""):
                out["appearance"] = val

    if not out["appearance"]:
        # Format 3: separate Colour field
        m = re.search(r"Colour\s+([^\n]+)", text, re.IGNORECASE)
        if m:
            val = m.group(1).strip()
            if val.lower() not in ("no data available", "n/a", ""):
                out["appearance"] = val

    # --- Storage conditions (Section 7.2) ------------------------------
    m = re.search(
        r"Conditions for safe storage[^\n]*\n(.+?)"
        r"(?=\n7\.3\b|\nSpecific end use)",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    raw_storage = " ".join(m.group(1).split()) if m else ""

    out["storage"] = _classify_storage(raw_storage)

    return out


def _classify_storage(raw: str) -> str:
    """
    Classify storage condition into 상온 / 냉장 / 냉동.
    If ambiguous (both cold categories match), return the original raw text.
    """
    if not raw:
        return raw
    t = raw.lower()

    # ----- 냉동 (-20°C, -80°C, freezer, freeze, below 0) -----
    freeze_patterns = [
        r"-\s*20\s*[°℃c]", r"-\s*80\s*[°℃c]", r"-\s*70\s*[°℃c]",
        r"below\s*0", r"freezer", r"frozen", r"\bfreeze\b",
    ]
    is_freeze = any(re.search(p, t) for p in freeze_patterns)

    # ----- 냉장 (2-8°C, 4°C, refrigerate, cool, cold) -----
    fridge_patterns = [
        r"2\s*[-~]\s*8\s*[°℃c]", r"4\s*[°℃c]", r"0\s*[-~]\s*5\s*[°℃c]",
        r"refrigerat", r"\bcool\b", r"\bcold\b", r"cold.chain",
    ]
    is_fridge = any(re.search(p, t) for p in fridge_patterns)

    # Freeze and fridge both present → explicitly ambiguous
    if is_freeze and is_fridge:
        return raw
    if is_freeze:
        return "냉동"
    if is_fridge:
        return "냉장"

    # ----- 상온 — only checked when no cold signal found -----
    room_patterns = [
        r"room\s*temp", r"ambient", r"15\s*[-~]\s*25\s*[°℃c]",
        r"25\s*[°℃c]", r"\bdry\b",
    ]
    is_room = any(re.search(p, t) for p in room_patterns)
    if is_room:
        return "상온"

    # No recognisable keyword → return original text
    return raw


def download_and_parse_sds(
    bd_number: str, session: requests.Session
) -> dict | None:
    """Download SDS PDF and return extracted fields, or None on failure."""
    sds_url = build_sds_url(bd_number)
    try:
        r = session.get(sds_url, timeout=30)
        r.raise_for_status()
        with pdfplumber.open(BytesIO(r.content)) as pdf:
            raw = "\n".join(page.extract_text() or "" for page in pdf.pages)
        return extract_sds_fields(raw)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main pipeline for a single CAS number
# ---------------------------------------------------------------------------

def process_cas(cas: str, session: requests.Session) -> dict:
    """
    Full extraction pipeline.
    Returns a dict with keys:
      input_cas, product_name, catalog_number, cas_number,
      appearance, storage, prices, status
    """
    base = {
        "input_cas": cas,
        "product_name": "",
        "catalog_number": "",
        "cas_number": "",
        "appearance": "",
        "storage": "",
        "prices": "",
        "status": "",
    }

    info = get_product_info(cas, session)
    if not info or not info.get("bd_number"):
        base["status"] = "제품 없음"
        return base

    # Fetch price info (does not require SDS)
    prices = ""
    if info.get("proid"):
        prices = get_price_info(info["proid"], session)

    base.update(
        {
            "product_name": info.get("product_name") or "",
            "catalog_number": info["bd_number"],
            "cas_number": cas,
            "prices": prices,
            "status": "SDS 없음",
        }
    )

    if not info.get("sds_available"):
        return base

    fields = download_and_parse_sds(info["bd_number"], session)
    if fields is None:
        base["status"] = "SDS 다운로드/파싱 실패"
        return base

    base.update(
        {
            "product_name": fields["product_name"] or base["product_name"],
            "catalog_number": fields["catalog_number"] or base["catalog_number"],
            "cas_number": fields["cas_number"] or cas,
            "appearance": fields["appearance"],
            "storage": fields["storage"],
            "status": "성공",
        }
    )
    return base


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.title("🧪 BLD Pharm MSDS 정보 추출기")
st.markdown(
    "CAS 번호를 아래 표에 직접 입력하면 [bldpharm.com](https://www.bldpharm.com/) "
    "SDS를 자동 검색하여 제품명, 보관조건, 가격 등을 추출합니다."
)

st.divider()

# ---------------------------------------------------------------------------
# CAS 번호 입력 테이블
# ---------------------------------------------------------------------------

st.subheader("CAS 번호 입력")
st.caption("아래 표의 CAS No. 열에 CAS 번호를 입력하세요 (형식 예: 3952-78-1). 행은 자유롭게 추가할 수 있습니다.")

# 기본 행 수 슬라이더
default_rows = st.number_input("입력할 시약 수", min_value=1, max_value=100, value=5, step=1)

# 세션 상태에 편집 테이블 유지
if "cas_table" not in st.session_state or len(st.session_state.cas_table) != default_rows:
    st.session_state.cas_table = pd.DataFrame(
        {"CAS No.": [""] * int(default_rows)}
    )

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

# 유효한 CAS 번호 추출
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

if cas_list and st.button("🔍 MSDS 정보 추출 시작", type="primary"):
    session = make_session()
    results: list[dict] = []

    prog_bar = st.progress(0, text="시작 중...")
    status_msg = st.empty()

    for i, cas in enumerate(cas_list):
        status_msg.info(f"[{i + 1}/{len(cas_list)}] CAS **{cas}** 처리 중...")
        result = process_cas(cas, session)
        results.append(result)
        prog_bar.progress(
            (i + 1) / len(cas_list),
            text=f"{i + 1}/{len(cas_list)} — {cas}: {result['status']}",
        )
        time.sleep(0.4)

    n_ok = sum(1 for r in results if r["status"] == "성공")
    status_msg.success(f"완료! 성공 {n_ok} / 전체 {len(results)}개")

    # ---- 결과 테이블 표시 ----
    display_df = pd.DataFrame(
        [
            {
                "CAS No. (입력)": r["input_cas"],
                "제품명": r["product_name"],
                "Cat. No.": r["catalog_number"],
                "CAS No. (확인)": r["cas_number"],
                "성상": r["appearance"],
                "보관 조건": r["storage"],
                "가격 (용량/$)": r["prices"],
                "상태": r["status"],
            }
            for r in results
        ]
    )
    st.dataframe(display_df, use_container_width=True)

    # ---- 출력 Excel 구성 (헤더 행 포함) ----
    header = ["CAS No.", "제품명 (Product Name)", "Cat. No.", "CAS No. (확인)",
              "성상 (Appearance)", "보관 조건", "가격 (용량/$)"]

    rows = []
    for r in results:
        rows.append([
            r["input_cas"],
            r["product_name"],
            r["catalog_number"],
            r["cas_number"],
            r["appearance"],
            r["storage"],
            r["prices"],
        ])

    df_out = pd.DataFrame(rows, columns=header)

    out_buf = BytesIO()
    with pd.ExcelWriter(out_buf, engine="openpyxl") as writer:
        df_out.to_excel(writer, index=False, header=True)
    out_buf.seek(0)

    st.download_button(
        label="📥 결과 Excel 다운로드",
        data=out_buf.getvalue(),
        file_name="bld_msds_results.xlsx",
        mime=(
            "application/vnd.openxmlformats-officedocument"
            ".spreadsheetml.sheet"
        ),
    )
