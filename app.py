import re
import io
import json
import hashlib
import pandas as pd
import streamlit as st
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

st.set_page_config(page_title="SINTA Live Parser", layout="wide")

# ----------------------------
# Parsing (robust HTML structure)
# ----------------------------
def clean_text(x: str) -> str:
    return re.sub(r"\s+", " ", (x or "")).strip()

def extract_doi(text: str) -> str:
    m = re.search(r"\bDOI\s*:\s*([^\s<]+)", text, flags=re.I)
    doi = m.group(1).strip() if m else ""
    if doi in {"-", "—"} or doi.lower() in {"n/a", "na"}:
        doi = ""
    return doi

def extract_year(text: str) -> str:
    m = re.search(r"\b(19\d{2}|20\d{2})\b", text)
    return m.group(1) if m else ""

def extract_sinta(text: str) -> str:
    m = re.search(r"Accred\s*:\s*Sinta\s*(\d)", text, flags=re.I)
    return m.group(1) if m else ""

def extract_authors_from_meta(meta_text: str) -> str:
    t = clean_text(meta_text)
    t = re.sub(r"Author Order\s*:\s*\d+\s*of\s*\d+\s*", "", t, flags=re.I).strip()

    cut_markers = [r"\b(19\d{2}|20\d{2})\b", r"\bDOI\s*:", r"\bAccred\s*:"]
    cut_pos = len(t)
    for pat in cut_markers:
        m = re.search(pat, t, flags=re.I)
        if m:
            cut_pos = min(cut_pos, m.start())

    authors = clean_text(t[:cut_pos].strip(" -–—|"))
    if not authors:
        return ""

    # sanity check
    if (";" not in authors) and ("," not in authors):
        if not re.match(r"^[A-Za-zÀ-ÖØ-öø-ÿ'’.\- ]+,\s*[A-Za-zÀ-ÖØ-öø-ÿ'’.\- ]+$", authors):
            return ""
    return authors

def title_from_item(item) -> str:
    a = item.find("a", href=re.compile(r"documents/detail/", re.I))
    if a:
        return clean_text(a.get_text(" ", strip=True))

    for a in item.find_all("a", href=True):
        txt = clean_text(a.get_text(" ", strip=True))
        if not txt or len(txt) < 8:
            continue
        low = txt.lower()
        if "author order" in low or "accred" in low or "doi:" in low:
            continue
        if re.search(r"\bvol\b|\bno\b|\bvolume\b", low):
            continue
        return txt
    return ""

def journal_from_item(item) -> str:
    pub = item.find("a", class_=re.compile(r"\bar-pub\b"))
    if pub:
        return clean_text(pub.get_text(" ", strip=True))
    text = item.get_text("\n", strip=True)
    for ln in [x.strip() for x in text.split("\n") if x.strip()]:
        if re.search(r"\bVol\b|\bNo\b|\bVolume\b", ln):
            return clean_text(ln)
    return ""

def year_from_item(item) -> str:
    y = item.find("a", class_=re.compile(r"\bar-year\b"))
    if y:
        return extract_year(y.get_text(" ", strip=True))
    return extract_year(item.get_text(" ", strip=True))

def doi_from_item(item) -> str:
    cited = item.find("a", class_=re.compile(r"\bar-cited\b"))
    if cited:
        doi = extract_doi(cited.get_text(" ", strip=True))
        if doi:
            return doi
    return extract_doi(item.get_text(" ", strip=True))

def sinta_from_item(item) -> str:
    return extract_sinta(item.get_text(" ", strip=True))

def authors_from_item(item) -> str:
    meta = None
    for div in item.find_all("div", class_=re.compile(r"\bar-meta\b")):
        if re.search(r"\bAuthor Order\b", div.get_text(" ", strip=True), flags=re.I):
            meta = div
            break
    if meta:
        a = extract_authors_from_meta(meta.get_text(" ", strip=True))
        if a:
            return a

    # fallback: look for a visible author list containing ';'
    for a in item.find_all("a"):
        txt = clean_text(a.get_text(" ", strip=True))
        low = txt.lower()
        if not txt:
            continue
        if "author order" in low or "accred" in low or "doi:" in low:
            continue
        if re.search(r"\bvol\b|\bno\b|\bvolume\b", low):
            continue
        if ";" in txt:
            return txt
    return ""

def parse_one_page(html: str, source: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    items = soup.select("div.ar-list-item")
    rows = []
    for it in items:
        title = title_from_item(it)
        journal = journal_from_item(it)
        year = year_from_item(it)
        authors = authors_from_item(it)
        doi = doi_from_item(it)
        sinta = sinta_from_item(it)

        if not (title or doi or journal):
            continue

        rows.append({
            "Judul Artikel": title,
            "Tahun": year,
            "Authors": authors,
            "Nama Jurnal": journal,
            "Sinta": sinta,
            "DOI": doi,
            "SourceFile": source
        })
    return pd.DataFrame(rows)

def smart_dedup(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["Judul Artikel","Tahun","Authors","Nama Jurnal","Sinta","DOI","SourceFile"]:
        if col not in df.columns:
            df[col] = ""

    for col in ["Judul Artikel","Tahun","Authors","Nama Jurnal","DOI"]:
        df[col] = df[col].fillna("").astype(str).map(clean_text)

    def make_key(r):
        if r["DOI"]:
            return "DOI|" + r["DOI"].lower()
        return "META|" + "|".join([
            r["Judul Artikel"].lower(),
            r["Tahun"].lower(),
            r["Nama Jurnal"].lower(),
            r["Authors"].lower(),
        ])

    df["__key__"] = df.apply(make_key, axis=1)
    df = df.drop_duplicates(subset=["__key__"], keep="first").drop(columns=["__key__"])
    return df

def to_csv_semicolon(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, sep=";", index=False, encoding="utf-8")
    return buf.getvalue().encode("utf-8")


# ----------------------------
# URL utils: ensure view=garuda and set page
# ----------------------------
def normalize_profile_url(url: str) -> str:
    url = url.strip()
    p = urlparse(url)
    q = parse_qs(p.query)
    q["view"] = ["garuda"]  # force garuda view
    q.pop("page", None)     # we'll add page dynamically
    new_q = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))

def set_page(url: str, page: int) -> str:
    p = urlparse(url)
    q = parse_qs(p.query)
    q["view"] = ["garuda"]
    if page > 1:
        q["page"] = [str(page)]
    else:
        q.pop("page", None)
    new_q = urlencode(q, doseq=True)
    return urlunparse((p.scheme, p.netloc, p.path, p.params, new_q, p.fragment))

# ----------------------------
# Cookies handling (JSON upload)
# ----------------------------
def load_cookies_into_session(sess: requests.Session, cookies_json: str, base_url: str):
    """
    Accepts either:
    - list of cookies (Chrome export style): [{"name": "...", "value": "...", "domain": "...", "path": "..."}]
    - dict style: {"cookies":[...]}
    """
    data = json.loads(cookies_json)
    cookies = data["cookies"] if isinstance(data, dict) and "cookies" in data else data
    if not isinstance(cookies, list):
        raise ValueError("Cookie JSON must be a list or {'cookies':[...]}")

    parsed = urlparse(base_url)
    host = parsed.netloc

    for c in cookies:
        name = c.get("name")
        value = c.get("value")
        if not name:
            continue
        domain = c.get("domain") or host
        path = c.get("path") or "/"
        sess.cookies.set(name, value, domain=domain, path=path)

# ----------------------------
# UI
# ----------------------------
st.title("SINTA Live Parser (Login Cookies + Auto Pagination)")

st.write("1) Paste the **first page** SINTA profile URL (any page is OK).")
profile_url = st.text_input("SINTA profile URL", value="")

st.write("2) If the page requires login, upload your **cookies JSON** (optional).")
cookie_file = st.file_uploader("Cookies JSON (optional)", type=["json"], accept_multiple_files=False)

colA, colB = st.columns(2)
with colA:
    max_pages_cap = st.number_input("Max pages (safety cap)", min_value=1, max_value=500, value=100)
with colB:
    delay = st.number_input("Delay per page (seconds)", min_value=0.0, max_value=5.0, value=0.6, step=0.1)

run = st.button("Fetch & Parse")

if run:
    if not profile_url.strip():
        st.error("Please paste a profile URL.")
        st.stop()

    base = normalize_profile_url(profile_url)
    st.write("Normalized URL:", base)

    sess = requests.Session()
    sess.headers.update({
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9,id;q=0.8",
    })

    if cookie_file is not None:
        try:
            cookies_json = cookie_file.getvalue().decode("utf-8", errors="ignore")
            load_cookies_into_session(sess, cookies_json, base)
            st.success("Cookies loaded into session.")
        except Exception as e:
            st.error(f"Failed to load cookies: {e}")
            st.stop()

    all_parts = []
    seen_page_fps = set()
    empty_streak = 0

    progress = st.progress(0)
    status = st.empty()

    for page in range(1, int(max_pages_cap) + 1):
        url = set_page(base, page)

        try:
            r = sess.get(url, timeout=25)
            html = r.text
        except Exception as e:
            st.error(f"Request failed on page {page}: {e}")
            break

        # Detect repeated pages / end reached
        page_fp = hashlib.md5(html.encode("utf-8", errors="ignore")).hexdigest()
        if page_fp in seen_page_fps:
            status.write(f"Stopped: page {page} is identical to a previous page (end reached / pagination not changing).")
            break
        seen_page_fps.add(page_fp)

        dfp = parse_one_page(html, source=f"page_{page}")

        status.write(f"Page {page}: extracted {len(dfp)} rows | HTTP {r.status_code}")
        all_parts.append(dfp)

        # Stop when consecutive pages give 0 rows (end)
        if len(dfp) == 0:
            empty_streak += 1
            if empty_streak >= 2:
                status.write("Stopped: 2 pages in a row returned 0 rows.")
                break
        else:
            empty_streak = 0

        progress.progress(min(page / int(max_pages_cap), 1.0))

        if delay > 0:
            import time
            time.sleep(float(delay))

    if not all_parts:
        st.warning("No data extracted.")
        st.stop()

    df = pd.concat(all_parts, ignore_index=True)
    before = len(df)
    df = smart_dedup(df)
    after = len(df)

    df.insert(0, "No", range(1, len(df) + 1))
    df = df[["No","Judul Artikel","Tahun","Authors","Nama Jurnal","Sinta","DOI","SourceFile"]]

    st.success(f"Done. Rows before dedup: {before} | After smart dedup: {after}")
    st.dataframe(df, use_container_width=True, height=560)

    st.download_button(
        "Download CSV (delimiter ;)",
        data=to_csv_semicolon(df),
        file_name="sinta_export.csv",
        mime="text/csv"
    )
