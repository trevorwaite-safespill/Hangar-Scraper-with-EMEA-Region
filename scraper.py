"""
Safespill Hangar News Scraper
------------------------------
Runs weekly, searches for new hangar/fire-protection project news across
North America and EMEA using SerpAPI (Google Search), SAM.gov, CanadaBuys,
AusTender, and TED Europa. Emails a formatted Excel report with two tabs
(North America and EMEA) to the Safespill team every Monday at 8am PST.

Setup: see README.md
"""

import os
import io
import re
import time
import datetime
import logging
import requests
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from html.parser import HTMLParser

# --- Logging -----------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[logging.StreamHandler()],
)
log = logging.getLogger(__name__)

# --- Configuration -----------------------------------------------------------
SERPAPI_KEY   = os.environ["SERPAPI_KEY"]
SMTP_HOST     = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", 587))
SMTP_USER     = os.environ["SMTP_USER"]
SMTP_PASSWORD = os.environ["SMTP_PASSWORD"]
RECIPIENT     = os.environ.get("REPORT_RECIPIENT", "trevorw@safespill.com")

# --- Search configuration ----------------------------------------------------
SEARCH_QUERIES = [
    # New construction & development
    "new aircraft hangar construction",
    "hangar development project announced",
    "airport hangar expansion planned",
    "hangar construction contract awarded",
    "new FBO hangar development",
    "aircraft hangar real estate development",
    # Retrofit & upgrades
    "hangar retrofit project",
    "hangar renovation contract",
    "hangar fire suppression upgrade",
    # Military
    "military hangar construction contract",
    "Air Force hangar project",
    "Navy hangar construction",
    # Commercial aviation
    "airline maintenance hangar project",
    "MRO facility construction",
    "commercial aviation hangar development",
    # Fire protection specific
    "hangar fire protection system",
    "aircraft hangar fire suppression project",
]

# Site filters removed — free SerpAPI tier works best with clean short queries

NA_COUNTRIES = {"United States", "Canada", "Mexico"}

SAM_KEYWORDS = [
    "hangar", "aircraft", "airfield", "aviation", "fire suppression",
    "fire protection", "foam", "suppression system",
]

LOOKBACK_DAYS = 7

# --- EMEA Search configuration -----------------------------------------------
EMEA_SEARCH_QUERIES = [
    # New construction & development
    "new aircraft hangar construction",
    "hangar development project announced",
    "airport hangar expansion planned",
    "hangar construction contract awarded",
    "new FBO hangar development",
    "aircraft maintenance hangar project",
    # MRO facilities
    "MRO facility construction",
    "MRO hangar project",
    "aircraft maintenance facility construction",
    "MRO centre expansion",
    # Retrofit & upgrades
    "hangar retrofit project",
    "hangar renovation contract",
    "hangar fire suppression upgrade",
    "hangar fire protection upgrade",
    # Fire protection specific
    "hangar fire protection system",
    "aircraft hangar fire suppression project",
    "hangar foam suppression system",
    # Military & government
    "military hangar construction contract",
    "air force hangar project",
    "NATO hangar construction",
    "defence hangar project",
    # Region-specific with city/country names
    "hangar construction Dubai",
    "hangar project UAE",
    "aircraft hangar Nigeria",
    "MRO facility Africa",
    "hangar construction UK",
    "aircraft maintenance hangar Germany",
    "hangar project Saudi Arabia",
    "MRO facility Middle East",
    "hangar construction Poland",
    "aircraft hangar Turkey",
    "MRO hangar Kenya",
    "aviation facility South Africa",
]

# EMEA news domains extracted from known relevant sources
EMEA_NEWS_DOMAINS = [
    "zawya.com", "adsadvance.co.uk", "aviationbusinessme.com",
    "asianaviation.com", "arabianbusiness.com", "mepmiddleeast.com",
    "aviationweek.com", "aviationpros.com", "ainonline.com",
    "aerotime.aero", "aviationsourcenews.com", "avitrader.com",
    "engineeringnews.co.za", "ecofinagency.com", "businessairportinternational.com",
    "aircraftinteriorsinternational.com", "traveldailynews.com", "agbi.com",
    "breakingdefense.com", "defence-industry.eu", "gulfbusiness.com",
    "thisdaylive.com", "vanguardngr.com", "newtelegraphng.com",
    "punchng.com", "thesun.ng", "legit.ng", "arise.tv", "apanews.net",
    "addisinsight.net", "united24media.com", "haaretz.com",
    "ted.europa.eu", "travelandtourworld.com", "aviationbusinessnews.com",
]



HEADERS_UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# --- Helpers -----------------------------------------------------------------

def date_range():
    end   = datetime.date.today()
    start = end - datetime.timedelta(days=LOOKBACK_DAYS)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def week_label():
    today  = datetime.date.today()
    monday = today - datetime.timedelta(days=today.weekday())
    return monday.strftime("%Y-%m-%d")


# --- Article page fetching ---------------------------------------------------

# Common <meta> date property names used by news sites
META_DATE_PROPS = [
    "article:published_time",
    "article:modified_time",
    "og:published_time",
    "datePublished",
    "date",
    "pubdate",
    "publish_date",
    "DC.date",
    "DC.Date",
]

# Common JSON-LD / schema.org date fields
JSONLD_DATE_KEYS = ["datePublished", "dateCreated", "dateModified"]


class MetaParser(HTMLParser):
    """Extract <meta> tags and <script type=application/ld+json> from HTML."""

    def __init__(self):
        super().__init__()
        self.meta   = {}   # property/name -> content
        self.scripts = []  # raw text of ld+json blocks
        self._in_jsonld = False
        self._buf       = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta":
            key   = attrs.get("property") or attrs.get("name") or ""
            value = attrs.get("content", "")
            if key and value:
                self.meta[key.lower()] = value
        if tag == "script" and attrs.get("type") == "application/ld+json":
            self._in_jsonld = True
            self._buf = []

    def handle_endtag(self, tag):
        if tag == "script" and self._in_jsonld:
            self.scripts.append("".join(self._buf))
            self._in_jsonld = False
            self._buf = []

    def handle_data(self, data):
        if self._in_jsonld:
            self._buf.append(data)


def fetch_article_meta(url: str) -> dict:
    """
    Fetch an article page and extract:
      - publish_date  (YYYY-MM-DD string or "")
      - country       (str or "")
      - state         (str or "")
    Returns a dict with those three keys.
    """
    result = {"publish_date": "", "location": "", "state": ""}
    try:
        r = requests.get(url, headers=HEADERS_UA, timeout=15, allow_redirects=True)
        if r.status_code != 200:
            return result
        html = r.text[:150_000]   # only parse first ~150 KB to stay fast
    except Exception as exc:
        log.debug("Could not fetch %s: %s", url, exc)
        return result

    parser = MetaParser()
    try:
        parser.parse(html)
    except Exception:
        pass

    # --- Extract date --------------------------------------------------------
    date_str = ""

    # 1. Check <meta> tags
    for prop in META_DATE_PROPS:
        val = parser.meta.get(prop.lower(), "")
        if val:
            date_str = val
            break

    # 2. Check JSON-LD blocks
    if not date_str:
        import json
        for script in parser.scripts:
            try:
                data = json.loads(script)
                # data might be a list
                items = data if isinstance(data, list) else [data]
                for item in items:
                    for key in JSONLD_DATE_KEYS:
                        if item.get(key):
                            date_str = item[key]
                            break
                    if date_str:
                        break
            except Exception:
                continue

    # 3. Regex fallback — look for ISO date in the HTML
    if not date_str:
        m = re.search(r'(\d{4}-\d{2}-\d{2})', html[:50_000])
        if m:
            date_str = m.group(1)

    if date_str:
        result["publish_date"] = parse_iso_date(date_str)

    # --- Extract location from full page text --------------------------------
    # Use a larger chunk of visible text for better location detection
    visible = re.sub(r'<[^>]+>', ' ', html)
    result["country"] = detect_location(visible[:30_000])

    return result


def parse_iso_date(raw: str) -> str:
    """Normalise various date string formats to YYYY-MM-DD."""
    raw = raw.strip()
    # ISO 8601 with time component e.g. 2025-12-03T14:30:00Z
    m = re.match(r'(\d{4}-\d{2}-\d{2})', raw)
    if m:
        return m.group(1)
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%d %B %Y", "%d %b %Y"):
        try:
            return datetime.datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


# --- SerpAPI -----------------------------------------------------------------

def serpapi_news_search(query: str) -> list[dict]:
    url = "https://serpapi.com/search"
    params = {
        "engine":  "google",
        "q":       query,
        "gl":      "us",
        "hl":      "en",
        "num":     10,
        "api_key": SERPAPI_KEY,
    }
    try:
        r = requests.get(url, params=params, timeout=20)
        r.raise_for_status()
        data    = r.json()
        results = data.get("organic_results", [])
        log.info("SerpAPI '%s' -> %d results", query, len(results))
        return results
    except Exception as exc:
        log.warning("SerpAPI error for '%s': %s", query, exc)
        return []


def parse_serpapi_result(item: dict) -> dict | None:
    title   = item.get("title", "").strip()
    link    = item.get("link", "").strip()
    snippet = item.get("snippet", "").strip()

    if not title or not link:
        return None

    # Fallback date/location from snippet (will be overwritten by page fetch)
    date_raw         = item.get("date", "")
    fallback_date    = parse_google_date(date_raw)
    location = detect_location(snippet + " " + title)

    return {
        "Project Title":  title,
        "Source URL":     link,
        "Summary":        snippet,
        "Date Published": fallback_date,
        "Location":       location,
        "Source":         "news",
    }


def parse_google_date(raw: str) -> str:
    raw   = raw.strip().lower()
    today = datetime.date.today()
    if "hour" in raw or "minute" in raw or "second" in raw:
        return today.strftime("%Y-%m-%d")
    if "day" in raw:
        try:
            return (today - datetime.timedelta(days=int(raw.split()[0]))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    if "week" in raw:
        try:
            return (today - datetime.timedelta(weeks=int(raw.split()[0]))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%m/%d/%Y"):
        try:
            return datetime.datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def enrich_with_page_data(rows: list[dict]) -> list[dict]:
    """
    Visit each article URL and overwrite date/country/state with
    accurate values extracted directly from the page.
    """
    for i, row in enumerate(rows):
        url = row.get("Source URL", "")
        if not url or row.get("Source") == "sam_gov":
            continue
        log.info("Fetching page %d/%d: %s", i + 1, len(rows), url)
        meta = fetch_article_meta(url)
        if meta.get("publish_date"):
            row["Date Published"] = meta["publish_date"]
        if meta.get("country"):
            row["Location"] = meta["country"]
        time.sleep(0.5)   # be polite
    return rows


# --- SAM.gov -----------------------------------------------------------------

SAM_API_URL = "https://api.sam.gov/opportunities/v2/search"

def samgov_search(start_date: str, end_date: str) -> list[dict]:
    """
    Search SAM.gov using keyword-based title searches.
    Date format must be MM/DD/YYYY for SAM.gov API.
    """
    # Convert YYYY-MM-DD to MM/DD/YYYY for SAM.gov
    def fmt(d):
        parts = d.split("-")
        return parts[1] + "/" + parts[2] + "/" + parts[0]

    sam_keywords = [
        "hangar",
        "aircraft hangar",
        "hangar fire suppression",
        "hangar fire protection",
        "hangar construction",
        "hangar renovation",
    ]

    results = []
    seen_ids = set()

    for keyword in sam_keywords:
        params = {
            "api_key":    os.environ.get("SAM_API_KEY", "DEMO_KEY"),
            "postedFrom": fmt(start_date),
            "postedTo":   fmt(end_date),
            "title":      keyword,
            "limit":      25,
            "offset":     0,
        }
        try:
            r = requests.get(SAM_API_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            opps = data.get("opportunitiesData", [])
            log.info("SAM.gov '%s' -> %d results", keyword, len(opps))
            for opp in opps:
                notice_id = opp.get("noticeId", "")
                if notice_id in seen_ids:
                    continue
                seen_ids.add(notice_id)
                row = parse_sam_result(opp)
                if row:
                    results.append(row)
        except Exception as exc:
            log.warning("SAM.gov error for '%s': %s", keyword, exc)
        time.sleep(0.5)
    return results


def parse_sam_result(opp: dict) -> dict | None:
    title    = opp.get("title", "") or ""
    desc     = opp.get("description", "") or ""
    combined = (title + " " + desc).lower()

    if not any(kw in combined for kw in SAM_KEYWORDS):
        return None

    place        = opp.get("placeOfPerformance", {}) or {}
    state_code   = (place.get("state", {}) or {}).get("name", "")
    country_name = (place.get("country", {}) or {}).get("name", "United States")
    if country_name not in NA_COUNTRIES:
        country_name = "United States"

    posted = opp.get("postedDate", "")[:10] if opp.get("postedDate") else ""
    url    = "https://sam.gov/opp/" + opp.get("noticeId", "") + "/view"

    return {
        "Project Title":  title.strip(),
        "Source URL":     url,
        "Summary":        desc.strip()[:300] if desc else "",
        "Date Published": posted,
        "Region":         "NA",
        "Location":       location,
        "Source":         "sam_gov",
    }


# --- CanadaBuys (Canada) -----------------------------------------------------
# CanadaBuys publishes daily open data CSV files — no API key required.
# We download the "new tender notices" CSV and filter for hangar keywords.

CANADABUYS_CSV_URL = "https://canadabuys.canada.ca/opendata/pub/newTenderNotice-nouvelAvisAppelOffres.csv"

CANADABUYS_KEYWORDS = [
    "hangar", "aircraft", "airfield", "aviation",
    "fire suppression", "fire protection",
]

def canadabuys_search(start_date: str) -> list[dict]:
    """
    Download CanadaBuys new tender notices CSV and filter for
    hangar/aviation related opportunities published this week.
    """
    results = []
    try:
        r = requests.get(CANADABUYS_CSV_URL, timeout=30)
        r.raise_for_status()

        import csv
        import io as _io
        reader = csv.DictReader(_io.StringIO(r.text))
        for row in reader:
            # Column names vary — try common ones
            title = (
                row.get("title-titre-eng", "") or
                row.get("title_eng", "") or
                row.get("Title", "") or ""
            ).strip()
            pub_date = (
                row.get("publicationDate-datePublication", "") or
                row.get("publication_date", "") or
                row.get("Date", "") or ""
            ).strip()[:10]  # take YYYY-MM-DD part
            notice_id = (
                row.get("referenceNumber-numeroReference", "") or
                row.get("reference_number", "") or
                row.get("ID", "") or ""
            ).strip()
            desc = (
                row.get("description-eng", "") or
                row.get("description", "") or ""
            ).strip()

            if not title:
                continue

            # Filter by keyword
            combined = (title + " " + desc).lower()
            if not any(kw in combined for kw in CANADABUYS_KEYWORDS):
                continue

            # Filter by date — only keep results from this week
            if pub_date and pub_date < start_date:
                continue

            url = (
                "https://canadabuys.canada.ca/en/tender-opportunities/tender-notice/" +
                notice_id if notice_id else "https://canadabuys.canada.ca/en/tender-opportunities"
            )

            results.append({
                "Project Title":  title,
                "Source URL":     url,
                "Summary":        desc[:300] if desc else "",
                "Date Published": pub_date,
                "Region":         "NA",
                "Location":       "Canada",
                "Source":         "canadabuys",
            })

        log.info("CanadaBuys -> %d results", len(results))
    except Exception as exc:
        log.warning("CanadaBuys error: %s", exc)
    return results


# --- AusTender (Australia) ---------------------------------------------------
# AusTender OCDS API — requires a free auth token registered at tenders.gov.au
# Token is passed via the AUSTENDER_TOKEN environment variable / GitHub secret.

AUSTENDER_API_URL = "https://api.tenders.gov.au/atm/v1/releases"

AUSTENDER_KEYWORDS = [
    "hangar", "aircraft", "airfield", "aviation",
    "fire suppression", "fire protection",
]

def austender_search(start_date: str, end_date: str) -> list[dict]:
    """
    Search AusTender OCDS API for hangar-related opportunities.
    Requires AUSTENDER_TOKEN environment variable.
    """
    token = os.environ.get("AUSTENDER_TOKEN", "")
    if not token:
        log.warning("AUSTENDER_TOKEN not set — skipping AusTender search")
        return []

    results = []
    try:
        headers = {
            "Authorization": "Bearer " + token,
            "Accept": "application/json",
        }
        params = {
            "dateFrom": start_date,
            "dateTo":   end_date,
            "limit":    100,
        }
        r = requests.get(AUSTENDER_API_URL, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

        releases = data.get("releases", [])
        log.info("AusTender raw results: %d", len(releases))

        for release in releases:
            tender = release.get("tender", {}) or {}
            title  = (tender.get("title", "") or "").strip()
            desc   = (tender.get("description", "") or "").strip()

            if not title:
                continue

            combined = (title + " " + desc).lower()
            if not any(kw in combined for kw in AUSTENDER_KEYWORDS):
                continue

            pub_date   = (release.get("date", "") or "")[:10]
            release_id = release.get("ocid", "") or release.get("id", "")
            url = "https://www.tenders.gov.au/Atm/Show/" + release_id if release_id else "https://www.tenders.gov.au"

            results.append({
                "Project Title":  title,
                "Source URL":     url,
                "Summary":        desc[:300] if desc else "",
                "Date Published": pub_date,
                "Location":       "Australia",
                "Source":         "austender",
            })

        log.info("AusTender -> %d relevant results", len(results))
    except Exception as exc:
        log.warning("AusTender error: %s", exc)
    return results


# --- TED Europa (European procurement) ---------------------------------------
# TED (Tenders Electronic Daily) is the EU's official procurement portal.
# The public search API requires no authentication.

TED_API_URL = "https://api.ted.europa.eu/v3/notices/search"

TED_KEYWORDS = [
    "hangar", "aircraft hangar", "fire suppression hangar",
    "hangar fire protection", "hangar construction", "MRO facility",
]

def ted_europa_search(start_date: str, end_date: str) -> list[dict]:
    """
    Search TED Europa for hangar/aviation related procurement notices.
    No API key required for basic public search.
    """
    results = []
    seen_ids = set()

    for keyword in TED_KEYWORDS:
        try:
            payload = {
                "query":  keyword,
                "fields": ["title", "summary", "publicationDate", "noticePublicationId", "noticeType"],
                "page":   1,
                "limit":  25,
                "filters": {
                    "publicationDate": {
                        "gte": start_date,
                        "lte": end_date,
                    }
                },
                "sort": [{"publicationDate": "desc"}],
            }
            r = requests.post(TED_API_URL, json=payload, timeout=30)
            r.raise_for_status()
            data = r.json()
            notices = data.get("notices", [])
            log.info("TED Europa '%s' -> %d results", keyword, len(notices))

            for notice in notices:
                notice_id = notice.get("noticePublicationId", "")
                if notice_id in seen_ids:
                    continue
                seen_ids.add(notice_id)

                title    = (notice.get("title", {}) or {}).get("eng", "") or ""
                if not title:
                    title = next(iter((notice.get("title", {}) or {}).values()), "")
                summary  = (notice.get("summary", {}) or {}).get("eng", "") or ""
                pub_date = (notice.get("publicationDate", "") or "")[:10]
                url      = "https://ted.europa.eu/en/notice/-/detail/" + notice_id if notice_id else "https://ted.europa.eu"

                if not title:
                    continue

                results.append({
                    "Project Title":  title.strip(),
                    "Source URL":     url,
                    "Summary":        summary.strip()[:300] if summary else "",
                    "Date Published": pub_date,
                    "Location":        "Europe",
                    "Source":         "ted_europa",
                })

        except Exception as exc:
            log.warning("TED Europa error for '%s': %s", keyword, exc)
        time.sleep(0.5)

    log.info("TED Europa total -> %d results", len(results))
    return results


# --- Location detection ------------------------------------------------------

US_STATES = {
    "Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut",
    "Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa",
    "Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan",
    "Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada",
    "New Hampshire","New Jersey","New Mexico","New York","North Carolina",
    "North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island",
    "South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont",
    "Virginia","Washington","West Virginia","Wisconsin","Wyoming",
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY",
}

CA_PROVINCES = {
    "Alberta","British Columbia","Manitoba","New Brunswick",
    "Newfoundland and Labrador","Nova Scotia","Ontario",
    "Prince Edward Island","Quebec","Saskatchewan",
    "Northwest Territories","Nunavut","Yukon",
    "AB","BC","MB","NB","NL","NS","ON","PE","QC","SK","NT","NU","YT",
}

MX_STATES = {
    "Aguascalientes","Baja California","Baja California Sur","Campeche",
    "Chiapas","Chihuahua","Coahuila","Colima","Durango","Guanajuato",
    "Guerrero","Hidalgo","Jalisco","Mexico City","Michoacan","Morelos",
    "Nayarit","Nuevo Leon","Oaxaca","Puebla","Queretaro","Quintana Roo",
    "San Luis Potosi","Sinaloa","Sonora","Tabasco","Tamaulipas","Tlaxcala",
    "Veracruz","Yucatan","Zacatecas",
}

# European countries
EUROPE_COUNTRIES = [
    "Albania","Andorra","Armenia","Austria","Azerbaijan","Belarus","Belgium",
    "Bosnia and Herzegovina","Bulgaria","Croatia","Cyprus","Czech Republic",
    "Czechia","Denmark","Estonia","Finland","France","Georgia","Germany",
    "Greece","Hungary","Iceland","Ireland","Italy","Kazakhstan","Kosovo",
    "Latvia","Liechtenstein","Lithuania","Luxembourg","Malta","Moldova",
    "Monaco","Montenegro","Netherlands","North Macedonia","Norway","Poland",
    "Portugal","Romania","Russia","San Marino","Serbia","Slovakia","Slovenia",
    "Spain","Sweden","Switzerland","Turkey","Ukraine","United Kingdom",
    "Vatican","England","Scotland","Wales","Northern Ireland",
    # Major European cities that commonly appear in articles
    "London","Paris","Berlin","Madrid","Rome","Amsterdam","Brussels",
    "Vienna","Warsaw","Prague","Budapest","Bucharest","Stockholm","Oslo",
    "Copenhagen","Helsinki","Athens","Lisbon","Dublin","Zurich","Geneva",
    "Munich","Frankfurt","Hamburg","Barcelona","Milan","Naples","Turin",
    "Schiphol","Heathrow","Gatwick","Stansted","Luton","Birmingham",
    "Tewkesbury","Gloucester","Powidz","Larnaca","Nicosia",
]

# Middle Eastern countries and cities
MIDDLE_EAST_COUNTRIES = [
    "Bahrain","Cyprus","Egypt","Iran","Iraq","Israel","Jordan","Kuwait",
    "Lebanon","Oman","Palestine","Qatar","Saudi Arabia","Syria",
    "United Arab Emirates","UAE","Yemen",
    # Major cities
    "Dubai","Abu Dhabi","Doha","Riyadh","Jeddah","Muscat","Manama",
    "Amman","Beirut","Tel Aviv","Cairo","Kuwait City","Dubai South",
    "Mohammed bin Rashid","Sharjah","Ras Al Khaimah",
]

# African countries and cities
AFRICA_COUNTRIES = [
    "Algeria","Angola","Benin","Botswana","Burkina Faso","Burundi",
    "Cameroon","Cape Verde","Central African Republic","Chad","Comoros",
    "Congo","Ivory Coast","Djibouti","Egypt","Equatorial Guinea","Eritrea",
    "Eswatini","Ethiopia","Gabon","Gambia","Ghana","Guinea","Guinea-Bissau",
    "Kenya","Lesotho","Liberia","Libya","Madagascar","Malawi","Mali",
    "Mauritania","Mauritius","Morocco","Mozambique","Namibia","Niger",
    "Nigeria","Rwanda","Sao Tome","Senegal","Seychelles","Sierra Leone",
    "Somalia","South Africa","South Sudan","Sudan","Tanzania","Togo",
    "Tunisia","Uganda","Zambia","Zimbabwe",
    # Major cities
    "Lagos","Abuja","Nairobi","Johannesburg","Cape Town","Accra","Addis Ababa",
    "Dar es Salaam","Casablanca","Tunis","Algiers","Kampala","Dakar",
    "Anambra","Enugu","Kano","Port Harcourt",
]

# Asian countries relevant to aviation (for Asia-Pacific results)
ASIA_COUNTRIES = [
    "Afghanistan","Bangladesh","Bhutan","Brunei","Cambodia","China",
    "India","Indonesia","Japan","Laos","Malaysia","Maldives","Mongolia",
    "Myanmar","Nepal","North Korea","Pakistan","Philippines","Singapore",
    "South Korea","Sri Lanka","Taiwan","Thailand","Timor-Leste","Vietnam",
    "Australia","New Zealand","Papua New Guinea","Fiji",
    # Major cities
    "Beijing","Shanghai","Tokyo","Seoul","Singapore City","Mumbai","Delhi",
    "Bangkok","Jakarta","Kuala Lumpur","Manila","Karachi","Dhaka",
    "Hong Kong","Taipei","Colombo","Kathmandu","Yangon","Phnom Penh",
    "Ho Chi Minh","Hanoi","Kochi","Kerala","Larnaca",
]

def detect_location(text: str) -> str:
    """
    Detect the most specific location mentioned in the text.
    Returns a location string (country, city, or region).
    Checks EMEA regions before defaulting to North America.
    """
    lower = text.lower()

    # Check Middle East first (before Africa/Europe to catch UAE, Dubai etc.)
    for place in sorted(MIDDLE_EAST_COUNTRIES, key=len, reverse=True):
        if place.lower() in lower:
            return place

    # Check Africa
    for place in sorted(AFRICA_COUNTRIES, key=len, reverse=True):
        if place.lower() in lower:
            return place

    # Check Europe
    for place in sorted(EUROPE_COUNTRIES, key=len, reverse=True):
        if place.lower() in lower:
            return place

    # Check Asia-Pacific
    for place in sorted(ASIA_COUNTRIES, key=len, reverse=True):
        if place.lower() in lower:
            return place

    # Check Canada
    CA_PROVINCES_LIST = sorted(CA_PROVINCES, key=len, reverse=True)
    if "canada" in lower:
        for prov in CA_PROVINCES_LIST:
            if prov.lower() in lower:
                return "Canada - " + prov
        return "Canada"

    # Check Mexico
    MX_STATES_LIST = sorted(MX_STATES, key=len, reverse=True)
    if "mexico" in lower:
        for st in MX_STATES_LIST:
            if st.lower() in lower:
                return "Mexico - " + st
        return "Mexico"

    # Check US states
    for st in sorted(US_STATES, key=len, reverse=True):
        if st.lower() in lower:
            return "United States"

    return ""


# --- Deduplication -----------------------------------------------------------

def deduplicate(rows: list[dict]) -> list[dict]:
    seen_urls, seen_titles, unique = set(), set(), []
    for row in rows:
        url   = (row.get("Source URL") or "").strip().lower()
        title = (row.get("Project Title") or "").strip().lower()
        if (url and url in seen_urls) or (title and title in seen_titles):
            continue
        seen_urls.add(url)
        seen_titles.add(title)
        unique.append(row)
    return unique


# --- Excel report ------------------------------------------------------------

HEADERS = [
    "Project Title", "Source URL", "Summary",
    "Date Published", "Location",
]

COL_WIDTHS = {
    "Project Title":  40, "Source URL": 50, "Summary": 60,
    "Date Published": 15, "Location": 20,
}

HEADER_FILL  = PatternFill("solid", fgColor="1F3864")
HEADER_FONT  = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
ALT_ROW_FILL = PatternFill("solid", fgColor="DCE6F1")
THIN         = Side(style="thin", color="AAAAAA")
BORDER       = Border(left=THIN, right=THIN, top=THIN, bottom=THIN)


def write_sheet(ws, rows: list[dict]):
    """Write a set of rows into a given worksheet with full formatting."""
    ws.freeze_panes = "A2"

    for col_idx, header in enumerate(HEADERS, start=1):
        cell           = ws.cell(row=1, column=col_idx, value=header)
        cell.font      = HEADER_FONT
        cell.fill      = HEADER_FILL
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border    = BORDER

    ws.row_dimensions[1].height = 20

    for row_idx, row in enumerate(rows, start=2):
        fill = ALT_ROW_FILL if row_idx % 2 == 0 else PatternFill()
        for col_idx, header in enumerate(HEADERS, start=1):
            value          = row.get(header, "")
            cell           = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.fill      = fill
            cell.border    = BORDER
            cell.alignment = Alignment(vertical="top", wrap_text=True)
            cell.font      = Font(name="Calibri", size=10)
            if header == "Source URL" and value:
                cell.hyperlink = value
                cell.font = Font(name="Calibri", size=10, color="0563C1", underline="single")

    for col_idx, header in enumerate(HEADERS, start=1):
        ws.column_dimensions[get_column_letter(col_idx)].width = COL_WIDTHS[header]

    ws.auto_filter.ref = ws.dimensions


def build_workbook(na_rows: list[dict], emea_rows: list[dict], week: str) -> openpyxl.Workbook:
    """Build a workbook with two tabs: North America and EMEA."""
    wb = openpyxl.Workbook()

    # Tab 1: North America
    ws_na       = wb.active
    ws_na.title = "North America - " + week
    write_sheet(ws_na, na_rows)

    # Tab 2: EMEA
    ws_emea       = wb.create_sheet()
    ws_emea.title = "EMEA - " + week
    write_sheet(ws_emea, emea_rows)

    return wb


def workbook_to_bytes(wb: openpyxl.Workbook) -> bytes:
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# --- Email -------------------------------------------------------------------

EMAIL_BODY = """\
Hi Safespill Team,

Please find attached this week's Safespill Hangar Intelligence Report.

The report covers new hangar projects, retrofit work, construction contracts,
and fire protection opportunities globally, discovered between
{start_date} and {end_date}.

The Excel file contains two tabs:
  - North America: Google Search, SAM.gov, CanadaBuys, AusTender
  - EMEA: Google Search, TED Europa

Total results this week: {count}

Safespill Automated Intelligence Report
"""


def send_email(xlsx_bytes: bytes, filename: str, start_date: str, end_date: str, count: int):
    msg            = MIMEMultipart()
    msg["From"]    = SMTP_USER
    msg["To"]      = RECIPIENT
    msg["Subject"] = "Safespill Hangar Intelligence Report - Week of " + week_label()

    msg.attach(MIMEText(EMAIL_BODY.format(
        start_date=start_date, end_date=end_date, count=count), "plain"))

    part = MIMEBase("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    part.set_payload(xlsx_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", 'attachment; filename="' + filename + '"')
    msg.attach(part)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.sendmail(SMTP_USER, RECIPIENT, msg.as_string())

    log.info("Email sent to %s", RECIPIENT)


# --- Main --------------------------------------------------------------------

def main():
    start_date, end_date = date_range()
    week = week_label()
    log.info("Running report for %s -> %s", start_date, end_date)

    # ---- North America ----
    na_rows = []

    # 1. Google Search (NA queries)
    for query in SEARCH_QUERIES:
        log.info("[NA] Searching Google: '%s'", query)
        items = serpapi_news_search(query)
        for item in items:
            row = parse_serpapi_result(item)
            if row:
                na_rows.append(row)
        time.sleep(1)

    # 2. SAM.gov
    log.info("[NA] Searching SAM.gov ...")
    na_rows.extend(samgov_search(start_date, end_date))

    # 3. CanadaBuys
    log.info("[NA] Searching CanadaBuys ...")
    na_rows.extend(canadabuys_search(start_date))

    # 4. AusTender
    log.info("[NA] Searching AusTender ...")
    na_rows.extend(austender_search(start_date, end_date))

    # ---- EMEA ----
    emea_rows = []

    # 5. Google Search (EMEA queries)
    for query in EMEA_SEARCH_QUERIES:
        log.info("[EMEA] Searching Google: '%s'", query)
        items = serpapi_news_search(query)
        for item in items:
            row = parse_serpapi_result(item)
            if row:
                emea_rows.append(row)
        time.sleep(1)

    # 6. TED Europa
    log.info("[EMEA] Searching TED Europa ...")
    emea_rows.extend(ted_europa_search(start_date, end_date))

    # ---- Process both ----
    # 7. Deduplicate
    na_rows   = deduplicate(na_rows)
    emea_rows = deduplicate(emea_rows)
    log.info("NA unique: %d | EMEA unique: %d", len(na_rows), len(emea_rows))

    # 8. Enrich with accurate dates and locations
    log.info("Enriching NA articles ...")
    na_rows = enrich_with_page_data(na_rows)
    log.info("Enriching EMEA articles ...")
    emea_rows = enrich_with_page_data(emea_rows)

    # 9. Sort by date
    na_rows.sort(key=lambda r: r.get("Date Published", ""), reverse=True)
    emea_rows.sort(key=lambda r: r.get("Date Published", ""), reverse=True)

    total = len(na_rows) + len(emea_rows)
    log.info("Total unique results: %d (NA: %d, EMEA: %d)", total, len(na_rows), len(emea_rows))

    # 10. Build Excel with two tabs
    wb       = build_workbook(na_rows, emea_rows, week)
    xlsx     = workbook_to_bytes(wb)
    filename = "Safespill_Hangar_Report_" + week + ".xlsx"

    # 11. Send email
    send_email(xlsx, filename, start_date, end_date, total)
    log.info("Done.")


if __name__ == "__main__":
    main()
