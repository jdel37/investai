import asyncio
import feedparser
import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from openai import OpenAI
import os
import json
import re
from dotenv import load_dotenv
from datetime import datetime
from pydantic import BaseModel
from typing import List

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

try:
    import networkx as nx
    HAS_NX = True
except ImportError:
    HAS_NX = False

try:
    import yfinance as yf
    HAS_YF = True
except ImportError:
    HAS_YF = False

# ─── HIGH-IMPACT PERSONS ─────────────────────────────────────────────────────
HIGH_IMPACT_PERSONS = {
    # US Politics / Government
    "Trump","Biden","Harris","Bessent","Rubio","Pelosi","Schumer","McCarthy",
    "Obama","Clinton","Bush",
    # Central Banks
    "Powell","Yellen","Lagarde","Ueda","Bailey","Waller","Daly","Williams",
    "Bernanke","Draghi","Trichet",
    # World Leaders
    "Xi","Putin","Modi","Macron","Scholz","Sunak","Meloni","Milei","Lula",
    "Erdogan","MBS","Netanyahu","Zelensky","Kim",
    # Finance / Investing legends
    "Buffett","Munger","Dimon","Dalio","Ackman","Icahn","Soros","Griffin",
    "Simons","Marks","Tepper","Druckenmiller","Bass","Burry","Einhorn",
    # Tech CEOs
    "Musk","Altman","Zuckerberg","Bezos","Cook","Huang","Nadella","Pichai",
    "Benioff","Ellison","Gates","Page","Brin","Thiel","Andreessen",
    # Energy / Commodities
    "Aramco","OPEC","Wirth","Woods",
    # Economists
    "Roubini","Krugman","Summers","Shiller","Rogoff","Stiglitz","Piketty",
}

# ─── SOURCE TIERS (veracity weighting) ───────────────────────────────────────
TIER1_SOURCES = {
    "Reuters Business","Reuters Markets","Reuters World",
    "BBC Business","BBC World","FT","Bloomberg Markets","Bloomberg Tech",
    "AP Business","AP Politics","WSJ Markets","WSJ Economy",
    "The Economist Finance","Nikkei Asia",
    # expanded — wire services + papers of record + primary data
    "NYT Business","NYT Economy","Guardian Business","Bloomberg Economics",
    "Federal Reserve","ECB Press","Bank of England","US Treasury","BLS Releases",
    "S&P Global",
}
TIER2_SOURCES = {
    "CNBC Markets","CNBC Economy","CNBC Tech","CNBC Top News",
    "MarketWatch Top","MarketWatch Markets","Foreign Policy","CFR",
    "Brookings","SCMP Business","Deutsche Welle Biz","Barrons",
    "ECB Speeches","IMF Blog","BIS Speeches",
    "Peterson IIE","SEC Press","POLITICO Economy","Axios Markets",
    # expanded
    "NPR Economy","CNN Business","Fortune","Forbes Markets","Guardian World",
    "Bloomberg Politics","Investing.com","Morningstar","Kitco Gold","OECD",
    "World Bank Blog",
}

# Sentiment/rumor sources — useful for crowd signal but NOT for fact corroboration.
# Claims appearing ONLY here are treated as low-credibility (possible fake/unverified).
SENTIMENT_SOURCES = {
    "Reddit Investing","Reddit WSB","Reddit Economics","Reddit Stocks","Reddit Crypto",
    "Zero Hedge","Hacker News","CoinGape","Crypto Briefing","CryptoSlate",
}

# ─── MARKET TICKERS ──────────────────────────────────────────────────────────
MARKET_TICKERS = {
    "SPY":"S&P500","QQQ":"Nasdaq","^DJI":"DowJones","^VIX":"VIX",
    "BTC-USD":"Bitcoin","ETH-USD":"Ethereum",
    "GLD":"Oro","TLT":"Bonos30Y","DX-Y.NYB":"DolarIdx",
    "CL=F":"Petróleo","GC=F":"OroFut","^TNX":"Yield10Y",
}

load_dotenv()

GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT", GMAIL_USER)
ALERT_MIN_PRIORITY = int(os.getenv("ALERT_MIN_PRIORITY", "8"))

app = FastAPI(title="InvestAI")
_openai_client = None

def get_openai_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        key = os.getenv("OPENAI_API_KEY")
        if not key:
            raise ValueError("OPENAI_API_KEY no configurado")
        _openai_client = OpenAI(api_key=key)
    return _openai_client

# Server state — single user app
_state: dict = {"analysis": {}, "articles": [], "fetched_at": None, "graph": None, "history": []}

# ─── 80+ FEEDS ───────────────────────────────────────────────────────────────
FEEDS = {
    # FINANCIAL / MARKETS
    "Reuters Business":      "https://feeds.reuters.com/reuters/businessNews",
    "Reuters Markets":       "https://feeds.reuters.com/reuters/financialsNews",
    "Reuters World":         "https://feeds.reuters.com/reuters/worldNews",
    "BBC Business":          "https://feeds.bbci.co.uk/news/business/rss.xml",
    "BBC World":             "https://feeds.bbci.co.uk/news/world/rss.xml",
    "CNBC Markets":          "https://www.cnbc.com/id/15839135/device/rss/rss.html",
    "CNBC Economy":          "https://www.cnbc.com/id/20910258/device/rss/rss.html",
    "CNBC Tech":             "https://www.cnbc.com/id/19854910/device/rss/rss.html",
    "CNBC Top News":         "https://www.cnbc.com/id/100003114/device/rss/rss.html",
    "MarketWatch Top":       "https://feeds.marketwatch.com/marketwatch/topstories/",
    "MarketWatch Markets":   "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "FT":                    "https://www.ft.com/rss/home",
    "Bloomberg Markets":     "https://feeds.bloomberg.com/markets/news.rss",
    "Bloomberg Tech":        "https://feeds.bloomberg.com/technology/news.rss",
    "Seeking Alpha":         "https://seekingalpha.com/market_currents.xml",
    "Yahoo Finance":         "https://finance.yahoo.com/news/rssindex",
    "Barrons":               "https://www.barrons.com/xml/rss/3_7510.xml",
    "Business Insider":      "https://markets.businessinsider.com/rss/news",
    "The Street":            "https://www.thestreet.com/.rss/full/",
    "Kiplinger":             "https://www.kiplinger.com/feeds/rss/investing.xml",
    "Wolf Street":           "https://wolfstreet.com/feed/",
    "Calculated Risk":       "https://www.calculatedriskblog.com/feeds/posts/default",
    "Naked Capitalism":      "https://www.nakedcapitalism.com/feed",

    # TECHNOLOGY
    "TechCrunch":            "https://techcrunch.com/feed/",
    "Ars Technica":          "https://feeds.arstechnica.com/arstechnica/index",
    "The Verge":             "https://www.theverge.com/rss/index.xml",
    "Wired":                 "https://www.wired.com/feed/rss",
    "MIT Tech Review":       "https://www.technologyreview.com/topnews.rss",
    "VentureBeat":           "https://venturebeat.com/feed/",
    "Hacker News":           "https://news.ycombinator.com/rss",
    "ZDNet":                 "https://www.zdnet.com/news/rss.xml",
    "The Register":          "https://www.theregister.com/headlines.atom",
    "IEEE Spectrum":         "https://spectrum.ieee.org/feeds/feed.rss",

    # GEOPOLITICAL / MACRO
    "Foreign Policy":        "https://foreignpolicy.com/feed/",
    "Geopolitical Monitor":  "https://www.geopoliticalmonitor.com/feed/",
    "CFR":                   "https://www.cfr.org/rss/all",
    "Al Jazeera":            "https://www.aljazeera.com/xml/rss/all.xml",
    "The Diplomat":          "https://thediplomat.com/feed/",
    "Euractiv":              "https://www.euractiv.com/feed/",
    "Deutsche Welle Biz":    "https://rss.dw.com/xml/rss-en-bus",
    "Deutsche Welle World":  "https://rss.dw.com/xml/rss-en-world",
    "Brookings":             "https://www.brookings.edu/feed/",
    "Carnegie Endowment":    "https://carnegieendowment.org/rss/everything",
    "Project Syndicate":     "https://www.project-syndicate.org/feed",
    "VoxEU":                 "https://cepr.org/voxeu/rss.xml",
    "Zero Hedge":            "https://feeds.feedburner.com/zerohedge/feed",
    "Mises Institute":       "https://mises.org/rss",

    # CRYPTO
    "CoinDesk":              "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "CoinTelegraph":         "https://cointelegraph.com/rss",
    "Decrypt":               "https://decrypt.co/feed",
    "Bitcoin Magazine":      "https://bitcoinmagazine.com/feed",
    "The Block":             "https://www.theblock.co/rss.xml",
    "Crypto Briefing":       "https://cryptobriefing.com/feed/",
    "CryptoSlate":           "https://cryptoslate.com/feed/",

    # ENERGY / COMMODITIES
    "OilPrice":              "https://oilprice.com/rss/main",
    "Mining.com":            "https://www.mining.com/feed/",

    # REAL ESTATE
    "HousingWire":           "https://www.housingwire.com/feed",

    # ASIA / EMERGING
    "Nikkei Asia":           "https://asia.nikkei.com/rss/feed/nar",
    "SCMP Business":         "https://www.scmp.com/rss/92/feed",
    "Economic Times":        "https://economictimes.indiatimes.com/rssfeedstopstories.cms",

    # LATIN AMERICA
    "El País Economía":      "https://feeds.elpais.com/mrss-s/pages/ep/site/elpais.com/section/economia/portada",
    "Expansión MX":          "https://expansion.mx/rss",
    "Infobae Economía":      "https://www.infobae.com/feeds/rss/economia/",
    "Portafolio CO":         "https://www.portafolio.co/rss/portafolio.xml",
    "Ámbito AR":             "https://www.ambito.com/rss/pages/economia.xml",

    # SOCIAL / SENTIMENT
    "Reddit Investing":      "https://www.reddit.com/r/investing/.rss",
    "Reddit WSB":            "https://www.reddit.com/r/wallstreetbets/.rss",
    "Reddit Economics":      "https://www.reddit.com/r/economics/.rss",
    "Reddit Stocks":         "https://www.reddit.com/r/stocks/.rss",
    "Reddit Crypto":         "https://www.reddit.com/r/CryptoCurrency/.rss",

    # OFFICIAL / INSTITUTIONAL (alto impacto, declaraciones de líderes)
    "Federal Reserve":       "https://www.federalreserve.gov/feeds/press_all.xml",
    "ECB Speeches":          "https://www.ecb.europa.eu/rss/speeches.html",
    "IMF Blog":              "https://www.imf.org/en/blogs/rss",
    "World Bank Blog":       "https://blogs.worldbank.org/en/rss.xml",
    "BIS Speeches":          "https://www.bis.org/rss/content_cbspeech.rss",
    "US Treasury":           "https://home.treasury.gov/news/press-releases/rss.xml",
    "Peterson IIE":          "https://www.piie.com/rss/all",
    "SEC Press":             "https://efts.sec.gov/LATEST/search-index?q=%22press+release%22&dateRange=custom&startdt=2024-01-01&forms=8-K",
    "OECD":                  "https://www.oecd.org/newsroom/rss.xml",

    # PREMIUM INVESTMENT MEDIA
    "WSJ Markets":           "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "WSJ Economy":           "https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml",
    "The Economist Finance": "https://www.economist.com/finance-and-economics/rss.xml",
    "The Economist World":   "https://www.economist.com/the-world-this-week/rss.xml",
    "AP Business":           "https://feeds.apnews.com/apnews/business",
    "AP Politics":           "https://feeds.apnews.com/apnews/politics",
    "Bloomberg Opinion":     "https://feeds.bloomberg.com/bview/news.rss",
    "POLITICO Economy":      "https://rss.politico.com/economy.xml",
    "POLITICO World":        "https://rss.politico.com/politics-news.xml",
    "Axios Markets":         "https://api.axios.com/feed/",
    "The Hill Finance":      "https://thehill.com/rss/syndicator/19110",
    "Investopedia":          "https://www.investopedia.com/feeds/all.aspx",
    "Motley Fool":           "https://www.fool.com/feeds/index.aspx",

    # MACRO / THINK TANKS
    "RAND":                  "https://www.rand.org/pubs/rss/all.xml",
    "Atlantic Council":      "https://www.atlanticcouncil.org/feed/",
    "Chatham House":         "https://www.chathamhouse.org/rss/news.xml",
    "Harvard Belfer":        "https://www.belfercenter.org/rss.xml",
    "Cato Institute":        "https://www.cato.org/rss.xml",

    # TECH / AI (CEOs y movimientos que mueven mercados)
    "OpenAI Blog":           "https://openai.com/blog/rss.xml",
    "Google DeepMind":       "https://deepmind.google/discover/blog/rss.xml",
    "Anthropic News":        "https://www.anthropic.com/news/rss",
    "TechCrunch AI":         "https://techcrunch.com/category/artificial-intelligence/feed/",
    "Semaphore Tech":        "https://semaphoreco.substack.com/feed",

    # INVESTMENT ANALYSIS / INFLUENCERS
    "Hussman Funds":         "https://hussmanfunds.com/rss/",
    "Morningstar":           "https://www.morningstar.com/news/rss.xml",
    "Seeking Alpha Premium": "https://seekingalpha.com/articles/investing-strategy.xml",

    # EXPANDED COVERAGE — broad reputable outlets (más fuentes = mejor corroboración)
    "Guardian Business":     "https://www.theguardian.com/uk/business/rss",
    "Guardian World":        "https://www.theguardian.com/world/rss",
    "NYT Business":          "https://rss.nytimes.com/services/xml/rss/nyt/Business.xml",
    "NYT Economy":           "https://rss.nytimes.com/services/xml/rss/nyt/Economy.xml",
    "NPR Economy":           "https://feeds.npr.org/1017/rss.xml",
    "CNN Business":          "http://rss.cnn.com/rss/money_latest.rss",
    "Fortune":               "https://fortune.com/feed/",
    "Forbes Markets":        "https://www.forbes.com/markets/feed/",
    "Quartz":                "https://qz.com/rss",
    "Bloomberg Economics":   "https://feeds.bloomberg.com/economics/news.rss",
    "Bloomberg Politics":    "https://feeds.bloomberg.com/politics/news.rss",
    "Investing.com":         "https://www.investing.com/rss/news.rss",
    "Investing Commodities": "https://www.investing.com/rss/commodities_News.rss",
    "Kitco Gold":            "https://www.kitco.com/rss/KitcoNewsRSS.xml",
    # OFFICIAL / DATA (alta veracidad — fuente primaria)
    "Bank of England":       "https://www.bankofengland.co.uk/rss/news",
    "ECB Press":             "https://www.ecb.europa.eu/rss/press.html",
    "BLS Releases":          "https://www.bls.gov/feed/news_release.rss",
    "S&P Global":            "https://www.spglobal.com/en/rss/news",
    # CRYPTO (más cobertura)
    "Bankless":              "https://newsletter.banklesshq.com/feed",
    "CoinGape":              "https://coingape.com/feed/",
}

# Feeds de solo Tier-1 para check rápido de breaking news
FEEDS_TIER1 = {k: v for k, v in FEEDS.items() if k in TIER1_SOURCES | TIER2_SOURCES}

# ─── MARKET SNAPSHOT ─────────────────────────────────────────────────────────
def fetch_market_snapshot() -> str:
    if not HAS_YF:
        return ""
    parts = []
    for ticker, name in MARKET_TICKERS.items():
        try:
            fi = yf.Ticker(ticker).fast_info
            price = fi.last_price
            prev = fi.previous_close
            chg = (price / prev * 100 - 100) if prev and prev > 0 else 0
            parts.append(f"{name}:{price:.2f}({chg:+.1f}%)")
        except Exception:
            pass
    return " | ".join(parts)


# ─── BREAKING NEWS DETECTION ─────────────────────────────────────────────────
BREAKING_KEYWORDS = {
    "crash","crisis","collapse","emergency","war","attack","sanctions","default",
    "bankruptcy","recession","rate hike","rate cut","tariff","invasion","coup",
    "explosion","assassination","shock","panic","surge","plunge","halt",
}

def detect_breaking_news(articles: list[dict]) -> str:
    """Returns description of breaking news trigger, or empty string if none."""
    hits = []
    for art in articles:
        src = art.get("source", "")
        if src not in TIER1_SOURCES:
            continue
        text = (art["title"] + " " + art.get("summary", "")).lower()
        kw_hits = [kw for kw in BREAKING_KEYWORDS if kw in text]
        person_hits = [p for p in HIGH_IMPACT_PERSONS if p.lower() in text]
        # Trigger: Tier-1 source + breaking keyword + high-impact person
        if kw_hits and person_hits:
            hits.append(f"{art['source']}: {art['title'][:80]}")
    if len(hits) >= 2:
        return "; ".join(hits[:3])
    return ""


# ─── FETCH ───────────────────────────────────────────────────────────────────
SEM = asyncio.Semaphore(25)

async def fetch_feed(name: str, url: str, http: httpx.AsyncClient) -> list[dict]:
    async with SEM:
        try:
            resp = await http.get(url, timeout=9.0, follow_redirects=True)
            feed = feedparser.parse(resp.text)
            out = []
            for entry in feed.entries[:6]:
                title = entry.get("title", "").strip()
                if not title:
                    continue
                summary = re.sub(r"<[^>]+>", " ",
                    entry.get("summary", entry.get("description", ""))).strip()
                out.append({
                    "source": name,
                    "title": title,
                    "summary": summary[:200],
                    "published": entry.get("published", ""),
                    "link": entry.get("link", ""),
                })
            return out
        except Exception:
            return []


async def gather_news() -> list[dict]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    }
    async with httpx.AsyncClient(headers=headers) as http:
        tasks = [fetch_feed(n, u, http) for n, u in FEEDS.items()]
        results = await asyncio.gather(*tasks)

    articles = []
    seen_titles: set[str] = set()
    for batch in results:
        for art in batch:
            norm = re.sub(r"\W+", "", art["title"].lower())[:60]
            if norm not in seen_titles:
                seen_titles.add(norm)
                articles.append(art)

    return assess_credibility(articles)


# ─── FAKE-NEWS / CREDIBILITY FILTER ──────────────────────────────────────────
# Clickbait / sensationalism markers — hallmark of low-quality or fake content.
CLICKBAIT_MARKERS = (
    "you won't believe", "you wont believe", "shocking", "mind-blowing", "jaw-dropping",
    "this one trick", "doctors hate", "what happens next", "will blow your mind",
    "secret that", "they don't want you", "get rich", "make millions", "millionaire overnight",
    "guaranteed profit", "guaranteed returns", "100x", "1000x", "to the moon", "next bitcoin",
    "can't miss", "must buy now", "explodes", "skyrockets", "going parabolic", "free money",
)
_HYPE_RE = re.compile(r"(!!!|\?!|\?\?\?|\$\$\$)")
# Tokens that signal an unverified claim (needs corroboration before trusting).
_RUMOR_RE = re.compile(r"\b(rumor|rumour|alleged|unconfirmed|leak(?:ed)?|sources say|reportedly|speculat)\w*\b", re.I)


def assess_credibility(articles: list[dict]) -> list[dict]:
    """Tag each article with a credibility score and drop obvious junk.

    Score factors: source tier, clickbait/hype markers, unverified-rumor phrasing.
    Pure-sentiment-source + clickbait items are dropped (likely noise/fake)."""
    kept = []
    for art in articles:
        src = art.get("source", "")
        title = art.get("title", "")
        text = (title + " " + art.get("summary", "")).lower()

        if src in TIER1_SOURCES:
            score = 3.0
        elif src in TIER2_SOURCES:
            score = 2.0
        elif src in SENTIMENT_SOURCES:
            score = 0.6
        else:
            score = 1.3

        clickbait = sum(1 for m in CLICKBAIT_MARKERS if m in text)
        if clickbait:
            score -= 1.2 * clickbait
        if _HYPE_RE.search(title):
            score -= 0.8
        # ALL-CAPS shouting (>=2 long all-caps words, excluding known tickers/acronyms)
        caps = re.findall(r"\b[A-Z]{4,}\b", title)
        if len(caps) >= 2:
            score -= 0.5
        is_rumor = bool(_RUMOR_RE.search(text))
        if is_rumor and src not in TIER1_SOURCES:
            score -= 0.6  # unverified claim from non-wire source

        art["cred_score"] = round(score, 2)
        art["is_rumor"] = is_rumor
        art["credibility"] = "high" if score >= 2.5 else "medium" if score >= 1.2 else "low"

        # Drop only the clear garbage: untrusted/sentiment source AND clickbait.
        if src not in TIER1_SOURCES and src not in TIER2_SOURCES and clickbait and score < 0.5:
            continue
        kept.append(art)

    return kept


def build_corroboration_map(articles: list[dict]) -> dict:
    """Count distinct sources per entity, weighted by source tier and high-impact status."""
    STOP = {
        "the","and","for","are","but","not","you","all","can","was","one","our",
        "out","had","him","his","how","its","who","did","get","has","may","new",
        "now","old","see","two","way","with","that","have","this","from","they",
        "will","been","each","which","their","there","what","said","also","when",
        "than","into","more","some","just","first","could","after","over","most",
        "about","says","year","last","were","would","been","this","says","amid",
        "high","low","than","next","show","week","days","more","make","take",
    }
    entity_sources: dict[str, set] = {}
    entity_tier1: dict[str, int] = {}
    entity_high_impact: dict[str, bool] = {}
    entity_credible_srcs: dict[str, set] = {}  # non-sentiment sources only

    for art in articles:
        src = art["source"]
        text = art["title"] + " " + art.get("summary", "")
        proper = re.findall(r'\b[A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z]{2,})*\b', art["title"])
        words = re.findall(r'\b[a-z]{4,}\b', text.lower())
        entities = set()
        for p in proper:
            if p.lower() not in STOP and len(p) > 3:
                entities.add(p)
        for w in words:
            if w not in STOP:
                entities.add(w)
        for e in entities:
            entity_sources.setdefault(e, set()).add(src)
            if src in TIER1_SOURCES:
                entity_tier1[e] = entity_tier1.get(e, 0) + 1
            if src not in SENTIMENT_SOURCES:
                entity_credible_srcs.setdefault(e, set()).add(src)
            if any(p.lower() in e.lower() for p in HIGH_IMPACT_PERSONS):
                entity_high_impact[e] = True

    # Score: base count + tier1 bonus + high-impact bonus.
    # Anti-fake-news gate: require >=2 NON-sentiment sources, else it's rumor-level — skip.
    scored = {}
    for e, srcs in entity_sources.items():
        base = len(srcs)
        if base < 3:
            continue
        if len(entity_credible_srcs.get(e, set())) < 2:
            continue  # only echoed by Reddit/ZeroHedge-type sources → not corroborated fact
        t1_bonus = entity_tier1.get(e, 0) * 0.5
        hi_bonus = 2.0 if entity_high_impact.get(e) else 0.0
        scored[e] = round(base + t1_bonus + hi_bonus, 1)

    top = sorted(scored.items(), key=lambda x: -x[1])[:30]
    return dict(top)


# ─── ENTITY GRAPH ────────────────────────────────────────────────────────────

# Heuristic classifier keywords
_POLITICAL_TITLES = {
    "President", "Vice", "Senator", "Representative", "Governor", "Minister",
    "Secretary", "Chancellor", "Premier", "Parliament", "Congress", "Senate",
    "Treasury", "Federal", "Reserve", "Fed", "White", "House", "Biden", "Trump",
    "Harris", "Xi", "Putin", "Macron", "Scholz", "Sunak", "Draghi", "Lagarde",
}
_COMPANY_SUFFIXES = {
    "Inc", "Corp", "Ltd", "LLC", "Group", "Bank", "Fund", "Capital",
    "Holdings", "Technologies", "Systems", "Energy", "Motors", "Financial",
    "Investment", "Ventures", "Partners", "Solutions", "Networks",
}
_SCIENCE_MARKERS = {
    "Lab", "Labs", "Institute", "University", "Research", "Science",
    "Foundation", "Academy", "Nobel", "MIT", "Harvard", "Stanford",
    "NASA", "CERN", "WHO", "CDC", "NIH",
}
_NOISE_ENTITIES = {
    "The", "This", "That", "These", "Those", "When", "Where", "What", "Which",
    "After", "Before", "During", "While", "Here", "There", "Some", "Many",
    "Most", "More", "Less", "Such", "Each", "Both", "From", "With", "Into",
    "Over", "Under", "About", "Through", "Between", "Against", "Without",
    "January", "February", "March", "April", "June", "July", "August",
    "September", "October", "November", "December", "Monday", "Tuesday",
    "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
}


def _detect_entity_type(name: str) -> str:
    parts = name.split()
    for suf in _COMPANY_SUFFIXES:
        if suf in parts:
            return "company"
    for kw in _SCIENCE_MARKERS:
        if kw in parts:
            return "science"
    for kw in _POLITICAL_TITLES:
        if kw in parts:
            return "political"
    if len(parts) == 1 and name.isupper() and 2 <= len(name) <= 5:
        return "ticker"
    if len(parts) >= 2:
        return "person_or_org"
    return "concept"


def build_entity_graph(articles: list[dict]):
    """Build weighted co-mention graph. No AI needed."""
    if not HAS_NX:
        return None

    G = nx.Graph()

    for art in articles:
        src = art["source"]
        proper = re.findall(r'\b[A-Z][a-zA-Z]{2,}(?:\s+[A-Z][a-zA-Z]{2,})*\b', art["title"])
        entities = [p for p in proper if p not in _NOISE_ENTITIES and len(p) > 3]

        for e in entities:
            is_hi = any(p.lower() in e.lower() for p in HIGH_IMPACT_PERSONS)
            if not G.has_node(e):
                etype = "high_impact" if is_hi else _detect_entity_type(e)
                G.add_node(e, mentions=0, sources=set(), type=etype, high_impact=is_hi)
            G.nodes[e]["mentions"] += 1
            G.nodes[e]["sources"].add(src)

        # co-mention edges within same article
        seen = list(dict.fromkeys(entities))  # deduplicate, preserve order
        for i, e1 in enumerate(seen):
            for e2 in seen[i + 1:]:
                if G.has_edge(e1, e2):
                    G[e1][e2]["weight"] += 1
                    G[e1][e2]["sources"].add(src)
                else:
                    G.add_edge(e1, e2, weight=1, sources={src})

    return G


def _graph_context_for_gpt(G) -> str:
    """Summarize top graph nodes — high-impact persons first."""
    if not G or G.number_of_nodes() == 0:
        return ""
    nodes = list(G.nodes(data=True))
    # High-impact persons first, then by source count
    hi = [(n, d) for n, d in nodes if d.get("high_impact")]
    rest = [(n, d) for n, d in nodes if not d.get("high_impact")]
    hi.sort(key=lambda x: -len(x[1].get("sources", set())))
    rest.sort(key=lambda x: -len(x[1].get("sources", set())))
    top = (hi[:6] + rest[:6])[:12]

    lines = []
    if hi:
        lines.append(f"PERSONAS DE ALTO IMPACTO ({len(hi)} detectadas):")
        for node, data in hi[:6]:
            src_n = len(data.get("sources", set()))
            neighbors = sorted(G[node].items(), key=lambda x: -x[1].get("weight", 0))[:3]
            nbr_str = ", ".join(n for n, _ in neighbors) or "—"
            lines.append(f"  ★ {node} — {src_n} fuentes → {nbr_str}")
    lines.append("TOP ENTIDADES:")
    for node, data in rest[:6]:
        src_n = len(data.get("sources", set()))
        deg = G.degree(node)
        lines.append(f"  • {node} [{data.get('type','?')}] — {src_n} fuentes, {deg} cx")
    return "\n".join(lines)


def synthesize_no_ai(articles: list[dict], G) -> dict:
    """Investment signals from graph — no OpenAI call."""
    BULLISH_W = {"surge", "rally", "gain", "rise", "soar", "record", "growth", "boom", "strong", "jump", "climb"}
    BEARISH_W = {"fall", "drop", "crash", "decline", "plunge", "loss", "weak", "recession", "crisis", "cut", "slump"}

    all_titles = " ".join(a["title"].lower() for a in articles)
    bull = sum(1 for w in BULLISH_W if w in all_titles)
    bear = sum(1 for w in BEARISH_W if w in all_titles)
    mood = "bullish" if bull > bear * 1.3 else ("bearish" if bear > bull * 1.3 else "neutral")

    corroboration = build_corroboration_map(articles)
    key_themes = list(corroboration.keys())[:5]

    investments = []
    if G and G.number_of_nodes() > 0:
        # Score = source_diversity × log(1 + degree)
        import math
        scored = sorted(
            [(n, len(d.get("sources", set())), G.degree(n), d)
             for n, d in G.nodes(data=True)
             if len(d.get("sources", set())) >= 3],
            key=lambda x: -(x[1] * math.log1p(x[2]))
        )[:10]

        max_score = scored[0][1] * math.log1p(scored[0][2]) if scored else 1

        _type_to_asset = {
            "ticker": "stock", "company": "stock",
            "political": "index", "science": "etf",
            "person_or_org": "index", "concept": "etf",
        }

        for node, src_n, deg, data in scored:
            score = src_n * math.log1p(deg)
            priority = max(1, min(10, round((score / max_score) * 10)))
            neighbors = sorted(G[node].items(), key=lambda x: -x[1].get("weight", 0))[:3]
            nbr_names = [n for n, _ in neighbors]
            entity_type = data.get("type", "concept")
            srcs_list = list(data.get("sources", set()))[:3]

            investments.append({
                "asset": node,
                "type": _type_to_asset.get(entity_type, "etf"),
                "priority": priority,
                "signal": "watch",
                "rationale": (
                    f"Detectado en {src_n} fuentes independientes con {deg} co-menciones "
                    f"en la red. Tipo: {entity_type}. "
                    f"Relacionado con: {', '.join(nbr_names) or 'múltiples activos'}."
                ),
                "timeframe": "short",
                "risk": "medium",
                "catalysts": [
                    f"Alta presencia mediática: {src_n} fuentes",
                    f"{deg} conexiones en grafo de co-menciones",
                ],
                "examples": [node] if entity_type == "ticker" else [],
                "portfolio_weight": f"{round(score / max_score * 15)}%",
                "entry_strategy": "Monitorear evolución en próximas 24-48h",
                "stop_loss": "N/A — señal de vigilancia",
                "target": "N/A — señal de vigilancia",
                "corroboration_score": min(10, src_n),
                "sources_confirming": srcs_list,
            })

    n_nodes = G.number_of_nodes() if G else 0
    n_edges = G.number_of_edges() if G else 0

    return {
        "macro_regime": "uncertainty",
        "macro_regime_description": (
            f"Análisis de grafo puro: {n_nodes} entidades, {n_edges} conexiones detectadas "
            f"en {len(articles)} artículos. Sin procesamiento de IA."
        ),
        "market_mood": mood,
        "market_summary": (
            f"El grafo de co-menciones identificó {n_nodes} entidades únicas con "
            f"{n_edges} relaciones. Temas más corroborados: {', '.join(key_themes[:3])}. "
            f"Señales {'alcistas' if mood == 'bullish' else 'bajistas' if mood == 'bearish' else 'mixtas'} "
            f"({bull} bullish / {bear} bearish keywords)."
        ),
        "key_themes": key_themes,
        "sector_rotation": {"overweight": [], "underweight": []},
        "investments": investments,
        "macro_hedges": [],
        "risks": [
            "Análisis sin IA — sin interpretación cualitativa de contexto",
            "Señales basadas en frecuencia y co-menciones, no en fundamentales",
            "Entidades de alto perfil mediático no implican oportunidad de inversión",
        ],
        "watchlist": [],
        "disclaimer": (
            "Análisis automático sin IA. Basado únicamente en frecuencia y co-menciones "
            "en medios. No constituye asesoramiento financiero."
        ),
        "mode": "graph-only",
    }


# ─── EMAIL ALERTS ────────────────────────────────────────────────────────────

def send_buy_alert(investments: list[dict], generated_at: str) -> int:
    """Send Gmail alert for high-priority buy signals. Returns number of alerts sent."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD or not ALERT_RECIPIENT:
        return 0

    buys = [
        inv for inv in investments
        if inv.get("signal") == "buy" and inv.get("priority", 0) >= ALERT_MIN_PRIORITY
    ]
    if not buys:
        return 0

    # Build HTML email body
    _RISK_COLOR = {"low": "#10b981", "medium": "#f59e0b", "high": "#ef4444"}
    _TF_LABEL = {"short": "⚡ Corto plazo", "medium": "📅 Mediano plazo", "long": "🌳 Largo plazo"}

    rows = ""
    for inv in sorted(buys, key=lambda x: -x.get("priority", 0)):
        tickers = ", ".join(inv.get("examples", [])) or inv["asset"]
        risk = inv.get("risk", "medium")
        rc = _RISK_COLOR.get(risk, "#f59e0b")
        tf = _TF_LABEL.get(inv.get("timeframe", "short"), "—")
        target = inv.get("target", "—")
        stop = inv.get("stop_loss", "—")
        rows += f"""
        <tr>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45">
            <div style="font-weight:700;color:#e2e8f0;font-size:15px">{inv['asset']}</div>
            <div style="font-size:11px;color:#93c5fd;margin-top:2px">{tickers}</div>
          </td>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45;color:#22d3ee;font-size:22px;font-weight:800;text-align:center">{inv.get('priority',0)}<span style="font-size:11px;color:#64748b">/10</span></td>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45;color:{rc};font-weight:700;text-transform:uppercase;font-size:12px">{risk}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45;color:#94a3b8;font-size:12px">{tf}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45;color:#10b981;font-size:12px;font-weight:600">{target}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45;color:#ef4444;font-size:12px">{stop}</td>
          <td style="padding:10px 12px;border-bottom:1px solid #1f2d45;color:#94a3b8;font-size:12px;max-width:260px">{inv.get('rationale','')[:220]}</td>
        </tr>"""

    html = f"""
    <div style="font-family:-apple-system,sans-serif;background:#0a0e1a;color:#e2e8f0;padding:32px;max-width:920px">
      <h1 style="color:#3b82f6;margin-bottom:4px">📡 InvestAI — Señales de Compra</h1>
      <p style="color:#64748b;margin-bottom:24px">{generated_at}</p>
      <p style="margin-bottom:16px">{len(buys)} señal(es) BUY con prioridad ≥ {ALERT_MIN_PRIORITY}:</p>
      <table style="width:100%;border-collapse:collapse;background:#111827;border-radius:8px;overflow:hidden">
        <thead>
          <tr style="background:#1c2536">
            <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">ACTIVO / TICKERS</th>
            <th style="padding:10px 12px;text-align:center;color:#64748b;font-size:11px">PRIORIDAD</th>
            <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">RIESGO</th>
            <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">PLAZO</th>
            <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">TARGET</th>
            <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">STOP LOSS</th>
            <th style="padding:10px 12px;text-align:left;color:#64748b;font-size:11px">RAZÓN</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <p style="color:#64748b;font-size:11px;margin-top:24px">
        Análisis solo informativo. No constituye asesoramiento financiero.
      </p>
    </div>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📡 InvestAI — {len(buys)} señal(es) BUY (prioridad ≥{ALERT_MIN_PRIORITY})"
    msg["From"] = GMAIL_USER
    msg["To"] = ALERT_RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        return len(buys)
    except Exception as e:
        print(f"Gmail alert failed: {e}")
        return 0


def send_scheduled_digest(analysis: dict, articles: list[dict], trigger: str = "scheduled") -> bool:
    """Full digest email — sent every 3 days or on breaking news trigger."""
    if not GMAIL_USER or not GMAIL_APP_PASSWORD or not ALERT_RECIPIENT:
        return False

    investments = analysis.get("investments", [])
    mood = analysis.get("market_mood", "neutral")
    mood_icon = "📈" if mood == "bullish" else ("📉" if mood == "bearish" else "↔")
    mood_color = "#10b981" if mood == "bullish" else ("#ef4444" if mood == "bearish" else "#f59e0b")

    _RISK_COLOR = {"low": "#10b981", "medium": "#f59e0b", "high": "#ef4444"}
    _TF_LABEL = {"short": "Corto", "medium": "Mediano", "long": "Largo"}

    rows = ""
    for inv in sorted(investments, key=lambda x: -x.get("priority", 0)):
        sig = inv.get("signal", "watch").upper()
        sig_color = "#10b981" if sig == "BUY" else ("#ef4444" if sig == "SELL" else "#f59e0b")
        risk = inv.get("risk", "medium")
        rc = _RISK_COLOR.get(risk, "#f59e0b")
        rows += f"""<tr>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:#e2e8f0;font-weight:600">{inv['asset']}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:{sig_color};font-weight:700">{sig}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:#22d3ee;font-weight:700">{inv.get('priority',0)}/10</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:{rc};font-size:12px">{risk.upper()}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:#94a3b8;font-size:12px">{_TF_LABEL.get(inv.get('timeframe','short'),'—')}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:#10b981;font-size:12px">{inv.get('target','—')}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:#ef4444;font-size:12px">{inv.get('stop_loss','—')}</td>
          <td style="padding:8px 10px;border-bottom:1px solid #1f2d45;color:#94a3b8;font-size:11px">{inv.get('rationale','')[:160]}</td>
        </tr>"""

    risks_html = "".join(f"<li style='margin-bottom:6px'>{r}</li>" for r in analysis.get("risks", []))
    themes_html = "".join(f"<span style='background:#1c2536;border:1px solid #1f2d45;padding:3px 10px;border-radius:20px;font-size:12px;margin:3px;display:inline-block'>{t}</span>" for t in analysis.get("key_themes", []))
    trigger_badge = f"<span style='background:#7c3aed20;border:1px solid #7c3aed;padding:2px 10px;border-radius:4px;font-size:11px;color:#a78bfa'>⚡ BREAKING</span>" if trigger == "breaking" else "<span style='background:#1c2536;border:1px solid #1f2d45;padding:2px 10px;border-radius:4px;font-size:11px;color:#64748b'>🗓 PROGRAMADO</span>"

    html = f"""<div style="font-family:-apple-system,sans-serif;background:#0a0e1a;color:#e2e8f0;padding:32px;max-width:960px">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:24px">
        <h1 style="color:#3b82f6;margin:0">📡 InvestAI — Informe de Mercado</h1>
        {trigger_badge}
      </div>
      <p style="color:#64748b;margin-bottom:20px">{analysis.get('generated_at','')}</p>

      <div style="background:#111827;border:1px solid;border-radius:12px;padding:20px;margin-bottom:24px;border-color:{mood_color}40">
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px">
          <span style="font-size:20px">{mood_icon}</span>
          <span style="color:{mood_color};font-weight:700;text-transform:uppercase">{mood}</span>
          <span style="color:#64748b;font-size:13px;margin-left:auto">{analysis.get('macro_regime','').upper()}</span>
        </div>
        <p style="color:#94a3b8;font-size:14px;line-height:1.6;margin:0">{analysis.get('market_summary','')}</p>
        <div style="margin-top:12px">{themes_html}</div>
      </div>

      <h3 style="color:#e2e8f0;margin-bottom:12px">🎯 Oportunidades de Inversión</h3>
      <table style="width:100%;border-collapse:collapse;background:#111827;border-radius:8px;overflow:hidden;margin-bottom:24px">
        <thead><tr style="background:#1c2536">
          <th style="padding:8px 10px;text-align:left;color:#64748b;font-size:11px">ACTIVO</th>
          <th style="padding:8px 10px;color:#64748b;font-size:11px">SEÑAL</th>
          <th style="padding:8px 10px;color:#64748b;font-size:11px">PRIOR.</th>
          <th style="padding:8px 10px;color:#64748b;font-size:11px">RIESGO</th>
          <th style="padding:8px 10px;color:#64748b;font-size:11px">PLAZO</th>
          <th style="padding:8px 10px;color:#64748b;font-size:11px">TARGET</th>
          <th style="padding:8px 10px;color:#64748b;font-size:11px">STOP</th>
          <th style="padding:8px 10px;text-align:left;color:#64748b;font-size:11px">RAZÓN</th>
        </tr></thead>
        <tbody>{rows}</tbody>
      </table>

      <h3 style="color:#ef4444;margin-bottom:12px">⚠ Riesgos Principales</h3>
      <ul style="color:#94a3b8;font-size:13px;line-height:1.8;padding-left:20px">{risks_html}</ul>

      <p style="color:#374151;font-size:11px;margin-top:24px;border-top:1px solid #1f2d45;padding-top:12px">
        {analysis.get('disclaimer','')} Fuentes: {len(analysis.get('sources_used',[]))} | Artículos: {analysis.get('articles_fetched',0)}
      </p>
    </div>"""

    subject_tag = "⚡ BREAKING — " if trigger == "breaking" else "📊 Informe 3 días — "
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📡 InvestAI {subject_tag}{mood.upper()} | {len(investments)} oportunidades"
    msg["From"] = GMAIL_USER
    msg["To"] = ALERT_RECIPIENT
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_USER, GMAIL_APP_PASSWORD)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"Digest email failed: {e}")
        return False


# ─── ANALYSIS ────────────────────────────────────────────────────────────────
def analyze_with_gpt(articles: list[dict], graph_context: str = "") -> dict:
    corroboration = build_corroboration_map(articles)
    corr_text = ", ".join(f'"{e}"({n})' for e, n in corroboration.items())

    # Rank articles by corroboration signal — high-overlap articles first
    corr_keys = set(corroboration.keys())
    def _score(a):
        t = (a["title"] + " " + a.get("summary", "")).lower()
        return sum(1 for e in corr_keys if e.lower() in t)

    ranked = sorted([a for a in articles if a["title"]], key=_score, reverse=True)

    # Top 30: title + summary. Next 70: title only. Total ≈1200 tokens vs 3500
    detail = "\n\n".join(
        f"[{a['source']}] {a['title']}\n{a['summary'][:150]}"
        for a in ranked[:30]
    )
    titles = "\n".join(f"[{a['source']}] {a['title']}" for a in ranked[30:100])
    articles_text = detail + ("\n\n---\n" + titles if titles else "")

    cred_counts = {"high": 0, "medium": 0, "low": 0}
    for a in articles:
        cred_counts[a.get("credibility", "medium")] = cred_counts.get(a.get("credibility", "medium"), 0) + 1
    cred_section = (f"\nCREDIBILIDAD FUENTES: {cred_counts['high']} alta / "
                    f"{cred_counts['medium']} media / {cred_counts['low']} baja. "
                    "La corroboración multi-fuente de abajo YA excluye rumores de fuentes no verificadas.\n")

    market = fetch_market_snapshot()
    market_section = f"\nMERCADO AHORA:{market}\n" if market else ""
    graph_section = f"\nGRAFO:{graph_context}\n" if graph_context else ""

    system = (
        "Eres un gestor de patrimonio top con 20 años de experiencia, especializado en "
        "construcción de riqueza a LARGO PLAZO mediante aportes mensuales (DCA) y capitalización compuesta. "
        "Piensas en escenarios, no solo en datos actuales. "
        "Tu objetivo: MAXIMIZAR el patrimonio del inversor en horizonte de 5-15 años. "
        "Filosofía núcleo-satélite: un NÚCLEO diversificado de bajo costo y baja rotación (índices amplios, "
        "megatendencias estructurales) que se compra cada mes pase lo que pase, más SATÉLITES de alta convicción "
        "para amplificar el retorno. Priorizas tendencias seculares duraderas sobre el ruido de corto plazo. "
        "Eres audaz pero disciplinado: aprovechas las caídas para acumular más, no para vender con pánico. "
        "Solo JSON válido. Español."
    )

    user = f"""Fecha:{datetime.now().strftime('%Y-%m-%d %H:%M')} Fuentes:{len(set(a['source'] for a in articles))} Artículos:{len(articles)}
MERCADO ACTUAL:{market_section.strip()}
{cred_section}CORROBORACIÓN MULTI-FUENTE (ya filtrada de fake-news):{corr_text}
{graph_section}
NOTICIAS CLAVE:
{articles_text[:6000]}

MODO DE ANÁLISIS — RIQUEZA A LARGO PLAZO CON APORTES MENSUALES (DCA):
El inversor ahorra cada mes y reinvierte. NO le interesa el trading de corto plazo. Horizonte: 5-15 años.
1. Identifica las 3 narrativas dominantes con mayor corroboración multi-fuente.
2. Distingue señal estructural (megatendencias de años: IA, energía, demografía, desdolarización) del ruido pasajero.
3. NÚCLEO (long_term_core): la base que se compra TODOS los meses sin importar el titular. Índices amplios y diversificados + 1-2 megatendencias duraderas. Baja rotación, máxima capitalización compuesta. Esto debe ser ESTABLE entre análisis.
4. SATÉLITES (investments): apuestas de mayor convicción para amplificar retorno. Prioriza timeframe "long" y "medium"; usa "short" solo si la señal es excepcional.
5. Para cada idea imagina escenario BASE (50%), ALCISTA (30%), BAJISTA (20%). En las caídas, el plan es ACUMULAR más barato, no vender.
6. expected_annual_return: estima retorno anualizado realista a largo plazo por activo (ej "8-12%").
7. Objetivo: máximo patrimonio compuesto a 10 años. Disciplina sobre euforia.
8. SEÑALES DE COMPRA HONESTAS: solo marca signal="buy" si la tesis está corroborada por >=2 fuentes fiables. corroboration_score (0-10) debe reflejar la corroboración REAL — no infles. Una señal "buy" con corroboración débil será degradada automáticamente a "watch", así que sé riguroso. Si dudas, usa "watch".

JSON (sin markdown):
{{"macro_regime":"risk-on|risk-off|stagflation|reflation|deflation|recovery|uncertainty","macro_regime_description":"str — incluye qué escenario domina y por qué","market_mood":"bullish|bearish|neutral","market_summary":"str — qué está pasando, qué VIENE, qué precio ya descuenta el mercado y qué no","key_themes":["t1","t2","t3","t4","t5"],"secular_trends":["megatendencia estructural de varios años + por qué perdura","t2","t3"],"sector_rotation":{{"overweight":["s1","s2"],"underweight":["s1","s2"]}},"long_term_core":[{{"asset":"NOMBRE ESPECÍFICO (ej: MSCI World, S&P 500, Bitcoin)","type":"etf|index|crypto|stock","examples":["TICKER ej VWCE, SPY, IWDA"],"core_weight":"X%","rationale":"por qué es base de cartera a 10 años","expected_annual_return":"8-12%","risk":"low|medium|high"}}],"investments":[{{"asset":"NOMBRE ESPECÍFICO (ej: NVIDIA, Bitcoin, Tesla, Gold)","type":"stock|etf|crypto|commodity|bond|real_estate|currency|index","priority":8,"signal":"buy|hold|sell|watch","rationale":"str — qué tendencia estructural lo respalda y por qué compone a largo plazo","timeframe":"short|medium|long","risk":"low|medium|high","expected_annual_return":"10-15%","catalysts":["catalizador concreto con fecha o trigger","c2"],"examples":["TICKER1","TICKER2"],"portfolio_weight":"X%","entry_strategy":"str — para DCA: \"comprar cada mes\" o condición de acumulación extra","stop_loss":"str — nivel o tesis de salida (a largo plazo: qué invalidaría la idea)","target":"str — precio/retorno objetivo en escenario base y alcista a 3-5 años","corroboration_score":8,"sources_confirming":["f1","f2"]}}],"dca_guidance":"str — cómo repartir el aporte mensual entre núcleo y satélites y cuándo acumular extra","macro_hedges":["cobertura específica 1","cobertura 2"],"risks":["riesgo concreto con impacto estimado","r2","r3"],"watchlist":["activo + razón + trigger a vigilar","w2"],"disclaimer":"str"}}
3-4 activos en long_term_core (suma core_weight ≈ 100%). 5-8 investments (satélites).
Prioridad 10=máxima convicción. CRÍTICO: "asset" = nombre ESPECÍFICO — NUNCA sectores genéricos. "examples" = tickers reales."""

    resp = get_openai_client().chat.completions.create(
        model="gpt-4.1-mini",
        max_tokens=3500,
        temperature=0.35,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
    )
    result = json.loads(resp.choices[0].message.content)
    result["mode"] = "ai"
    return result


# ─── CHAT ────────────────────────────────────────────────────────────────────
class ChatMsg(BaseModel):
    role: str
    content: str

class ChatReq(BaseModel):
    message: str
    history: List[ChatMsg] = []


@app.post("/api/chat")
async def chat(req: ChatReq):
    analysis_ctx = json.dumps(_state["analysis"], ensure_ascii=False)[:4000]
    article_titles = "\n".join(
        f"[{a['source']}] {a['title']}"
        for a in _state["articles"][:60]
    )

    system_msg = f"""Asesor financiero senior. Español. Directo y accionable. Markdown.
ANÁLISIS:{analysis_ctx}
NOTICIAS:{article_titles}"""

    messages = [{"role": "system", "content": system_msg}]
    for h in req.history[-12:]:
        messages.append({"role": h.role, "content": h.content})
    messages.append({"role": "user", "content": req.message})

    def stream_response():
        try:
            stream = get_openai_client().chat.completions.create(
                model="gpt-4.1-mini",
                messages=messages,
                stream=True,
                max_tokens=800,
            )
            for chunk in stream:
                delta = chunk.choices[0].delta
                if delta.content:
                    yield f"data: {json.dumps({'content': delta.content})}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(stream_response(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ─── PERSISTENCE TRACKING ────────────────────────────────────────────────────

def _normalize_asset(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())

def tag_persistence(investments: list[dict], history: list[dict]) -> list[dict]:
    """Tag each investment with how many previous analyses included the same asset."""
    # Build lookup: normalized_asset → max streak count from history
    historical: dict[str, int] = {}
    for past in history[-4:]:  # look at last 4 analyses
        for inv in past.get("investments", []):
            key = _normalize_asset(inv.get("asset", ""))
            historical[key] = historical.get(key, 0) + 1

    for inv in investments:
        key = _normalize_asset(inv.get("asset", ""))
        streak = historical.get(key, 0)
        inv["streak"] = streak  # 0 = new, 1 = seen once before, 2+ = confirmed
    return investments


def validate_buy_signals(investments: list[dict], corroboration: dict) -> list[dict]:
    """Review every BUY before it reaches the user. Downgrade weak/uncorroborated buys
    to WATCH so alerts only fire on genuinely confirmed signals.

    A BUY survives only if: corroboration_score >= 6 AND >=2 confirming sources.
    High-risk buys need corroboration_score >= 7. Anything weaker → watch + reason."""
    corr_lower = {k.lower(): v for k, v in corroboration.items()}
    for inv in investments:
        if inv.get("signal") != "buy":
            continue
        try:
            cs = float(inv.get("corroboration_score", 0))
        except Exception:
            cs = 0.0
        confirming = inv.get("sources_confirming", []) or []
        risk = inv.get("risk", "medium")

        # Cross-check: is the asset name actually present in the multi-source consensus?
        name = inv.get("asset", "").lower()
        in_consensus = any(tok in corr_lower for tok in name.split()) or name in corr_lower

        min_cs = 7.0 if risk == "high" else 6.0
        reasons = []
        if cs < min_cs:
            reasons.append(f"corroboración {cs:g}<{min_cs:g}")
        if len(confirming) < 2:
            reasons.append("menos de 2 fuentes confirman")
        if not in_consensus and cs < 8:
            reasons.append("no aparece en el consenso multi-fuente")

        if reasons:
            inv["signal"] = "watch"
            inv["downgraded_from"] = "buy"
            inv["downgrade_reason"] = "; ".join(reasons)
        else:
            inv["validated_buy"] = True
    return investments


def save_to_history(analysis: dict) -> None:
    snapshot = {
        "generated_at": analysis.get("generated_at"),
        "investments": [
            {"asset": i.get("asset"), "signal": i.get("signal"), "priority": i.get("priority")}
            for i in analysis.get("investments", [])
        ],
    }
    _state["history"].append(snapshot)
    _state["history"] = _state["history"][-5:]  # keep last 5


# ─── ENDPOINTS ───────────────────────────────────────────────────────────────
CACHE_TTL_HOURS = 2

@app.get("/api/analyze")
async def analyze(force: bool = False):
    # Return cached analysis if < CACHE_TTL_HOURS old and force=False
    if not force and _state["analysis"] and _state["fetched_at"]:
        try:
            age = (datetime.now() - datetime.fromisoformat(_state["fetched_at"])).total_seconds() / 3600
            if age < CACHE_TTL_HOURS:
                cached = dict(_state["analysis"])
                cached["cached"] = True
                cached["cache_age_min"] = round(age * 60)
                return cached
        except Exception:
            pass

    articles = await gather_news()
    if not articles:
        return JSONResponse({"error": "No se pudieron obtener noticias"}, status_code=503)

    G = build_entity_graph(articles)
    _state["graph"] = G

    try:
        analysis = analyze_with_gpt(articles, _graph_context_for_gpt(G))
    except Exception as e:
        return JSONResponse({"error": str(e), "type": type(e).__name__}, status_code=500)

    analysis["articles_fetched"] = len(articles)
    analysis["sources_used"] = sorted(set(a["source"] for a in articles))
    analysis["generated_at"] = datetime.now().isoformat()

    # Tag each investment with persistence streak from history
    analysis["investments"] = tag_persistence(analysis.get("investments", []), _state["history"])
    # Review BUY signals — downgrade uncorroborated ones to WATCH before they reach alerts
    analysis["investments"] = validate_buy_signals(analysis["investments"], build_corroboration_map(articles))

    _state["analysis"] = analysis
    _state["articles"] = articles
    _state["fetched_at"] = datetime.now().isoformat()
    save_to_history(analysis)

    sent = send_buy_alert(analysis.get("investments", []), analysis["generated_at"])
    if sent:
        analysis["alerts_sent"] = sent

    return analysis


@app.get("/api/analyze-no-ai")
async def analyze_no_ai():
    articles = await gather_news()
    if not articles:
        return JSONResponse({"error": "No se pudieron obtener noticias"}, status_code=503)

    G = build_entity_graph(articles)
    _state["graph"] = G

    analysis = synthesize_no_ai(articles, G)
    analysis["articles_fetched"] = len(articles)
    analysis["sources_used"] = sorted(set(a["source"] for a in articles))
    analysis["generated_at"] = datetime.now().isoformat()

    _state["analysis"] = analysis
    _state["articles"] = articles
    _state["fetched_at"] = datetime.now().isoformat()

    return analysis


@app.get("/api/graph-data")
async def graph_data():
    G = _state.get("graph")
    if not G or G.number_of_nodes() == 0:
        return JSONResponse({"error": "Sin grafo. Ejecuta /api/analyze o /api/analyze-no-ai primero."}, status_code=404)

    nodes = [
        {
            "id": n,
            "label": n,
            "type": G.nodes[n].get("type", "concept"),
            "mentions": G.nodes[n].get("mentions", 0),
            "sources": len(G.nodes[n].get("sources", set())),
            "degree": G.degree(n),
        }
        for n in G.nodes()
        if G.nodes[n].get("mentions", 0) >= 2
    ]
    edges = [
        {
            "source": u,
            "target": v,
            "weight": d.get("weight", 1),
            "sources": len(d.get("sources", set())),
        }
        for u, v, d in G.edges(data=True)
        if d.get("weight", 0) >= 2
    ]
    return {"nodes": nodes, "edges": edges, "total_nodes": G.number_of_nodes(), "total_edges": G.number_of_edges()}


@app.get("/api/status")
async def status():
    G = _state.get("graph")
    return {
        "has_analysis": bool(_state["analysis"]),
        "articles": len(_state["articles"]),
        "fetched_at": _state["fetched_at"],
        "graph_nodes": G.number_of_nodes() if G else 0,
        "graph_edges": G.number_of_edges() if G else 0,
        "mode": _state["analysis"].get("mode", "none") if _state["analysis"] else "none",
    }


@app.get("/api/portfolio")
async def portfolio(budget: float = 1000.0, currency: str = "USD"):
    """Given a budget, return personalized allocation based on current analysis."""
    analysis = _state.get("analysis")
    if not analysis or not analysis.get("investments"):
        return JSONResponse(
            {"error": "Sin análisis. Ejecuta 'Analizar' primero."},
            status_code=400
        )

    investments = analysis["investments"]
    # Use AI-assigned portfolio_weight if available, else distribute by priority
    weights = []
    for inv in investments:
        pw = inv.get("portfolio_weight", "")
        try:
            w = float(re.sub(r"[^0-9.]", "", str(pw)))
        except Exception:
            w = 0.0
        weights.append(w)

    total_w = sum(weights)
    if total_w <= 0:
        # Fallback: distribute by priority score
        priorities = [inv.get("priority", 5) for inv in investments]
        total_p = sum(priorities)
        weights = [p / total_p * 100 for p in priorities]
        total_w = 100.0

    allocations = []
    for inv, w in zip(investments, weights):
        pct = round(w / total_w * 100, 1)
        amount = round(budget * pct / 100, 2)
        if amount <= 0:
            continue
        allocations.append({
            "asset": inv["asset"],
            "type": inv["type"],
            "signal": inv["signal"],
            "priority": inv["priority"],
            "risk": inv["risk"],
            "timeframe": inv["timeframe"],
            "pct": pct,
            "amount": amount,
            "currency": currency,
            "examples": inv.get("examples", []),
            "rationale": inv.get("rationale", ""),
            "entry_strategy": inv.get("entry_strategy", ""),
            "stop_loss": inv.get("stop_loss", ""),
            "target": inv.get("target", ""),
        })

    # Sort by amount desc
    allocations.sort(key=lambda x: -x["amount"])

    return {
        "budget": budget,
        "currency": currency,
        "total_allocated": round(sum(a["amount"] for a in allocations), 2),
        "allocations": allocations,
        "macro_regime": analysis.get("macro_regime", ""),
        "market_mood": analysis.get("market_mood", ""),
        "generated_at": analysis.get("generated_at", ""),
        "disclaimer": analysis.get("disclaimer", ""),
    }


# ─── DCA / LONG-TERM PLAN ─────────────────────────────────────────────────────

# Profile → (core %, satellite %). Core = diversified low-churn base bought every month.
RISK_PROFILES = {
    "conservador": (0.80, 0.20),
    "balanceado":  (0.60, 0.40),
    "agresivo":    (0.40, 0.60),
}

# Fallback core if GPT analysis lacks long_term_core (broad diversified, low cost).
DEFAULT_CORE = [
    {"asset": "MSCI World", "type": "etf", "examples": ["VWCE", "IWDA"], "core_weight": "55%",
     "rationale": "Renta variable global diversificada — base de capitalización compuesta a largo plazo.",
     "expected_annual_return": "7-9%", "risk": "medium"},
    {"asset": "S&P 500", "type": "etf", "examples": ["SPY", "CSPX"], "core_weight": "30%",
     "rationale": "Megacaps EEUU, motor de innovación e IA.", "expected_annual_return": "8-10%", "risk": "medium"},
    {"asset": "Bitcoin", "type": "crypto", "examples": ["BTC"], "core_weight": "15%",
     "rationale": "Activo escaso no correlacionado — asimetría al alza a 10 años.",
     "expected_annual_return": "15-25%", "risk": "high"},
]


def _parse_pct(s, default=0.0) -> float:
    try:
        return float(re.sub(r"[^0-9.]", "", str(s)))
    except Exception:
        return default


def _parse_return(s, default=8.0) -> float:
    """'8-12%' → 10.0 (midpoint). '12%' → 12.0."""
    nums = re.findall(r"[0-9]+\.?[0-9]*", str(s))
    if not nums:
        return default
    vals = [float(n) for n in nums[:2]]
    return sum(vals) / len(vals)


# Map common asset names → a tradeable ticker for volatility lookup.
_NAME_TO_TICKER = {
    "bitcoin": "BTC-USD", "btc": "BTC-USD", "ethereum": "ETH-USD", "eth": "ETH-USD",
    "s&p 500": "SPY", "sp500": "SPY", "s&p500": "SPY", "nasdaq": "QQQ",
    "msci world": "URTH", "gold": "GLD", "oro": "GLD", "silver": "SLV", "plata": "SLV",
    "oil": "CL=F", "petróleo": "CL=F", "nvidia": "NVDA", "apple": "AAPL", "tesla": "TSLA",
    "microsoft": "MSFT", "amazon": "AMZN", "google": "GOOGL", "alphabet": "GOOGL",
    "meta": "META", "treasuries": "TLT", "bonds": "TLT", "bonos": "TLT",
}
_RISK_CACHE: dict = {}  # ticker → (annual_return_pct, annual_vol_pct, ts)
_RISK_TTL = 6 * 3600


def _resolve_ticker(asset: str, examples: list) -> str | None:
    for ex in (examples or []):
        t = str(ex).strip().upper()
        if t and re.fullmatch(r"[A-Z0-9.\-=^]{1,12}", t):
            return t
    return _NAME_TO_TICKER.get((asset or "").lower().strip())


def _ticker_stats(ticker: str):
    """Annualized (return%, volatility%) from ~2y of daily closes. Cached. None on failure."""
    if not HAS_YF or not ticker:
        return None
    now = datetime.now().timestamp()
    hit = _RISK_CACHE.get(ticker)
    if hit and now - hit[2] < _RISK_TTL:
        return hit[0], hit[1]
    try:
        hist = yf.Ticker(ticker).history(period="2y", interval="1d")
        closes = hist["Close"].dropna()
        if len(closes) < 60:
            return None
        rets = closes.pct_change().dropna()
        vol = float(rets.std() * (252 ** 0.5) * 100)
        cagr = float((closes.iloc[-1] / closes.iloc[0]) ** (252 / len(closes)) - 1) * 100
        _RISK_CACHE[ticker] = (round(cagr, 1), round(vol, 1), now)
        return round(cagr, 1), round(vol, 1)
    except Exception:
        return None


def compute_portfolio_risk(holdings: list[dict]) -> dict:
    """Given [{asset, examples, monthly_amount, expected_annual_return}], derive a
    market-data-grounded volatility & risk rating. Falls back gracefully if yfinance down."""
    total = sum(h.get("monthly_amount", 0) for h in holdings) or 1
    wvol = 0.0
    whist_ret = 0.0
    covered = 0.0
    per_asset = []
    for h in holdings:
        w = h.get("monthly_amount", 0) / total
        ticker = _resolve_ticker(h.get("asset", ""), h.get("examples", []))
        stats = _ticker_stats(ticker) if ticker else None
        if stats:
            ret, vol = stats
            wvol += w * vol
            whist_ret += w * ret
            covered += w
            per_asset.append({"asset": h.get("asset"), "ticker": ticker,
                              "annual_vol_pct": vol, "hist_return_pct": ret})
        else:
            # fallback vol by qualitative risk
            qv = {"low": 12, "medium": 22, "high": 55}.get(h.get("risk", "medium"), 22)
            wvol += w * qv
    rating = "low" if wvol < 15 else "medium" if wvol < 30 else "high"
    return {
        "portfolio_vol_pct": round(wvol, 1),
        "coverage": round(covered, 2),
        "hist_return_pct": round(whist_ret / covered, 1) if covered > 0 else None,
        "rating": rating,
        "per_asset": per_asset,
    }


def _project_dca(current: float, monthly: float, years: int, annual_pct: float) -> dict:
    """Future value of an initial lump + monthly contributions (annuity-due) compounding annually."""
    i = annual_pct / 100 / 12
    n = years * 12
    if i == 0:
        fv = current + monthly * n
    else:
        fv = current * (1 + i) ** n + monthly * (((1 + i) ** n - 1) / i) * (1 + i)
    contributed = current + monthly * n
    return {
        "annual_return_pct": round(annual_pct, 1),
        "future_value": round(fv, 2),
        "contributed": round(contributed, 2),
        "profit": round(fv - contributed, 2),
        "multiple": round(fv / contributed, 2) if contributed > 0 else 0,
    }


@app.get("/api/dca")
async def dca(monthly: float = 200.0, years: int = 10, current: float = 0.0,
              profile: str = "balanceado", currency: str = "USD"):
    """Long-term monthly savings plan: núcleo/satélite split + compound-interest projection."""
    analysis = _state.get("analysis")
    if not analysis or not analysis.get("investments"):
        return JSONResponse({"error": "Sin análisis. Ejecuta 'Analizar' primero."}, status_code=400)

    monthly = max(0.0, float(monthly))
    current = max(0.0, float(current))
    years = max(1, min(40, int(years)))
    core_share, sat_share = RISK_PROFILES.get(profile, RISK_PROFILES["balanceado"])

    # ── Núcleo: estable, baja rotación ──
    core_src = analysis.get("long_term_core") or DEFAULT_CORE
    core_weights = [_parse_pct(c.get("core_weight"), 0) for c in core_src]
    if sum(core_weights) <= 0:
        core_weights = [1.0] * len(core_src)
    core_total = sum(core_weights)
    core_budget = monthly * core_share
    core = []
    for c, w in zip(core_src, core_weights):
        amt = round(core_budget * w / core_total, 2)
        if amt <= 0:
            continue
        core.append({
            "asset": c.get("asset"), "type": c.get("type", "etf"),
            "examples": c.get("examples", []),
            "monthly_amount": amt,
            "pct_of_plan": round(amt / monthly * 100, 1) if monthly else 0,
            "expected_annual_return": c.get("expected_annual_return", ""),
            "risk": c.get("risk", "medium"),
            "rationale": c.get("rationale", ""),
        })

    # ── Satélites: convicción, prioriza largo/medio plazo ──
    sats = [i for i in analysis["investments"]
            if i.get("signal") in ("buy", "watch", "hold")]
    tf_rank = {"long": 0, "medium": 1, "short": 2}
    sats.sort(key=lambda x: (tf_rank.get(x.get("timeframe"), 3), -x.get("priority", 0)))
    sats = sats[:6]
    sat_weights = [_parse_pct(s.get("portfolio_weight"), 0) or s.get("priority", 5) for s in sats]
    sat_total = sum(sat_weights) or 1
    sat_budget = monthly * sat_share
    satellites = []
    for s, w in zip(sats, sat_weights):
        amt = round(sat_budget * w / sat_total, 2)
        if amt <= 0:
            continue
        satellites.append({
            "asset": s.get("asset"), "type": s.get("type"),
            "examples": s.get("examples", []),
            "monthly_amount": amt,
            "pct_of_plan": round(amt / monthly * 100, 1) if monthly else 0,
            "signal": s.get("signal"), "priority": s.get("priority"),
            "timeframe": s.get("timeframe"), "risk": s.get("risk"),
            "expected_annual_return": s.get("expected_annual_return", ""),
            "streak": s.get("streak", 0),
            "rationale": s.get("rationale", ""),
            "target": s.get("target", ""),
        })

    # ── Retorno esperado ponderado de toda la cartera ──
    all_alloc = [(c["monthly_amount"], _parse_return(c.get("expected_annual_return"), 8)) for c in core] + \
                [(s["monthly_amount"], _parse_return(s.get("expected_annual_return"), 12)) for s in satellites]
    wsum = sum(a for a, _ in all_alloc) or 1
    weighted_return = sum(a * r for a, r in all_alloc) / wsum

    # ── Riesgo real basado en datos de mercado (volatilidad histórica) ──
    risk = compute_portfolio_risk(core + satellites)
    # Blend GPT-expected return with market-historical return when available (más certero)
    if risk["hist_return_pct"] is not None and risk["coverage"] >= 0.4:
        base_r = round(0.6 * weighted_return + 0.4 * risk["hist_return_pct"], 1)
    else:
        base_r = round(weighted_return, 1)

    # ── Bandas de proyección derivadas de la volatilidad real, no fijas ──
    # Dispersión anualizada del retorno medio se reduce con el horizonte: vol/sqrt(años).
    vol = risk["portfolio_vol_pct"] or 20.0
    band = vol / (years ** 0.5)               # 1σ del retorno anualizado a 'years' años
    pess_r = max(0.0, base_r - band)
    opt_r = base_r + band
    scenarios = {
        "pesimista":  _project_dca(current, monthly, years, pess_r),
        "base":       _project_dca(current, monthly, years, base_r),
        "optimista":  _project_dca(current, monthly, years, opt_r),
    }
    for k, prob in (("pesimista", "~16%"), ("base", "~50%"), ("optimista", "~16%")):
        scenarios[k]["probability"] = prob

    return {
        "risk": risk,
        "monthly": round(monthly, 2),
        "years": years,
        "current_savings": round(current, 2),
        "profile": profile if profile in RISK_PROFILES else "balanceado",
        "split": {"core_pct": round(core_share * 100), "satellite_pct": round(sat_share * 100)},
        "currency": currency,
        "weighted_expected_return": base_r,
        "core": core,
        "satellites": satellites,
        "projection": scenarios,
        "dca_guidance": analysis.get("dca_guidance", ""),
        "secular_trends": analysis.get("secular_trends", []),
        "macro_regime": analysis.get("macro_regime", ""),
        "generated_at": analysis.get("generated_at", ""),
        "disclaimer": analysis.get("disclaimer", ""),
        "note": "Usando núcleo del análisis IA." if analysis.get("long_term_core") else "Usando núcleo diversificado por defecto.",
    }


@app.get("/api/scheduled-analysis")
async def scheduled_analysis():
    """Vercel cron: full analysis every 3 days + digest email."""
    articles = await gather_news()
    if not articles:
        return JSONResponse({"error": "No articles"}, status_code=503)
    G = build_entity_graph(articles)
    _state["graph"] = G
    try:
        analysis = analyze_with_gpt(articles, _graph_context_for_gpt(G))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    analysis["articles_fetched"] = len(articles)
    analysis["sources_used"] = sorted(set(a["source"] for a in articles))
    analysis["generated_at"] = datetime.now().isoformat()
    analysis["investments"] = validate_buy_signals(
        analysis.get("investments", []), build_corroboration_map(articles))
    _state["analysis"] = analysis
    _state["articles"] = articles
    _state["fetched_at"] = datetime.now().isoformat()
    sent_digest = send_scheduled_digest(analysis, articles, trigger="scheduled")
    sent_alerts = send_buy_alert(analysis.get("investments", []), analysis["generated_at"])
    return {"scheduled": True, "digest_sent": sent_digest, "buy_alerts": sent_alerts}


@app.get("/api/check-trigger")
async def check_trigger():
    """Vercel cron every 6h: lightweight Tier-1 check → full analysis on breaking news."""
    # Fetch only Tier-1 feeds for speed (completes < 10s)
    headers = {"User-Agent": "Mozilla/5.0"}
    async with httpx.AsyncClient(headers=headers) as http:
        tasks = [fetch_feed(n, u, http) for n, u in list(FEEDS_TIER1.items())[:20]]
        results = await asyncio.gather(*tasks)
    quick_articles = [art for batch in results for art in batch]

    breaking = detect_breaking_news(quick_articles)
    if not breaking:
        return {"triggered": False, "checked": len(quick_articles)}

    # Breaking news detected → full analysis
    articles = await gather_news()
    G = build_entity_graph(articles)
    _state["graph"] = G
    try:
        analysis = analyze_with_gpt(articles, _graph_context_for_gpt(G))
    except Exception as e:
        return JSONResponse({"error": str(e), "breaking": breaking}, status_code=500)
    analysis["articles_fetched"] = len(articles)
    analysis["sources_used"] = sorted(set(a["source"] for a in articles))
    analysis["generated_at"] = datetime.now().isoformat()
    _state["analysis"] = analysis
    _state["articles"] = articles
    _state["fetched_at"] = datetime.now().isoformat()
    sent = send_scheduled_digest(analysis, articles, trigger="breaking")
    return {"triggered": True, "breaking": breaking, "digest_sent": sent}


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("index.html", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
