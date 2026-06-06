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
}
TIER2_SOURCES = {
    "CNBC Markets","CNBC Economy","CNBC Tech","CNBC Top News",
    "MarketWatch Top","MarketWatch Markets","Foreign Policy","CFR",
    "Brookings","SCMP Business","Deutsche Welle Biz","Barrons",
    "Federal Reserve","ECB Speeches","IMF Blog","US Treasury","BIS Speeches",
    "Peterson IIE","SEC Press","POLITICO Economy","Axios Markets",
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
_state: dict = {"analysis": {}, "articles": [], "fetched_at": None, "graph": None}

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

    return articles


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
            if any(p.lower() in e.lower() for p in HIGH_IMPACT_PERSONS):
                entity_high_impact[e] = True

    # Score: base count + tier1 bonus + high-impact bonus
    scored = {}
    for e, srcs in entity_sources.items():
        base = len(srcs)
        if base < 3:
            continue
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

    market = fetch_market_snapshot()
    market_section = f"\nMERCADO AHORA:{market}\n" if market else ""
    graph_section = f"\nGRAFO:{graph_context}\n" if graph_context else ""

    system = "Analista financiero experto. Solo JSON válido. Español."

    user = f"""Fecha:{datetime.now().strftime('%Y-%m-%d %H:%M')} Fuentes:{len(set(a['source'] for a in articles))} Artículos:{len(articles)}
CORROBORACIÓN(entidad:score):{corr_text}
{market_section}{graph_section}
NOTICIAS:
{articles_text[:6000]}

Prioriza historias en múltiples fuentes. Usa el grafo para identificar políticos/científicos/empresas clave.
JSON (sin markdown):
{{"macro_regime":"risk-on|risk-off|stagflation|reflation|deflation|recovery|uncertainty","macro_regime_description":"str","market_mood":"bullish|bearish|neutral","market_summary":"str","key_themes":["t1","t2","t3","t4","t5"],"sector_rotation":{{"overweight":["s1","s2"],"underweight":["s1","s2"]}},"investments":[{{"asset":"str","type":"stock|etf|crypto|commodity|bond|real_estate|currency|index","priority":8,"signal":"buy|hold|sell|watch","rationale":"str","timeframe":"short|medium|long","risk":"low|medium|high","catalysts":["c1","c2"],"examples":["T1","T2"],"portfolio_weight":"X%","entry_strategy":"str","stop_loss":"str","target":"str","corroboration_score":8,"sources_confirming":["f1","f2"]}}],"macro_hedges":["h1","h2"],"risks":["r1","r2","r3"],"watchlist":["w1","w2"],"disclaimer":"str"}}
5-7 inversiones. Prioridad 10=máxima urgencia.
CRÍTICO: en "asset" pon el nombre ESPECÍFICO del activo (ej: "NVIDIA", "Bitcoin", "Apple", "Tesla", "Gold Futures") — NUNCA sectores genéricos ("Tecnología", "Energía", "Mercados emergentes"). En "examples" pon tickers reales (ej: ["NVDA","AMD","TSMC"] o ["BTC","ETH"])."""

    resp = get_openai_client().chat.completions.create(
        model="gpt-4.1-mini",
        max_tokens=2500,
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


# ─── ENDPOINTS ───────────────────────────────────────────────────────────────
@app.get("/api/analyze")
async def analyze():
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

    _state["analysis"] = analysis
    _state["articles"] = articles
    _state["fetched_at"] = datetime.now().isoformat()

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
