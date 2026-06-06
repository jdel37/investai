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

load_dotenv()

GMAIL_USER = os.getenv("GMAIL_USER", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT", GMAIL_USER)
ALERT_MIN_PRIORITY = int(os.getenv("ALERT_MIN_PRIORITY", "8"))

app = FastAPI(title="InvestAI")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

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
}

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
    """Count distinct sources per keyword entity. Returns top-30 consensus topics."""
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

    result = {
        e: len(s)
        for e, s in entity_sources.items()
        if len(s) >= 4
    }
    top = sorted(result.items(), key=lambda x: -x[1])[:30]
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
            if not G.has_node(e):
                G.add_node(e, mentions=0, sources=set(), type=_detect_entity_type(e))
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
    """Summarize top graph nodes for GPT prompt enrichment."""
    if not G or G.number_of_nodes() == 0:
        return ""
    scored = sorted(
        G.nodes(data=True),
        key=lambda x: -len(x[1].get("sources", set()))
    )[:12]
    lines = ["Top entidades detectadas en el grafo de co-menciones:"]
    for node, data in scored:
        src_n = len(data.get("sources", set()))
        deg = G.degree(node)
        neighbors = sorted(G[node].items(), key=lambda x: -x[1].get("weight", 0))[:3]
        nbr_str = ", ".join(n for n, _ in neighbors) or "—"
        lines.append(f"  • {node} [{data.get('type','?')}] — {src_n} fuentes, {deg} conexiones → relacionado con: {nbr_str}")
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
    rows = ""
    for inv in sorted(buys, key=lambda x: -x.get("priority", 0)):
        tickers = ", ".join(inv.get("examples", [])) or inv["asset"]
        rows += f"""
        <tr>
          <td style="padding:12px;border-bottom:1px solid #1f2d45;font-weight:700;color:#e2e8f0">{inv['asset']}</td>
          <td style="padding:12px;border-bottom:1px solid #1f2d45;color:#10b981;font-weight:700">▲ COMPRAR</td>
          <td style="padding:12px;border-bottom:1px solid #1f2d45;color:#22d3ee">{inv.get('priority',0)}/10</td>
          <td style="padding:12px;border-bottom:1px solid #1f2d45;color:#93c5fd">{tickers}</td>
          <td style="padding:12px;border-bottom:1px solid #1f2d45;color:#94a3b8;font-size:12px">{inv.get('rationale','')[:200]}</td>
        </tr>"""

    html = f"""
    <div style="font-family:-apple-system,sans-serif;background:#0a0e1a;color:#e2e8f0;padding:32px;max-width:800px">
      <h1 style="color:#3b82f6;margin-bottom:4px">📡 InvestAI — Señales de Compra</h1>
      <p style="color:#64748b;margin-bottom:24px">{generated_at}</p>
      <p style="margin-bottom:16px">{len(buys)} señal(es) BUY con prioridad ≥ {ALERT_MIN_PRIORITY}:</p>
      <table style="width:100%;border-collapse:collapse;background:#111827;border-radius:8px;overflow:hidden">
        <thead>
          <tr style="background:#1c2536">
            <th style="padding:12px;text-align:left;color:#64748b;font-size:12px">ACTIVO</th>
            <th style="padding:12px;text-align:left;color:#64748b;font-size:12px">SEÑAL</th>
            <th style="padding:12px;text-align:left;color:#64748b;font-size:12px">PRIORIDAD</th>
            <th style="padding:12px;text-align:left;color:#64748b;font-size:12px">TICKERS</th>
            <th style="padding:12px;text-align:left;color:#64748b;font-size:12px">RAZÓN</th>
          </tr>
        </thead>
        <tbody>{rows}</tbody>
      </table>
      <p style="color:#64748b;font-size:11px;margin-top:24px">
        Este análisis es solo informativo. No constituye asesoramiento financiero.
      </p>
    </div>"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📡 InvestAI — {len(buys)} señal(es) BUY detectada(s)"
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


# ─── ANALYSIS ────────────────────────────────────────────────────────────────
def analyze_with_gpt(articles: list[dict], graph_context: str = "") -> dict:
    corroboration = build_corroboration_map(articles)
    corr_text = ", ".join(
        f'"{e}" ({n} fuentes)' for e, n in corroboration.items()
    )

    articles_text = "\n\n".join(
        f"[{a['source']}] {a['title']}\n{a['summary']}"
        for a in articles[:350]
        if a["title"]
    )

    graph_section = f"\n\nCONTEXTO DEL GRAFO DE ENTIDADES:\n{graph_context}\n" if graph_context else ""

    system = (
        "Eres el mejor analista financiero del mundo: combinas macroeconomía, "
        "geopolítica, análisis técnico y fundamentales. Hablas español. "
        "Generas solo JSON válido, sin texto adicional."
    )

    user = f"""Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}

TÉRMINOS MÁS CORROBORADOS POR MÚLTIPLES FUENTES INDEPENDIENTES:
{corr_text}
{graph_section}
NOTICIAS ({len(articles)} artículos de {len(set(a['source'] for a in articles))} fuentes):
{articles_text[:14000]}

INSTRUCCIONES:
1. Identifica primero qué historias aparecen en MÚLTIPLES fuentes independientes (alta corroboración = más confianza).
2. Descarta o penaliza noticias de fuente única que contradigan el consenso.
3. Si tienes contexto del grafo de entidades, úsalo para identificar qué personajes políticos, científicos y empresas están en el centro de la narrativa y cómo se relacionan.
4. Genera un análisis de inversiones profundo, específico y accionable.

Responde ÚNICAMENTE con este JSON (sin markdown, sin ```):
{{
  "macro_regime": "risk-on|risk-off|stagflation|reflation|deflation|recovery|uncertainty",
  "macro_regime_description": "2 oraciones explicando el régimen macro actual",
  "market_mood": "bullish|bearish|neutral",
  "market_summary": "3-4 oraciones: qué está pasando, por qué, qué implica para inversores",
  "key_themes": ["tema1","tema2","tema3","tema4","tema5"],
  "sector_rotation": {{
    "overweight": ["sector con viento a favor 1","sector 2"],
    "underweight": ["sector bajo presión 1","sector 2"]
  }},
  "investments": [
    {{
      "asset": "nombre del activo o sector",
      "type": "stock|etf|crypto|commodity|bond|real_estate|currency|index",
      "priority": 8,
      "signal": "buy|hold|sell|watch",
      "rationale": "razón específica basada en noticias concretas (3-4 oraciones)",
      "timeframe": "short|medium|long",
      "risk": "low|medium|high",
      "catalysts": ["catalizador específico 1","catalizador 2","catalizador 3"],
      "examples": ["TICKER1","TICKER2","TICKER3"],
      "portfolio_weight": "X% del portafolio",
      "entry_strategy": "estrategia concreta de entrada: zonas, condiciones, triggers",
      "stop_loss": "nivel o % de stop loss sugerido y justificación",
      "target": "objetivo de precio o % de ganancia esperado",
      "corroboration_score": 8,
      "sources_confirming": ["fuente que confirma 1","fuente 2","fuente 3"]
    }}
  ],
  "macro_hedges": ["cobertura recomendada 1","cobertura 2"],
  "risks": ["riesgo concreto 1 con fuente","riesgo 2","riesgo 3","riesgo 4"],
  "watchlist": ["activo a vigilar 1 con razón breve","activo 2"],
  "disclaimer": "Este análisis es solo informativo y no constituye asesoramiento financiero profesional. Invierte con responsabilidad."
}}

Genera entre 7 y 10 inversiones. Prioridad 10 = máxima urgencia. Sé extremadamente específico y accionable."""

    resp = client.chat.completions.create(
        model="gpt-4.1-mini",
        max_tokens=5000,
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
    analysis_ctx = json.dumps(_state["analysis"], ensure_ascii=False)[:8000]
    article_titles = "\n".join(
        f"- [{a['source']}] {a['title']}"
        for a in _state["articles"][:120]
    )

    system_msg = f"""Eres un asesor financiero senior de primer nivel. Tienes acceso al análisis de mercado más reciente generado hace instantes.

ANÁLISIS ACTUAL DEL MERCADO:
{analysis_ctx}

MUESTRA DE ARTÍCULOS ANALIZADOS ({len(_state['articles'])} artículos totales):
{article_titles}

INSTRUCCIONES:
- Responde en español, de forma directa y accionable
- Para preguntas sobre activos específicos: da entrada, stop loss, sizing, catalizadores
- Cita fuentes específicas de las noticias cuando sea relevante
- Si preguntan por algo no cubierto en el análisis, dilo y da tu mejor criterio
- Sé conciso pero completo. Usa formato markdown (negritas, listas) para claridad
- Nunca des excusas ni descargos innecesarios — el usuario quiere información útil"""

    messages = [{"role": "system", "content": system_msg}]
    for h in req.history[-12:]:
        messages.append({"role": h.role, "content": h.content})
    messages.append({"role": "user", "content": req.message})

    def stream_response():
        try:
            stream = client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=messages,
                stream=True,
                max_tokens=1200,
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

    analysis = analyze_with_gpt(articles, _graph_context_for_gpt(G))
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


@app.get("/", response_class=HTMLResponse)
async def root():
    with open("index.html", encoding="utf-8") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
