"""
url_filters.py

Purpose: This module filters sitemap URLs down to “likely article/content pages”.

Design goals:
1) Site-specific rules for known domains (more precise, fewer false positives)
2) A reasonable generic fallback for unknown sites (avoid filtering everything out)

Notes: This is heuristic filtering, not a strict guarantee.
"""
from __future__ import annotations
from urllib.parse import urlsplit, urlparse, urlunparse, urlunsplit


TRACKING_KEYS = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid", "ref", "ref_src", "share")


def _path(url: str) -> str:
    """
    Extract and normalize the URL path.

    Example:
        url = "https://example.com/news/foo?utm_source=x"
        _path(url) -> "/news/foo"
    """
    return (urlsplit(url).path or "").strip()


def canonical_url(url: str) -> str:
    """
    Remove tracking query params / fragments so the same article maps to one ID.
    Minimal canonicalization: drop query+fragment entirely.
    """
    u = (url or "").strip()
    if not u:
        return u
    parts = urlsplit(u)
    # keep scheme/netloc/path only
    return urlunparse((parts.scheme, parts.netloc, parts.path, "", "", ""))


def _strip_tracking(u: str) -> str:
    parts = urlsplit(u)
    if not parts.query:
        return u
    # 只要 query 里出现常见跟踪参数，就直接丢弃整个 query（最省事&稳定）
    q = parts.query.lower()
    if any(k in q for k in TRACKING_KEYS):
        return urlunsplit((parts.scheme, parts.netloc, parts.path, "", parts.fragment))
    return u


def is_article_url(site: str, url: str) -> bool:
    """
    Determine whether a URL *looks like* a content/article page.

    Parameters
    ----------
    site: Logical site name from the source  (e.g. "wikipedia", "nasa", "govuk").
        This is not the hostname; it's your own identifier to select site rules.
    url: The candidate absolute URL from a sitemap.

    Returns
    -------
    bool
        True  -> keep it (likely a content page)
        False -> drop it (likely non-content / noise)

    """
    u = (url or "").strip()
    if not u.startswith("http"):
        return False

    u = _strip_tracking(u)
    parsed = urlparse(u)
    host = (parsed.netloc or "").lower()
    p = _path(u)

    # -------------------------------------------------------------------------
    # A) Global early filtering: obvious non-content URLs
    # if any(x in u for x in ["?share=", "utm_", "fbclid="]):
    #     pass

    # Reject common static / binary file extensions. These are almost never "articles".
    if any(p.endswith(ext) for ext in [
        ".jpg", ".jpeg", ".png", ".gif", ".svg",
        ".pdf", ".zip", ".mp4", ".mp3", ".webm",
        ".css", ".js", ".ico", ".woff", ".woff2",
    ]):
        return False

    site = site.lower()

    # B) Site-specific rules
    # -------------------------------------------------------------------------

    # 1) Wikipedia: keep only encyclopedia entry pages.
    #    - Must start with /wiki/
    #    - Exclude Main_Page and any namespace pages like Special:, File:, Category:, etc.
    if site == "wikipedia":
        # 只要 /wiki/XXX
        if not p.startswith("/wiki/"):
            return False
        title = p[len("/wiki/"):]
        if not title or title in ("Main_Page",):
            return False
        # Namespace pages contain ":" (e.g. Special:Random, File:..., Category:...)
        if ":" in title:
            return False
        return True

    # 2) BBC: focus on /news pages; exclude live streams and media players.
    if site == "bbc":
        if not p.startswith("/news"):
            return False
        if "/live" in p or "/av" in p:
            return False
        return True

    # 3) NASA: allow common content sections (avoid allowing every nasa.gov path)
    if site == "nasa":
        if not host.endswith("nasa.gov"):
            return False

        # allow common content sections for nasa.gov + science.nasa.gov
        if any(p.startswith(x) for x in [
            "/news/",
            "/press-release/",
            "/feature/",
            "/history/",
            "/missions/",
            # --- add: science.nasa.gov common content sections ---
            "/earth/",
            "/solar-system/",
            "/universe/",
            "/science-research/",
            "/humans-in-space/",
            "/technology/",
            "/climate/",
        ]):
            return True

        # otherwise be conservative
        return False

    # 4) GOV.UK: allow major content categories; reject common utility pages.
    if site == "govuk":
        # ✅ allow the common content buckets
        if any(p.startswith(x) for x in [
            "/guidance/",
            "/government/publications/",
            "/government/news/",
            "/government/speeches/",
            "/government/statistics/",
            "/government/world-location-news/",
            "/government/country/"
        ]):
            return True

        # Consultations: allow deeper leaf pages (responses/outcomes/government-response etc.)
        if p.startswith("/government/consultations/"):
            # if it's the consultation root page, keep
            # if it's deeper child pages, also keep (most are real content)
            return True

        if any(p.startswith(x) for x in ["/search", "/help", "/sign-in", "/contact"]):
            return False

        return False

    # 5) Python docs: accept most docs pages, reject root and static folders.
    if site == "python_docs":
        if host.endswith("docs.python.org"):
            # no empty paths
            if p in ("", "/"):
                return False
            # no static folders or obvious index pages
            if p.startswith("/assets") or p.startswith("/_static"):
                return False
            return True
        return False

    # 6) HowStuffWorks（howstuffworks.com）
    if site == "howstuffworks":
        # if "howstuffworks.com" not in host:
        #     return False
        if host != "www.howstuffworks.com":
            return False
        # no obvious non-content paths (video files, cdn, etc.)
        if "/videos/" in p or "/video/" in p:
            return False
        return True
    
    # 7) National Geographic（nationalgeographic.com）
    if site == "natgeo":
        if "nationalgeographic.com" not in host:
            return False
        # no obvious non-content paths (video files, cdn, etc.)
        if any(seg in p for seg in ["/photos/", "/video/", "/videos/"]):
            return False
        return True
    
    # 8) Smithsonian（smithsonianmag.com）
    if site == "smithsonian":
        if "smithsonianmag.com" not in host:
            return False
        return True

    # 9) The Guardian
    if site == "guardian":
        if "theguardian.com" not in host:
            return False
        # drop obvious games/puzzles pages (lots of noise)
        if any(seg in p for seg in ["/crosswords/", "/sudoku", "/killer-sudoku"]):
            return False
        # keep most content (guardian URL patterns vary a lot)
        return p not in ("", "/")

    # 10) MIT News
    if site == "mit_news":
        if "news.mit.edu" not in host:
            return False
        # homepage is not an article
        if p in ("", "/"):
            return False
        # allow almost everything else; later short_body will filter junk
        if any(p.endswith(ext) for ext in [".xml", ".jpg", ".png", ".gif", ".pdf"]):
            return False
        return True

    # 11) TechCrunch
    if site == "techcrunch":
        if "techcrunch.com" not in host:
            return False
        if p in ("", "/"):
            return False
        # article URL pattern: /YYYY/MM/DD/...
        if len(p) >= 12 and p[1:5].isdigit() and p[5] == "/" and p[6:8].isdigit() and p[8] == "/" and p[9:11].isdigit() and p[11] == "/":
            return True

        # drop obvious non-article landing pages
        if any(seg in p for seg in [
            # "/newsletters/",
            "/events/",
            "/about/",
            "/advertising/",
            "/contact/",
        ]):
            return False
        
        if "/newsletters/" in p:
            # allow those like /newsletters/xxx/yyy to pass
            parts = [s for s in p.split("/") if s]
            if len(parts) < 3:
                return False

        # keep the rest (techcrunch has category pages that might still be ok)
        return True

    # 12) World Bank
    if site == "world_bank":
        if "worldbank.org" not in host:
            return False
        if p in ("", "/"):
            return False

        # Allow likely content paths (tight but practical)
        if any(p.startswith(x) for x in [
            "/en/news/",
            "/en/news/press-release/",
            "/en/news/feature/",
            "/en/blogs/",
            "/en/publication/",
            "/en/research/",
            "/en/topic/",
            "/en/results/",
            "/en/country/"
        ]):
            return True

        # drop known thin / api-ish pages
        if any(seg in p for seg in [
            "/api/",
            "/data/",
            "/indicator/",
            "/businessready/multimedia/utility-services/",
        ]):
            return False

        # otherwise conservative
        return False

    # 13) Django docs
    if site == "django_docs":
        if "docs.djangoproject.com" not in host:
            return False
        if p in ("", "/"):
            return False
        if p.startswith("/en/") and ("/_static/" in p or "/_images/" in p):
            return False
        return True


    # -------------------------------------------------------------------------
    # C) Generic fallback rules (for unknown sites)
    # -------------------------------------------------------------------------
    low_p = p.lower()

    # C.1) Reject very common non-content routes.
    if any(bad in low_p for bad in [
        "/login", "/signin", "/sign-in", "/signup", "/register",
        "/account", "/settings", "/profile",
        "/cart", "/checkout",
        "/search", "/results",
        "/static/", "/assets/", "/media/",
        "/api/", "/graphql", "/admin/",
    ]):
        return False

    # C.2) Path must not be empty (root homepage is usually not a "document")
    segments = [s for s in low_p.split("/") if s]  # cut empty segments
    if not segments:
        return False

    # Single-segment paths like "/about" can still be useful content pages,
    # but reject extremely common non-content slugs
    if len(segments) == 1:
        seg = segments[0]
        # 排除一些常见的非内容 slug
        if seg in ("", "home", "index", "search", "login", "signup", "admin"):
            return False
        return True

    # Multi-segment paths usually indicate structured content:
    # e.g. /news/title, /blog/2024/01/post, /articles/something
    # Simple heuristic: if the path is not too short, keep it.
    if len(low_p) >= 8:
        return True

    # Too short Multi-segment paths == noises
    return False
