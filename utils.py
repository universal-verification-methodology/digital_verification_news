import datetime
import json
import logging
import os
import re
import shutil
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytz
import urllib
import urllib.error
import urllib.request
import urllib3
import requests
from bs4 import BeautifulSoup

import feedparser
from easydict import EasyDict
from pypdf import PdfReader

try:
    from pdf2image import convert_from_path
    from pdf2image.exceptions import PDFInfoNotInstalledError
    import pytesseract
except ImportError:  # pragma: no cover - optional heavy OCR dependencies
    convert_from_path = None  # type: ignore[assignment]
    PDFInfoNotInstalledError = None  # type: ignore[assignment]
    pytesseract = None  # type: ignore[assignment]

# Set up logger
logger = logging.getLogger(__name__)


def remove_duplicated_spaces(text: str) -> str:
    """Collapse duplicate whitespace characters into single spaces.

    Args:
        text: Input text that may contain duplicated whitespace.

    Returns:
        A string with all consecutive whitespace collapsed into a single space.
    """
    return " ".join(text.split())


def request_paper_with_arxiv_api(
    keyword: str,
    max_results: int,
    link: str = "OR",
) -> List[Dict[str, str]]:
    """Request papers from the arXiv API for a given keyword.

    Args:
        keyword: Search keyword that will be used in both title and abstract.
        max_results: Maximum number of results to retrieve from arXiv.
        link: Logical operator between title and abstract conditions, either
            ``"OR"`` or ``"AND"``.

    Returns:
        A list of dictionaries describing papers, each containing the default
        columns: ``Title``, ``Authors``, ``Abstract``, ``Link``, ``Tags``,
        ``Comment`` and ``Date``.

    Raises:
        AssertionError: If ``link`` is not ``"OR"`` or ``"AND"``.
        Exception: If there is any error when calling the arXiv API.
    """
    assert link in ["OR", "AND"], "link should be 'OR' or 'AND'"
    keyword = "\"" + keyword + "\""
    url = (
        "http://export.arxiv.org/api/query?"
        "search_query=ti:{0}+{2}+abs:{0}&max_results={1}&sortBy=lastUpdatedDate"
    ).format(keyword, max_results, link)
    url = urllib.parse.quote(url, safe="%/:=&?~#+!$,;'@()*[]")

    logger.info("Requesting papers from arXiv API for keyword: %s", keyword)
    try:
        response = urllib.request.urlopen(url).read().decode("utf-8")
        feed = feedparser.parse(response)
        logger.info("Successfully retrieved %d papers from arXiv API", len(feed.entries))
    except Exception as exc:
        logger.error("Failed to fetch papers from arXiv API: %s", exc)
        raise

    # NOTE default columns: Title, Authors, Abstract, Link, Tags, Comment, Date
    papers: List[Dict[str, str]] = []
    for entry in feed.entries:
        try:
            entry_ez = EasyDict(entry)
            paper: Dict[str, str] = {}

            # title
            paper["Title"] = remove_duplicated_spaces(
                entry_ez.title.replace("\n", " "),
            )
            # abstract
            paper["Abstract"] = remove_duplicated_spaces(
                entry_ez.summary.replace("\n", " "),
            )
            # authors
            paper["Authors"] = [
                remove_duplicated_spaces(author["name"].replace("\n", " "))
                for author in entry_ez.authors
            ]
            # link
            paper["Link"] = remove_duplicated_spaces(
                entry_ez.link.replace("\n", " "),
            )
            # tags
            paper["Tags"] = [
                remove_duplicated_spaces(tag["term"].replace("\n", " "))
                for tag in entry_ez.tags
            ]
            # comment
            paper["Comment"] = remove_duplicated_spaces(
                entry_ez.get("arxiv_comment", "").replace("\n", " "),
            )
            # date
            paper["Date"] = entry_ez.updated

            papers.append(paper)
        except Exception as exc:
            logger.warning("Failed to process paper entry: %s", exc)
            continue

    logger.info("Successfully processed %d papers", len(papers))
    return papers


def request_papers_with_crossref(
    keyword: str,
    max_results: int,
) -> List[Dict[str, str]]:
    """Request papers using the CrossRef API (metadata only).

    Args:
        keyword: Search keyword to query in CrossRef.
        max_results: Maximum number of results to retrieve.

    Returns:
        A list of paper dictionaries normalised to the common schema.
    """
    params = {
        "query": keyword,
        "rows": max_results,
        "sort": "published",
        "order": "desc",
    }
    url = "https://api.crossref.org/works?" + urllib.parse.urlencode(params)

    logger.info("Requesting papers from CrossRef for keyword: %s", keyword)
    try:
        request = urllib.request.Request(
            url,
            headers={
                # CrossRef requires a descriptive User-Agent including contact info.
                "User-Agent": (
                    "daily-papers-bot/0.1 "
                    "(mailto:YOUR_EMAIL@example.com)"
                ),
            },
        )
        with urllib.request.urlopen(request) as response:
            raw = response.read().decode("utf-8")
        data = json.loads(raw)
        items = data.get("message", {}).get("items", [])
        logger.info("Successfully retrieved %d papers from CrossRef", len(items))
    except Exception as exc:
        logger.error("Failed to fetch papers from CrossRef: %s", exc)
        raise

    papers: List[Dict[str, str]] = []
    for item in items:
        try:
            title_list = item.get("title") or []
            title = title_list[0] if title_list else "Untitled"

            abstract_raw = item.get("abstract", "") or ""
            abstract_text = re.sub(r"<.*?>", "", abstract_raw)

            authors_raw = item.get("author") or []
            authors = []
            for author in authors_raw:
                given = author.get("given", "")
                family = author.get("family", "")
                name = (given + " " + family).strip()
                if name:
                    authors.append(name)

            url_item = item.get("URL", "")

            date_parts = (
                item.get("issued", {}).get("date-parts")
                or item.get("published-print", {}).get("date-parts")
                or item.get("published-online", {}).get("date-parts")
                or []
            )
            if date_parts and date_parts[0]:
                year = date_parts[0][0]
                month = date_parts[0][1] if len(date_parts[0]) > 1 else 1
                day = date_parts[0][2] if len(date_parts[0]) > 2 else 1
                date_str = f"{year:04d}-{month:02d}-{day:02d}T00:00:00Z"
            else:
                date_str = "1970-01-01T00:00:00Z"

            container_list = item.get("container-title") or []
            container = container_list[0] if container_list else ""

            paper: Dict[str, str] = {
                "Title": remove_duplicated_spaces(title.replace("\n", " ")),
                "Abstract": remove_duplicated_spaces(abstract_text.replace("\n", " ")),
                "Authors": authors or ["Unknown"],
                "Link": url_item,
                "Tags": ["CrossRef"],
                "Comment": container,
                "Date": date_str,
            }
            papers.append(paper)
        except Exception as exc:
            logger.warning("Failed to process CrossRef paper entry: %s", exc)
            continue

    logger.info("Successfully processed %d CrossRef papers", len(papers))
    return papers


def request_papers_with_openalex(
    keyword: str,
    max_results: int,
) -> List[Dict[str, str]]:
    """Request papers using the OpenAlex API (metadata only).

    Args:
        keyword: Search keyword to query in OpenAlex.
        max_results: Maximum number of results to retrieve.

    Returns:
        A list of paper dictionaries normalised to the common schema.
    """
    params = {
        "search": keyword,
        "per-page": max_results,
        "sort": "publication_date:desc",
    }
    url = "https://api.openalex.org/works?" + urllib.parse.urlencode(params)

    logger.info("Requesting papers from OpenAlex for keyword: %s", keyword)
    try:
        with urllib.request.urlopen(url) as response:
            raw = response.read().decode("utf-8")
        data = json.loads(raw)
        results = data.get("results", [])
        logger.info("Successfully retrieved %d papers from OpenAlex", len(results))
    except Exception as exc:
        logger.error("Failed to fetch papers from OpenAlex: %s", exc)
        raise

    papers: List[Dict[str, str]] = []
    for item in results:
        try:
            title = item.get("title") or "Untitled"
            abstract_inverted = item.get("abstract_inverted_index") or {}
            # Flatten inverted index to a text snippet if available.
            if abstract_inverted:
                # abstract_inverted is {word: [positions...]}; reconstruct a rough abstract.
                positions: Dict[int, str] = {}
                for word, idxs in abstract_inverted.items():
                    for idx in idxs:
                        positions[idx] = word
                abstract_words = [positions[i] for i in sorted(positions.keys())]
                abstract_text = " ".join(abstract_words)
            else:
                abstract_text = ""

            authorships = item.get("authorships") or []
            authors = []
            for auth in authorships:
                author_info = auth.get("author", {})
                name = author_info.get("display_name", "")
                if name:
                    authors.append(name)

            url_item = item.get("primary_location", {}).get("landing_page_url") or item.get(
                "id",
                "",
            )

            date_str = item.get("publication_date") or "1970-01-01"
            if "T" not in date_str:
                date_str = f"{date_str}T00:00:00Z"

            venue = ""
            if item.get("host_venue"):
                venue = item["host_venue"].get("display_name", "") or ""

            paper = {
                "Title": remove_duplicated_spaces(title.replace("\n", " ")),
                "Abstract": remove_duplicated_spaces(abstract_text.replace("\n", " ")),
                "Authors": authors or ["Unknown"],
                "Link": url_item,
                "Tags": ["OpenAlex"],
                "Comment": venue,
                "Date": date_str,
            }
            papers.append(paper)
        except Exception as exc:
            logger.warning("Failed to process OpenAlex paper entry: %s", exc)
            continue

    logger.info("Successfully processed %d OpenAlex papers", len(papers))
    return papers


def request_papers_with_semantic_scholar(
    keyword: str,
    max_results: int,
) -> List[Dict[str, str]]:
    """Request papers using the Semantic Scholar API (metadata only).

    Args:
        keyword: Search keyword to query in Semantic Scholar.
        max_results: Maximum number of results to retrieve.

    Returns:
        A list of paper dictionaries normalised to the common schema.
    """
    params = {
        "query": keyword,
        "limit": max_results,
        "offset": 0,
        "fields": "title,abstract,authors,venue,year,url",
    }
    url = (
        "https://api.semanticscholar.org/graph/v1/paper/search?"
        + urllib.parse.urlencode(params)
    )

    logger.info("Requesting papers from Semantic Scholar for keyword: %s", keyword)
    try:
        with urllib.request.urlopen(url) as response:
            raw = response.read().decode("utf-8")
        data = json.loads(raw)
        items = data.get("data", [])
        logger.info(
            "Successfully retrieved %d papers from Semantic Scholar",
            len(items),
        )
    except Exception as exc:
        logger.error("Failed to fetch papers from Semantic Scholar: %s", exc)
        raise

    papers: List[Dict[str, str]] = []
    for item in items:
        try:
            title = item.get("title") or "Untitled"
            abstract_text = item.get("abstract") or ""

            authors_raw = item.get("authors") or []
            authors = []
            for author in authors_raw:
                name = author.get("name", "")
                if name:
                    authors.append(name)

            url_item = item.get("url", "")

            year = item.get("year")
            if year:
                date_str = f"{int(year):04d}-01-01T00:00:00Z"
            else:
                date_str = "1970-01-01T00:00:00Z"

            venue = item.get("venue", "") or ""

            paper = {
                "Title": remove_duplicated_spaces(title.replace("\n", " ")),
                "Abstract": remove_duplicated_spaces(abstract_text.replace("\n", " ")),
                "Authors": authors or ["Unknown"],
                "Link": url_item,
                "Tags": ["SemanticScholar"],
                "Comment": venue,
                "Date": date_str,
            }
            papers.append(paper)
        except Exception as exc:
            logger.warning("Failed to process Semantic Scholar paper entry: %s", exc)
            continue

    logger.info("Successfully processed %d Semantic Scholar papers", len(papers))
    return papers


def _is_verification_flavoured_query(keyword: str) -> bool:
    """Return True if the keyword looks like a DV/verification-style query.

    This is used to decide when additional post-filtering should be applied to
    generic aggregators such as CrossRef and OpenAlex to keep the feed focused
    on digital / hardware verification topics.
    """
    lowered = keyword.lower()
    return any(token in lowered for token in ["verification", "uvm", "uvm-", "dvcon"])


def _is_digital_verification_paper(paper: Dict[str, str]) -> bool:
    """Heuristically decide whether a paper is about digital / hardware verification.

    The check is intentionally conservative: it looks for common hardware and
    verification-related terms across the title, abstract and venue/comment
    fields, and treats anything failing this test as out of scope for DV-CON.
    """
    haystack = " ".join(
        [
            paper.get("Title", ""),
            paper.get("Abstract", ""),
            paper.get("Comment", ""),
        ],
    ).lower()
    if not haystack.strip():
        return False

    verification_markers = [
        "verification",
        "uvm",
        "systemverilog",
        "rtl",
        "testbench",
        "formal",
        "assertion",
        "coverage",
        "dvcon",
        "soc",
        "fpga",
        "asic",
        "hdl",
    ]
    return any(marker in haystack for marker in verification_markers)


def request_papers_with_acm_api(
    keyword: str,
    max_results: int,
) -> List[Dict[str, str]]:
    """Request papers from the ACM Digital Library API using a keyword.

    This function assumes you have configured an ACM API access token in the
    ``ACM_ACCESS_TOKEN`` environment variable. The exact endpoint and query
    parameters may need to be adjusted to match your ACM subscription or API
    documentation. The default implementation targets the generic metadata
    endpoint suggested by dltHub's ACM Digital Library connector
    (`acm_digital_library_migrations`) [1]_.

    Args:
        keyword: Free-text keyword query to search ACM metadata.
        max_results: Maximum number of records to return.

    Returns:
        A list of paper dictionaries normalised to the common schema.

    Raises:
        RuntimeError: If the ACM access token is not configured.

    References:
        .. [1] dltHub ACM Digital Library connector documentation.
    """
    access_token = os.getenv("ACM_ACCESS_TOKEN")
    if not access_token:
        raise RuntimeError(
            "ACM_ACCESS_TOKEN environment variable is not set. "
            "Please configure your ACM API access token before using the ACM "
            "metadata integration.",
        )

    base_url = os.getenv("ACM_BASE_URL", "https://dl.acm.org/v/")
    # The exact path and parameters depend on your ACM API contract. Here we
    # follow the pattern from dltHub's example, hitting a generic metadata
    # endpoint and passing a simple query string and pagination.
    metadata_url = urllib.parse.urljoin(base_url, "api/metadata")

    headers = {
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
    }

    page = 0
    page_size = min(max_results, 100)
    collected: List[Dict[str, str]] = []

    while len(collected) < max_results:
        params = {
            "q": keyword,
            "page": page,
            "size": page_size,
        }
        logger.info(
            "Requesting ACM metadata page=%d size=%d keyword=%s",
            page,
            page_size,
            keyword,
        )
        try:
            response = requests.get(
                metadata_url,
                params=params,
                headers=headers,
                timeout=30,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.error("Failed to fetch ACM metadata: %s", exc)
            break

        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            logger.error("Failed to decode ACM metadata JSON: %s", exc)
            break

        # The exact structure depends on the ACM API. We assume a top-level
        # "items" list for now; adjust if your API uses a different field.
        items = payload.get("items") or payload.get("data") or []
        if not items:
            logger.info("ACM metadata query returned no more items.")
            break

        for item in items:
            collected.append(item)
            if len(collected) >= max_results:
                break

        page += 1

    papers: List[Dict[str, str]] = []
    for item in collected:
        try:
            title = (
                item.get("title")
                or item.get("articleTitle")
                or item.get("fullTitle")
                or "Untitled"
            )
            abstract_text = item.get("abstract") or ""

            authors_raw = item.get("authors") or item.get("creators") or []
            authors: List[str] = []
            for author in authors_raw:
                name = (
                    author.get("name")
                    or author.get("preferredName")
                    or author.get("fullName")
                )
                if not name:
                    first = author.get("firstName", "")
                    last = author.get("lastName", "")
                    name = (first + " " + last).strip()
                if name:
                    authors.append(name)

            doi = item.get("doi")
            url_item = item.get("url") or ""
            if not url_item and doi:
                url_item = f"https://doi.org/{doi}"

            pub_date = (
                item.get("publicationDate")
                or item.get("date")
                or item.get("published")
            )
            year = item.get("year")

            if pub_date and isinstance(pub_date, str):
                if "T" in pub_date:
                    date_str = pub_date
                elif re.match(r"\d{4}-\d{2}-\d{2}", pub_date):
                    date_str = f"{pub_date}T00:00:00Z"
                else:
                    if year:
                        date_str = f"{int(year):04d}-01-01T00:00:00Z"
                    else:
                        date_str = "1970-01-01T00:00:00Z"
            elif year:
                date_str = f"{int(year):04d}-01-01T00:00:00Z"
            else:
                date_str = "1970-01-01T00:00:00Z"

            venue = (
                item.get("publicationTitle")
                or item.get("journal")
                or item.get("conference")
                or ""
            )

            paper: Dict[str, str] = {
                "Title": remove_duplicated_spaces(title.replace("\n", " ")),
                "Abstract": remove_duplicated_spaces(abstract_text.replace("\n", " ")),
                "Authors": authors or ["Unknown"],
                "Link": url_item,
                "Tags": ["ACM"],
                "Comment": venue,
                "Date": date_str,
            }
            papers.append(paper)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to process ACM paper entry: %s", exc)
            continue

    logger.info("Successfully processed %d ACM papers", len(papers))
    return papers


def _ieee_search_page(
    query_text: str,
    page: int,
    rows_per_page: int = 100,
    get_page_number: bool = False,
    retry: int = 5,
) -> Dict[str, str] | List[Dict[str, str]] | None:
    """Call the IEEE Xplore internal search endpoint for a single page.

    This is adapted from the CIRDC conference download script, but refactored
    to support arbitrary keyword queries and to return results instead of
    writing JSON files.

    Args:
        query_text: The IEEE Xplore ``queryText`` expression.
        page: 1-based page index to request.
        rows_per_page: Number of records per page (IEEE typically allows 100).
        get_page_number: If True, return only the total number of pages.
        retry: Maximum number of retries on request/parse failures.

    Returns:
        If ``get_page_number`` is True, returns the integer number of pages.
        Otherwise returns the list of ``records`` for the page, or ``None`` on
        persistent failure.
    """
    logger.info(
        "IEEE search page query=%s page=%d get_page_number=%s",
        query_text,
        page,
        get_page_number,
    )
    if get_page_number:
        assert page == 1

    data = {
        "newsearch": "true",
        "highlight": "true",
        "matchBoolean": "true",
        "matchPubs": "true",
        "action": "search",
        "queryText": query_text,
        "pageNumber": str(page),
        "rowsPerPage": rows_per_page,
    }

    headers = {
        "Accept": "application/json,text/plain,*/*",
        "Accept-Encoding": "gzip,deflate,br",
        "Accept-Language": "en-US,en;q=0.8",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Referer": "https://ieeexplore.ieee.org/search/searchresult.jsp?newsearch=true",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/108.0.0.0 Safari/537.36"
        ),
    }

    url = "https://ieeexplore.ieee.org/rest/search"
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    for attempt in range(retry):
        try:
            response = requests.post(
                url=url,
                data=json.dumps(data),
                headers=headers,
                timeout=30,
                verify=False,
            )
            response.raise_for_status()

            try:
                payload = response.json()
            except json.JSONDecodeError:
                logger.warning(
                    "IEEE JSON decode error on page %d, attempt %d of %d",
                    page,
                    attempt + 1,
                    retry,
                )
                continue

            if get_page_number:
                total_pages = int(payload.get("totalPages", 0))
                logger.info("IEEE keyword search totalPages=%d", total_pages)
                return total_pages

            records = payload.get("records", [])
            if not isinstance(records, list):
                logger.error("IEEE response missing 'records' on page %d", page)
                return None

            logger.info("IEEE page %d returned %d records", page, len(records))
            return records
        except requests.RequestException as exc:
            logger.warning(
                "IEEE request error on page %d, attempt %d of %d: %s",
                page,
                attempt + 1,
                retry,
                exc,
            )

    logger.error("Failed IEEE search page after %d attempts (page=%d)", retry, page)
    return None


def request_papers_with_ieee_keyword(
    keyword: str,
    max_results: int,
) -> List[Dict[str, str]]:
    """Request papers from IEEE Xplore using a keyword-based search.

    This function uses the same internal ``/rest/search`` endpoint and request
    structure as CIRDC, but issues a keyword query instead of a publication
    number filter and returns records directly in memory.

    Args:
        keyword: Free-text keyword query (matched against IEEE metadata).
        max_results: Maximum number of records to return across all pages.

    Returns:
        A list of paper dictionaries normalised to the common schema.
    """
    # Simple all-metadata keyword query; this mirrors the behaviour of the
    # IEEE Xplore search UI. More complex queries can be plugged in later.
    query_text = keyword

    logger.info("Requesting IEEE papers for keyword: %s", keyword)

    all_records: List[Dict[str, str]] = []

    total_pages_obj = _ieee_search_page(
        query_text=query_text,
        page=1,
        get_page_number=True,
    )
    if not isinstance(total_pages_obj, int) or total_pages_obj <= 0:
        logger.warning("IEEE keyword search returned no pages for '%s'", keyword)
        return []

    total_pages = total_pages_obj

    for page in range(1, total_pages + 1):
        if len(all_records) >= max_results:
            break
        records = _ieee_search_page(
            query_text=query_text,
            page=page,
            get_page_number=False,
        )
        if not records:
            continue
        for record in records:
            all_records.append(record)
            if len(all_records) >= max_results:
                break

    papers: List[Dict[str, str]] = []
    for rec in all_records:
        try:
            title = rec.get("articleTitle") or rec.get("title") or "Untitled"
            abstract_text = rec.get("abstract") or ""

            authors_raw = rec.get("authors") or []
            authors: List[str] = []
            for author in authors_raw:
                name = (
                    author.get("preferredName")
                    or author.get("fullName")
                    or author.get("firstName", "") + " " + author.get("lastName", "")
                ).strip()
                if name:
                    authors.append(name)

            article_number = rec.get("articleNumber")
            doi = rec.get("doi")
            if article_number:
                link = f"https://ieeexplore.ieee.org/document/{article_number}"
            elif doi:
                link = f"https://doi.org/{doi}"
            else:
                link = ""

            pub_date = rec.get("publicationDate") or ""
            pub_year = rec.get("publicationYear")

            if pub_date:
                # IEEE dates are often like "2023-05-01" or "01 May 2023".
                if "T" in pub_date:
                    date_str = pub_date
                elif re.match(r"\d{4}-\d{2}-\d{2}", pub_date):
                    date_str = f"{pub_date}T00:00:00Z"
                else:
                    # Fallback: just use year if available.
                    if pub_year:
                        date_str = f"{int(pub_year):04d}-01-01T00:00:00Z"
                    else:
                        date_str = "1970-01-01T00:00:00Z"
            elif pub_year:
                date_str = f"{int(pub_year):04d}-01-01T00:00:00Z"
            else:
                date_str = "1970-01-01T00:00:00Z"

            venue = rec.get("publicationTitle") or ""

            paper: Dict[str, str] = {
                "Title": remove_duplicated_spaces(title.replace("\n", " ")),
                "Abstract": remove_duplicated_spaces(abstract_text.replace("\n", " ")),
                "Authors": authors or ["Unknown"],
                "Link": link,
                "Tags": ["IEEE"],
                "Comment": venue,
                "Date": date_str,
            }
            papers.append(paper)
        except Exception as exc:
            logger.warning("Failed to process IEEE paper entry: %s", exc)
            continue

    logger.info("Successfully processed %d IEEE papers", len(papers))
    return papers

def filter_tags(
    papers: List[Dict[str, str]],
    target_fileds: List[str] = ["cs", "stat"],
) -> List[Dict[str, str]]:
    """Filter papers by arXiv-style subject tags.

    Args:
        papers: List of paper dictionaries, each containing a ``"Tags"`` field.
        target_fileds: List of allowed tag prefixes (e.g. ``["cs", "stat"]``).

    Returns:
        Filtered list of papers whose first tag component matches any of
        ``target_fileds``.
    """
    logger.info("Filtering papers by tags: %s", target_fileds)
    # filtering tags: only keep the papers in target_fileds
    results = []
    for paper in papers:
        tags = paper.get("Tags", [])
        for tag in tags:
            if tag.split(".")[0] in target_fileds:
                results.append(paper)
                break
    logger.info("Filtered papers: %d out of %d papers kept", len(results), len(papers))
    return results


def get_daily_papers_by_keyword_with_retries(
    keyword: str,
    column_names: List[str],
    max_result: int,
    link: str = "OR",
    retries: int = 6,
) -> List[Dict[str, str]]:
    """Retrieve papers with simple retry logic and a short backoff.

    This helper wraps :func:`get_daily_papers_by_keyword` with retry handling so
    that transient arXiv issues (empty responses, network hiccups, etc.) do not
    cause the whole update pipeline to fail immediately.

    Args:
        keyword: Search keyword to query.
        column_names: Column names to keep in the final result.
        max_result: Maximum number of results to retrieve.
        link: Logical operator between title and abstract conditions, either
            ``"OR"`` or ``"AND"``.
        retries: Maximum number of retry attempts on failure.

    Returns:
        A (possibly empty) list of paper dictionaries. If all retry attempts
        fail or keep returning empty results, an empty list is returned.
    """
    logger.info(
        "Attempting to get papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    retry_delay_seconds = 10

    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword(keyword, column_names, max_result, link)
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            else:
                logger.warning(
                    "Received empty list on attempt %d, retrying in %d seconds...",
                    attempt + 1,
                    retry_delay_seconds,
                )
                time.sleep(retry_delay_seconds)
        except Exception as exc:
            logger.error("Error on attempt %d: %s", attempt + 1, exc)
            if attempt < retries - 1:
                logger.info("Waiting %d seconds before retry...", retry_delay_seconds)
                time.sleep(retry_delay_seconds)

    logger.error("Failed to get papers after all retry attempts; returning empty list.")
    return []


def get_daily_papers_by_keyword(
    keyword: str,
    column_names: List[str],
    max_result: int,
    link: str = "OR",
) -> List[Dict[str, str]]:
    logger.info("Getting papers for keyword: %s", keyword)
    # get papers
    papers = request_paper_with_arxiv_api(keyword, max_result, link)
    # NOTE filtering tags: only keep the papers in cs field
    papers = filter_tags(papers)
    # select columns for display
    papers = [{column_name: paper[column_name] for column_name in column_names} for paper in papers]
    logger.info(
        "Retrieved %d papers after filtering and column selection",
        len(papers),
    )
    return papers


def get_daily_papers_by_keyword_from_crossref(
    keyword: str,
    column_names: List[str],
    max_result: int,
) -> List[Dict[str, str]]:
    """Get papers for a keyword using CrossRef.

    Args:
        keyword: Search keyword.
        column_names: Column names to keep in the result.
        max_result: Maximum number of results to retrieve.

    Returns:
        A list of dictionaries ready for table generation.
    """
    logger.info("Getting CrossRef papers for keyword: %s", keyword)
    papers = request_papers_with_crossref(keyword, max_result)

    # For verification-centric queries, aggressively drop non-DV papers from
    # generic aggregators so that DV-CON stays focused on digital verification.
    if _is_verification_flavoured_query(keyword):
        papers = [paper for paper in papers if _is_digital_verification_paper(paper)]

    # Select columns for display, falling back to empty string if missing.
    processed: List[Dict[str, str]] = []
    for paper in papers:
        processed.append(
            {column_name: paper.get(column_name, "") for column_name in column_names},
        )
    logger.info("Retrieved %d CrossRef papers after column selection", len(processed))
    return processed


def get_daily_papers_by_keyword_from_openalex(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Get papers for a keyword using OpenAlex.

    Args:
        keyword: Search keyword.
        column_names: Column names to keep in the result.
        max_result: Maximum number of results to retrieve.
        max_result: Maximum number of results to retrieve.

    Returns:
        A list of dictionaries ready for table generation.
    """
    logger.info("Getting OpenAlex papers for keyword: %s", keyword)
    papers = request_papers_with_openalex(keyword, max_result)

    if _is_verification_flavoured_query(keyword):
        papers = [paper for paper in papers if _is_digital_verification_paper(paper)]
    processed: List[Dict[str, str]] = []
    for paper in papers:
        processed.append(
            {column_name: paper.get(column_name, "") for column_name in column_names},
        )
    logger.info("Retrieved %d OpenAlex papers after column selection", len(processed))
    return processed


def get_daily_papers_by_keyword_from_semantic_scholar(
    keyword: str,
    column_names: List[str],
    max_result: int,
) -> List[Dict[str, str]]:
    """Get papers for a keyword using Semantic Scholar.

    Args:
        keyword: Search keyword.
        column_names: Column names to keep in the result.
        max_result: Maximum number of results to retrieve.

    Returns:
        A list of dictionaries ready for table generation.
    """
    logger.info("Getting Semantic Scholar papers for keyword: %s", keyword)
    papers = request_papers_with_semantic_scholar(keyword, max_result)

    if _is_verification_flavoured_query(keyword):
        papers = [paper for paper in papers if _is_digital_verification_paper(paper)]
    processed: List[Dict[str, str]] = []
    for paper in papers:
        processed.append(
            {column_name: paper.get(column_name, "") for column_name in column_names},
        )
    logger.info(
        "Retrieved %d Semantic Scholar papers after column selection",
        len(processed),
    )
    return processed


def get_daily_papers_by_keyword_from_acm(
    keyword: str,
    column_names: List[str],
    max_result: int,
) -> List[Dict[str, str]]:
    """Get papers for a keyword using the ACM Digital Library API.

    Args:
        keyword: Search keyword.
        column_names: Column names to keep in the result.
        max_result: Maximum number of results to retrieve.

    Returns:
        A list of dictionaries ready for table generation.
    """
    logger.info("Getting ACM papers for keyword: %s", keyword)
    papers = request_papers_with_acm_api(keyword, max_result)

    if _is_verification_flavoured_query(keyword):
        papers = [paper for paper in papers if _is_digital_verification_paper(paper)]
    processed: List[Dict[str, str]] = []
    for paper in papers:
        processed.append(
            {column_name: paper.get(column_name, "") for column_name in column_names},
        )
    logger.info("Retrieved %d ACM papers after column selection", len(processed))
    return processed


def get_daily_papers_by_keyword_from_dvcon(
    keyword: str,
    column_names: List[str],
    max_result: int,
) -> List[Dict[str, str]]:
    """Get DVCon-related entries for a keyword using the DVCon proceedings site.

    This helper performs a lightweight HTML-based search against
    ``https://dvcon-proceedings.org`` using the keyword and then extracts
    candidate links that look like proceedings entries. Because the site does
    not expose a documented public API, this is a best-effort scraper that
    focuses on:

    * link text containing the keyword, and
    * URLs that appear to point to individual proceedings entries.

    The results are normalised to the common schema so they can be rendered
    with :func:`generate_table`.

    Args:
        keyword: Search keyword (e.g. "UVM", "formal verification").
        column_names: Column names to keep in the result.
        max_result: Maximum number of results to retrieve.

    Returns:
        A list of dictionaries ready for table generation, restricted to
        DVCon venues.
    """
    logger.info("Getting DVCon papers for keyword via proceedings site: %s", keyword)

    base_url = "https://dvcon-proceedings.org/"
    params = {"s": keyword}

    # Use a browser-like User-Agent and referer; the site may block "generic"
    # clients with a 403 without these headers.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": base_url,
        "Connection": "keep-alive",
    }

    try:
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        response.raise_for_status()
    except requests.RequestException as exc:
        logger.error("Failed to fetch DVCon proceedings search page: %s", exc)
        raise

    soup = BeautifulSoup(response.text, "html.parser")

    results: List[Dict[str, str]] = []
    seen_links: set[str] = set()
    keyword_lower = keyword.lower()

    # Heuristic: many WordPress-based archives wrap titles in elements like
    # .entry-title a, but we also fall back to all links that look like
    # proceedings entries and whose text matches the keyword.
    candidate_links = soup.select(".entry-title a[href]") or soup.select("a[href]")

    for anchor in candidate_links:
        href = anchor.get("href", "").strip()
        title = anchor.get_text(strip=True)
        if not href or not title:
            continue

        # Skip nav / footer / non-proceedings links by a simple heuristic.
        href_lower = href.lower()
        if "dvcon-proceedings.org" not in href_lower and not href_lower.startswith(
            "/",
        ):
            continue
        if any(x in href_lower for x in ["wp-", "tag/", "category/"]):
            continue

        # Require the title text to contain the keyword (case-insensitive).
        if keyword_lower not in title.lower():
            continue

        # Normalise absolute URL.
        if href_lower.startswith("/"):
            link = urllib.parse.urljoin(base_url, href)
        else:
            link = href

        if link in seen_links:
            continue
        seen_links.add(link)

        # Best-effort year inference from the title / URL so that DVCon entries
        # render with realistic dates instead of the old 1970 placeholder.
        year_match = re.search(r"(19|20)\d{2}", f"{title} {href}", re.IGNORECASE)
        if year_match:
            year = int(year_match.group(0))
            date_value = f"{year:04d}-01-01T00:00:00Z"
        else:
            # Fallback for truly ambiguous cases.
            date_value = "1970-01-01T00:00:00Z"

        paper: Dict[str, str] = {
            "Title": remove_duplicated_spaces(title.replace("\n", " ")),
            "Abstract": "",
            "Authors": ["Unknown"],
            "Link": link,
            "Tags": ["DVConProceedings"],
            "Comment": "DVCon proceedings entry",
            "Date": date_value,
        }
        results.append(paper)

        if len(results) >= max_result:
            break

    processed: List[Dict[str, str]] = []
    for paper in results:
        processed.append(
            {column_name: paper.get(column_name, "") for column_name in column_names},
        )

    logger.info("Retrieved %d DVCon proceedings entries after selection", len(processed))
    return processed


def get_daily_papers_by_keyword_from_ieee(
    keyword: str,
    column_names: List[str],
    max_result: int,
) -> List[Dict[str, str]]:
    """Get papers for a keyword using the IEEE Xplore keyword search."""
    logger.info("Getting IEEE papers for keyword: %s", keyword)
    papers = request_papers_with_ieee_keyword(keyword, max_result)
    processed: List[Dict[str, str]] = []
    for paper in papers:
        processed.append(
            {column_name: paper.get(column_name, "") for column_name in column_names},
        )
    logger.info("Retrieved %d IEEE papers after column selection", len(processed))
    return processed


def get_daily_papers_by_keyword_with_retries_crossref(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Retry wrapper for fetching papers via CrossRef.

    This helper never raises on retry exhaustion and instead returns an empty
    list, which keeps the calling pipeline simple.
    """
    logger.info(
        "Attempting to get CrossRef papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword_from_crossref(
                keyword,
                column_names,
                max_result,
            )
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d CrossRef papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            logger.warning(
                "Received empty CrossRef list on attempt %d, retrying soon...",
                attempt + 1,
            )
            time.sleep(60)
        except Exception as exc:
            logger.error("Error on CrossRef attempt %d: %s", attempt + 1, exc)
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                logger.error(
                    "CrossRef returned HTTP %d for keyword '%s'; "
                    "skipping further CrossRef retries for this keyword.",
                    exc.code,
                    keyword,
                )
                return []
            if attempt < retries - 1:
                logger.info("Waiting 60 seconds before CrossRef retry...")
                time.sleep(60)

    logger.error("Failed to get CrossRef papers after all retry attempts")
    return []


def get_daily_papers_by_keyword_with_retries_openalex(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Retry wrapper for fetching papers via OpenAlex.

    This helper never raises on retry exhaustion and instead returns an empty
    list, which keeps the calling pipeline simple.
    """
    logger.info(
        "Attempting to get OpenAlex papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword_from_openalex(
                keyword,
                column_names,
                max_result,
            )
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d OpenAlex papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            logger.warning(
                "Received empty OpenAlex list on attempt %d, retrying soon...",
                attempt + 1,
            )
            time.sleep(60)
        except Exception as exc:
            logger.error("Error on OpenAlex attempt %d: %s", attempt + 1, exc)
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                logger.error(
                    "OpenAlex returned HTTP %d for keyword '%s'; "
                    "skipping further OpenAlex retries for this keyword.",
                    exc.code,
                    keyword,
                )
                return []
            if attempt < retries - 1:
                logger.info("Waiting 60 seconds before OpenAlex retry...")
                time.sleep(60)

    logger.error("Failed to get OpenAlex papers after all retry attempts")
    return []


def get_daily_papers_by_keyword_with_retries_semantic_scholar(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Retry wrapper for fetching papers via Semantic Scholar.

    This helper never raises on retry exhaustion and instead returns an empty
    list, which keeps the calling pipeline simple.
    """
    logger.info(
        "Attempting to get Semantic Scholar papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword_from_semantic_scholar(
                keyword,
                column_names,
                max_result,
            )
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d Semantic Scholar papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            logger.warning(
                "Received empty Semantic Scholar list on attempt %d, retrying soon...",
                attempt + 1,
            )
            time.sleep(60)
        except Exception as exc:
            logger.error("Error on Semantic Scholar attempt %d: %s", attempt + 1, exc)
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                logger.error(
                    "Semantic Scholar returned HTTP %d for keyword '%s'; "
                    "skipping further Semantic Scholar retries for this keyword.",
                    exc.code,
                    keyword,
                )
                return []
            if attempt < retries - 1:
                logger.info("Waiting 60 seconds before Semantic Scholar retry...")
                time.sleep(60)

    logger.error(
        "Failed to get Semantic Scholar papers after all retry attempts",
    )
    return []


def get_daily_papers_by_keyword_with_retries_acm(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Retry wrapper for fetching papers via the ACM Digital Library API.

    This helper never raises on retry exhaustion and instead returns an empty
    list, which keeps the calling pipeline simple.
    """
    logger.info(
        "Attempting to get ACM papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword_from_acm(
                keyword,
                column_names,
                max_result,
            )
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d ACM papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            logger.warning(
                "Received empty ACM list on attempt %d, retrying soon...",
                attempt + 1,
            )
            time.sleep(60)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error on ACM attempt %d: %s", attempt + 1, exc)
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                logger.error(
                    "ACM API returned HTTP %d for keyword '%s'; "
                    "skipping further ACM retries for this keyword.",
                    exc.code,
                    keyword,
                )
                return []
            if attempt < retries - 1:
                logger.info("Waiting 60 seconds before ACM retry...")
                time.sleep(60)

    logger.error("Failed to get ACM papers after all retry attempts")
    return []


def get_daily_papers_by_keyword_with_retries_dvcon(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Retry wrapper for fetching DVCon-related entries via proceedings search.

    This function calls :func:`get_daily_papers_by_keyword_from_dvcon` and
    retries on transient failures, mirroring the behaviour used for ACM, IEEE,
    CrossRef, OpenAlex and Semantic Scholar.

    Args:
        keyword: Search keyword.
        column_names: Column names to keep in the result.
        max_result: Maximum number of results to retrieve.
        retries: Maximum number of retries on failure.

    Returns:
        A list of dictionaries ready for table generation. Returns an empty
        list if all retries fail.
    """
    logger.info(
        "Attempting to get DVCon papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword_from_dvcon(
                keyword,
                column_names,
                max_result,
            )
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d DVCon papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            logger.warning(
                "Received empty DVCon list on attempt %d, retrying soon...",
                attempt + 1,
            )
            time.sleep(60)
        except Exception as exc:  # noqa: BLE001
            logger.error("Error on DVCon attempt %d: %s", attempt + 1, exc)
            if attempt < retries - 1:
                logger.info("Waiting 60 seconds before DVCon retry...")
                time.sleep(60)

    logger.error("Failed to get DVCon papers after all retry attempts")
    return []


def download_dvcon_assets(
    entries: List[Dict[str, str]],
    url_field: str = "Link",
    output_dir: str = "downloads/dvcon",
    delay_seconds: float = 1.0,
    allowed_extensions: Tuple[str, ...] = (".pdf", ".ppt", ".pptx", ".zip"),
) -> None:
    """Download assets (e.g. PDFs, PPTs, ZIPs) for a set of DVCon entries.

    This function takes a list of entries (typically generated by
    :func:`get_daily_papers_by_keyword_from_dvcon`) and attempts to download
    the asset for each entry. It treats the entry ``Link`` as the page URL and
    looks for links ending in ``.pdf`` on that page.

    Args:
        entries: List of DVCon entry dictionaries.
        url_field: Dictionary key holding the detail-page URL.
        output_dir: Directory where downloaded files will be saved.
        delay_seconds: Delay between downloads to avoid hammering the server.
        allowed_extensions: File extensions that are considered valid assets
            (e.g. ``(".pdf", ".ppt", ".pptx", ".zip")``).
    """
    os.makedirs(output_dir, exist_ok=True)

    # Reuse a browser-like session and headers to reduce HTTP 403 responses
    # from dvcon-proceedings.org, which may block generic clients.
    session = requests.Session()
    base_headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    for entry in entries:
        page_url = entry.get(url_field, "")
        if not page_url or not page_url.lower().startswith("http"):
            continue

        logger.info("Resolving DVCon asset from page: %s", page_url)
        try:
            headers = {
                **base_headers,
                "Referer": "https://dvcon-proceedings.org/",
            }
            resp = session.get(page_url, headers=headers, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as exc:  # noqa: BLE001
            logger.warning("Failed to fetch DVCon detail page %s: %s", page_url, exc)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        asset_link = None
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            href_lower = href.lower()
            if href and any(href_lower.endswith(ext) for ext in allowed_extensions):
                asset_link = href
                break

        if not asset_link:
            logger.info(
                "No asset link with extensions %s found on DVCon page: %s",
                allowed_extensions,
                page_url,
            )
            continue

        if asset_link.startswith("/"):
            asset_url = urllib.parse.urljoin(page_url, asset_link)
        else:
            asset_url = asset_link

        filename = asset_url.rstrip("/").split("/")[-1] or "dvcon_asset"
        filepath = os.path.join(output_dir, filename)

        # Check if file already exists
        if os.path.exists(filepath):
            logger.info("Skipping existing DVCon asset: %s", filepath)
            # Update entry Link to point to local file (use relative path)
            relative_path = os.path.relpath(filepath, start=".").replace("\\", "/")
            entry[url_field] = relative_path
            logger.debug("Updated entry link to local file: %s", relative_path)
            continue

        logger.info("Downloading DVCon asset %s -> %s", asset_url, filepath)
        try:
            headers = {
                **base_headers,
                # Many sites expect the PDF request to send the detail page as
                # Referer; this also slightly improves compatibility with basic
                # anti-bot protections.
                "Referer": page_url,
            }
            with session.get(asset_url, headers=headers, stream=True, timeout=60) as dl_resp:
                dl_resp.raise_for_status()
                with open(filepath, "wb") as out:
                    for chunk in dl_resp.iter_content(chunk_size=8192):
                        if chunk:
                            out.write(chunk)
            # Update entry Link to point to local file (use relative path)
            relative_path = os.path.relpath(filepath, start=".").replace("\\", "/")
            entry[url_field] = relative_path
            logger.debug("Updated entry link to local file: %s", relative_path)
        except requests.RequestException as exc:  # noqa: BLE001
            logger.warning("Failed to download DVCon asset %s: %s", asset_url, exc)
            # Keep original URL if download fails
            continue

        time.sleep(delay_seconds)


def extract_abstracts_from_downloaded_dvcon_pdfs(
    entries: List[Dict[str, str]],
    pdf_dir: Path = Path("downloads/dvcon"),
    url_field: str = "Link",
) -> List[Dict[str, str]]:
    """Extract abstracts from downloaded DVCon PDFs and update entry dictionaries.

    This function matches each DVCon entry to its downloaded PDF (if available),
    extracts the abstract from the PDF, and updates the entry's Abstract field.

    The matching is done by:
    1. Extracting a filename stem from the entry's URL
    2. Looking for PDFs in the download directory that match that stem

    Args:
        entries: List of DVCon entry dictionaries (typically from
            :func:`get_daily_papers_by_keyword_from_dvcon`).
        pdf_dir: Directory containing downloaded DVCon PDFs.
        url_field: Dictionary key holding the detail-page URL.

    Returns:
        The same list of entries with Abstract fields updated where PDFs were
        found and abstracts successfully extracted.
    """
    if not pdf_dir.exists():
        logger.info("DVCon PDF directory %s does not exist; skipping abstract extraction", pdf_dir)
        return entries

    pdf_files = list(pdf_dir.glob("*.pdf"))
    if not pdf_files:
        logger.info("No PDF files found in %s; skipping abstract extraction", pdf_dir)
        return entries

    logger.info("Extracting abstracts from %d downloaded DVCon PDFs", len(pdf_files))

    # Create a mapping from URL stems to PDF paths
    url_to_pdf: Dict[str, Path] = {}
    for pdf_path in pdf_files:
        # Try to match PDFs by URL stem (last part of URL path)
        stem = pdf_path.stem.lower()
        url_to_pdf[stem] = pdf_path

    updated_entries = []
    for entry in entries:
        page_url = entry.get(url_field, "")
        if not page_url:
            updated_entries.append(entry)
            continue

        # Extract a potential filename stem from the URL
        # e.g., "https://dvcon-proceedings.org/document/some-paper-title/" -> "some-paper-title"
        url_path = urllib.parse.urlparse(page_url).path.strip("/")
        url_stem = url_path.split("/")[-1].lower() if url_path else ""

        # Try to find matching PDF
        matching_pdf: Optional[Path] = None
        if url_stem and url_stem in url_to_pdf:
            matching_pdf = url_to_pdf[url_stem]
        else:
            # Fallback: try partial matching (e.g., if URL has extra suffixes)
            for stem, pdf_path in url_to_pdf.items():
                if url_stem in stem or stem in url_stem:
                    matching_pdf = pdf_path
                    break

        if matching_pdf:
            logger.info(
                "Extracting abstract from PDF %s for entry: %s",
                matching_pdf.name,
                entry.get("Title", "Unknown"),
            )
            abstract = extract_abstract_from_pdf(matching_pdf)
            if abstract:
                entry["Abstract"] = abstract
                logger.debug("Extracted abstract (length: %d chars) for: %s", len(abstract), entry.get("Title", "Unknown"))
            else:
                logger.debug("No abstract found in PDF: %s", matching_pdf.name)

            # Best-effort year inference so that DVCon entries carry a realistic
            # publication year instead of the legacy 1970 placeholder. We try,
            # in order:
            #   1. File name stem (fast, very reliable for DVCon assets).
            #   2. First-page text content around "DVCon" if necessary.
            try:
                current_year = datetime.datetime.now().year
                inferred_year: Optional[int] = None

                # 1) Infer from filename stem, e.g. "DVConEU_2025_paper_132".
                stem_match = re.search(r"(19|20)\d{2}", matching_pdf.stem)
                if stem_match:
                    candidate = int(stem_match.group(0))
                    if 1990 <= candidate <= current_year + 1:
                        inferred_year = candidate

                # 2) If filename-based inference failed, fall back to text.
                if inferred_year is None:
                    text_for_year = extract_text_with_fallback(
                        pdf_path=matching_pdf,
                        max_pages=1,
                    )
                    if text_for_year:
                        year_candidates = [
                            int(match.group(0))
                            for match in re.finditer(r"(19|20)\d{2}", text_for_year)
                        ]
                        year_candidates = [
                            y for y in year_candidates if 1990 <= y <= current_year + 1
                        ]

                        # Prefer years that appear near "DVCon" or similar.
                        if year_candidates:
                            lowered_text = text_for_year.lower()
                            for y in sorted(year_candidates, reverse=True):
                                if re.search(
                                    rf"(dvcon[^0-9]{{0,40}}{y})|({y}[^0-9]{{0,40}}dvcon)",
                                    lowered_text,
                                ):
                                    inferred_year = y
                                    break
                            if inferred_year is None:
                                inferred_year = max(year_candidates)

                # Only override obviously placeholder dates or unset dates.
                existing_date = entry.get("Date", "")
                is_placeholder = existing_date.startswith("1970-01-01") or not existing_date
                if inferred_year is not None and is_placeholder:
                    entry["Date"] = f"{inferred_year:04d}-01-01T00:00:00Z"
                    logger.debug(
                        "Inferred DVCon year %d for entry: %s",
                        inferred_year,
                        entry.get("Title", "Unknown"),
                    )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Failed to infer year from DVCon PDF %s: %s",
                    matching_pdf,
                    exc,
                )
        else:
            logger.debug("No matching PDF found for URL: %s", page_url)

        updated_entries.append(entry)

    abstracts_found = sum(1 for entry in updated_entries if entry.get("Abstract", "").strip())
    logger.info(
        "Abstract extraction complete: %d/%d entries now have abstracts",
        abstracts_found,
        len(updated_entries),
    )

    return updated_entries


def get_daily_papers_by_keyword_with_retries_ieee(
    keyword: str,
    column_names: List[str],
    max_result: int,
    retries: int = 3,
) -> List[Dict[str, str]]:
    """Retry wrapper for fetching papers via IEEE Xplore keyword search."""
    logger.info(
        "Attempting to get IEEE papers for keyword '%s' with %d retries",
        keyword,
        retries,
    )
    for attempt in range(retries):
        try:
            papers = get_daily_papers_by_keyword_from_ieee(
                keyword,
                column_names,
                max_result,
            )
            if len(papers) > 0:
                logger.info(
                    "Successfully retrieved %d IEEE papers on attempt %d",
                    len(papers),
                    attempt + 1,
                )
                return papers
            logger.warning(
                "Received empty IEEE list on attempt %d, retrying soon...",
                attempt + 1,
            )
            time.sleep(60)
        except Exception as exc:
            logger.error("Error on IEEE attempt %d: %s", attempt + 1, exc)
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                logger.error(
                    "IEEE returned HTTP %d for keyword '%s'; "
                    "skipping further IEEE retries for this keyword.",
                    exc.code,
                    keyword,
                )
                return []
            if attempt < retries - 1:
                logger.info("Waiting 60 seconds before IEEE retry...")
                time.sleep(60)

    logger.error("Failed to get IEEE papers after all retry attempts")
    return None


def _extract_raw_text_from_pdf(
    pdf_path: Path,
    max_pages: int = 2,
) -> str:
    """Extract raw text from the first pages of a PDF using ``pypdf``.

    This function is optimised for "digital" PDFs that already contain a text
    layer. It does not perform OCR; if the PDF is a scanned image, the return
    value will typically be empty or extremely short.

    Args:
        pdf_path: Path to the input PDF file.
        max_pages: Maximum number of pages to extract from, starting at page 0.

    Returns:
        Concatenated text content from up to ``max_pages`` pages.
    """
    reader = PdfReader(str(pdf_path))
    pages_to_read = min(max_pages, len(reader.pages))
    chunks: List[str] = []
    for idx in range(pages_to_read):
        page_text = reader.pages[idx].extract_text() or ""
        chunks.append(page_text)
    text = "\n".join(chunks)
    logger.debug(
        "Extracted %d characters from %s using pypdf",
        len(text),
        pdf_path,
    )
    return text


def _extract_raw_text_with_ocr(
    pdf_path: Path,
    max_pages: int = 2,
) -> str:
    """Extract text from a PDF using OCR on rendered page images.

    This uses ``pdf2image`` to render pages as images and ``pytesseract`` to
    perform OCR on those images. Both libraries are optional and must be
    installed separately; if they are missing, this function returns an empty
    string and logs a warning.

    Args:
        pdf_path: Path to the input PDF file.
        max_pages: Maximum number of pages to OCR, starting at page 0.

    Returns:
        OCR-derived text, or an empty string if OCR dependencies are not
        available or an error occurs.
    """
    if convert_from_path is None or pytesseract is None:
        logger.warning(
            "OCR requested for %s but pdf2image/pytesseract is not installed; "
            "skipping OCR step.",
            pdf_path,
        )
        return ""

    try:
        images = convert_from_path(
            pdf_path=pdf_path,
            first_page=1,
            last_page=max_pages,
        )
    except Exception as exc:  # noqa: BLE001
        # pdf2image surfaces missing Poppler via PDFInfoNotInstalledError; provide
        # a clearer, one-stop hint about how to install the system dependency.
        if "PDFInfoNotInstalledError" in type(exc).__name__ or (
            PDFInfoNotInstalledError is not None
            and isinstance(exc, PDFInfoNotInstalledError)
        ):
            logger.warning(
                (
                    "Failed to render PDF pages for OCR (%s): %s. "
                    "This usually means the Poppler utilities are not installed "
                    "or not on PATH. On Debian/Ubuntu (including WSL2) run "
                    "'sudo apt-get update && sudo apt-get install -y poppler-utils'; "
                    "on macOS use 'brew install poppler'."
                ),
                pdf_path,
                exc,
            )
        else:
            logger.warning("Failed to render PDF pages for OCR (%s): %s", pdf_path, exc)
        return ""

    ocr_chunks: List[str] = []
    for idx, image in enumerate(images, start=1):
        try:
            text = pytesseract.image_to_string(image)
            logger.debug(
                "OCR page %d of %s produced %d characters",
                idx,
                pdf_path,
                len(text),
            )
            ocr_chunks.append(text)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed OCR on page %d of %s: %s", idx, pdf_path, exc)
            continue

    return "\n".join(ocr_chunks)


def extract_text_with_fallback(
    pdf_path: Path,
    max_pages: int = 2,
    min_direct_chars: int = 200,
) -> str:
    """Extract text from a PDF, falling back to OCR for scanned files.

    The function first uses :func:`_extract_raw_text_from_pdf`. If the result
    is shorter than ``min_direct_chars``, it assumes the PDF is likely a
    scanned document and performs OCR on the first pages instead.

    Args:
        pdf_path: Path to the input PDF file.
        max_pages: Maximum number of pages to inspect.
        min_direct_chars: Minimum number of characters expected from direct
            text extraction before we consider OCR.

    Returns:
        A best-effort text representation of the first part of the PDF.
    """
    direct_text = _extract_raw_text_from_pdf(pdf_path, max_pages=max_pages)
    if len(direct_text) >= min_direct_chars:
        return direct_text

    logger.info(
        "Direct extraction from %s produced only %d characters; "
        "attempting OCR fallback.",
        pdf_path,
        len(direct_text),
    )
    ocr_text = _extract_raw_text_with_ocr(pdf_path, max_pages=max_pages)
    return ocr_text or direct_text


def extract_abstract_from_text(text: str) -> Optional[str]:
    """Extract the abstract section from raw PDF text.

    The heuristic is tuned for DVCon-style papers:

    * Search for the heading ``\"Abstract\"`` (case-insensitive).
    * Capture text from the end of that heading until the next common section
      heading (e.g. ``\"Introduction\"``, ``\"1. Introduction\"``, ``\"Keywords\"``),
      or until a large gap of blank lines.

    Args:
        text: Full or partial text content of a paper.

    Returns:
        The extracted abstract text if found; otherwise ``None``.
    """
    if not text:
        return None

    lowered = text.lower()
    match = re.search(r"\babstract\b", lowered)
    if not match:
        return None

    start = match.end()

    stop_tokens = [
        r"\b1\.\s*introduction\b",
        r"\b1\s+introduction\b",
        r"\bintroduction\b",
        r"\bkeywords\b",
        r"\bindex\s+terms\b",
    ]
    stop_positions: List[int] = []
    for pattern in stop_tokens:
        m = re.search(pattern, lowered[start:])
        if m:
            stop_positions.append(start + m.start())

    stop = min(stop_positions) if stop_positions else len(text)

    abstract_raw = text[start:stop].strip()
    lines = [ln.strip() for ln in abstract_raw.splitlines()]
    lines = [ln for ln in lines if ln]
    abstract = " ".join(lines)

    return abstract or None


def extract_abstract_from_pdf(
    pdf_path: Path,
    max_pages: int = 2,
) -> Optional[str]:
    """Extract the abstract of a DVCon paper from its PDF.

    This is a convenience wrapper that:

    1. Extracts text from the first ``max_pages`` pages of the PDF using
       :func:`extract_text_with_fallback`.
    2. Runs :func:`extract_abstract_from_text` on the result.

    Args:
        pdf_path: Path to the DVCon paper PDF.
        max_pages: Maximum number of pages to inspect for the abstract.

    Returns:
        The abstract text if a section labelled ``\"Abstract\"`` can be located,
        otherwise ``None``.
    """
    text = extract_text_with_fallback(pdf_path=pdf_path, max_pages=max_pages)
    if not text:
        logger.warning("No text extracted from PDF: %s", pdf_path)
        return None
    abstract = extract_abstract_from_text(text)
    if abstract is None:
        logger.info("No explicit 'Abstract' section found in %s", pdf_path)
    return abstract


def build_dvcon_readme_from_pdfs(
    pdf_dir: Path = Path("downloads/dvcon"),
    output_path: Path = Path("DVCON_README.md"),
) -> None:
    """Generate a small README-style Markdown file from DVCon PDFs.

    The function scans ``pdf_dir`` for ``*.pdf`` files, attempts to extract
    an abstract from each one using :func:`extract_abstract_from_pdf`, and
    writes a simple Markdown table to ``output_path`` containing the file
    stem and the abstract text.

    This is intentionally decoupled from the main ``README.md`` that is
    regenerated by the daily pipeline, so that DVCon-specific abstracts can
    be inspected or copy-pasted without interfering with the main flow.

    Args:
        pdf_dir: Directory containing DVCon PDFs (typically populated by
            :func:`download_dvcon_assets`).
        output_path: Output Markdown file path.
    """
    pdf_files = list(Path(pdf_dir).glob("*.pdf"))
    if not pdf_files:
        logger.warning("No PDF files found in %s; nothing to write.", pdf_dir)

    # Sort files by inferred year (descending) then by name to ensure a stable
    # order such that the latest conferences appear first in the README.
    def _infer_year_from_stem(stem: str) -> int:
        """Best-effort extraction of a four-digit year from a filename stem."""

        match = re.search(r"(19|20)\d{2}", stem)
        if not match:
            return 0
        try:
            return int(match.group(0))
        except ValueError:
            return 0

    pdf_files.sort(
        key=lambda p: (_infer_year_from_stem(p.stem), p.stem.lower()),
        reverse=True,
    )

    rows: List[str] = ["| Paper | Abstract |", "| --- | --- |"]

    for pdf_path in pdf_files:
        logger.info("Extracting abstract from DVCon PDF: %s", pdf_path)
        abstract = extract_abstract_from_pdf(pdf_path) or "N/A"
        title = pdf_path.stem.replace("_", " ").replace("-", " ")
        safe_abstract = abstract.replace("|", "\\|")
        rows.append(f"| {title} | {safe_abstract} |")

    content_lines: List[str] = [
        "# DVCon PDF Abstracts",
        "",
        "This file is generated from DVCon PDFs using a simple OCR-capable ",
        "pipeline. For best results, install the optional dependencies:",
        "",
        "```bash",
        "pip install pypdf pdf2image pytesseract",
        "```",
        "",
        "You may also need to install system packages for Poppler and ",
        "Tesseract OCR (see the respective project documentation).",
        "",
        "The canonical project abstract and search-friendly description are ",
        "maintained in the main `README.md`. This file is intended as a ",
        "supporting appendix that lists per-paper abstracts extracted from ",
        "DVCon PDFs.",
        "",
    ]
    content = "\n".join(content_lines + rows) + "\n"
    output_path.write_text(content, encoding="utf-8")
    logger.info("Wrote DVCon abstract README to %s", output_path)


def generate_table(
    papers: List[Dict[str, str]],
    ignore_keys: List[str] | None = None,
) -> str:
    """Convert a list of paper dictionaries into a Markdown table.

    The function sorts papers by date (newest first), formats the title as a
    markdown link, wraps long fields such as ``Abstract`` and ``Comment`` in
    collapsible details blocks, and returns a markdown table string.

    Args:
        papers: Normalised paper dictionaries.
        ignore_keys: Optional list of keys to omit from the table body
            (commonly ``["Abstract"]`` for issue templates).

    Returns:
        A markdown table string, or an empty string if ``papers`` is empty.
    """
    if ignore_keys is None:
        ignore_keys = []
    logger.info("Generating table for %d papers", len(papers))
    
    # Handle empty papers list
    if not papers:
        logger.warning("No papers provided, returning empty table")
        return ""
    
    # Sort papers by date (newest first) before formatting
    def parse_date(date_str: str) -> datetime.datetime:
        """Parse date string to datetime object for sorting.
        
        Args:
            date_str: Date string in format "YYYY-MM-DDTHH:MM:SSZ" or "YYYY-MM-DD".
        
        Returns:
            Datetime object, or epoch (1970-01-01) if parsing fails.
        """
        if not date_str:
            return datetime.datetime(1970, 1, 1)
        try:
            # Remove timezone suffix (Z) if present
            date_clean = date_str.rstrip("Z")
            if "T" in date_clean:
                # Split date and time
                date_part, time_part = date_clean.split("T", 1)
                # Parse date part
                year, month, day = map(int, date_part.split("-"))
                # Parse time part (may have microseconds)
                time_parts = time_part.split(":")
                hour = int(time_parts[0])
                minute = int(time_parts[1])
                second = int(time_parts[2].split(".")[0]) if len(time_parts) > 2 else 0
                return datetime.datetime(year, month, day, hour, minute, second)
            else:
                # Just date part
                year, month, day = map(int, date_clean.split("-"))
                return datetime.datetime(year, month, day)
        except (ValueError, AttributeError, IndexError) as exc:
            logger.debug("Failed to parse date '%s': %s", date_str, exc)
            return datetime.datetime(1970, 1, 1)
    
    # Sort papers by date (newest first), then by title for stable ordering
    sorted_papers = sorted(
        papers,
        key=lambda p: (
            parse_date(p.get("Date", "1970-01-01T00:00:00Z")),
            p.get("Title", "").lower(),
        ),
        reverse=True,  # Newest first
    )
    
    formatted_papers = []
    keys = sorted_papers[0].keys()
    for paper in sorted_papers:
        try:
            # process fixed columns
            formatted_paper = EasyDict()
            ## Title and Link
            formatted_paper.Title = "**" + "[{0}]({1})".format(paper["Title"], paper["Link"]) + "**"
            ## Process Date (format: 2021-08-01T00:00:00Z -> 2021-08-01)
            formatted_paper.Date = paper["Date"].split("T")[0]
            
            # process other columns
            for key in keys:
                if key in ["Title", "Link", "Date"] or key in ignore_keys:
                    continue
                elif key == "Abstract":
                    # add show/hide button for abstract
                    formatted_paper[key] = "<details><summary>Show</summary><p>{0}</p></details>".format(paper[key])
                elif key == "Authors":
                    # NOTE only use the first author
                    formatted_paper[key] = paper[key][0] + " et al."
                elif key == "Tags":
                    tags = ", ".join(paper[key])
                    if len(tags) > 10:
                        formatted_paper[key] = "<details><summary>{0}...</summary><p>{1}</p></details>".format(tags[:5], tags)
                    else:
                        formatted_paper[key] = tags
                elif key == "Comment":
                    if paper[key] == "":
                        formatted_paper[key] = ""
                    elif len(paper[key]) > 20:
                        formatted_paper[key] = "<details><summary>{0}...</summary><p>{1}</p></details>".format(paper[key][:5], paper[key])
                    else:
                        formatted_paper[key] = paper[key]
            formatted_papers.append(formatted_paper)
        except Exception as exc:
            logger.warning("Failed to format paper: %s", exc)
            continue

    # Handle case where all papers failed to format
    if not formatted_papers:
        logger.warning("No papers were successfully formatted, returning empty table")
        return ""

    # generate header
    columns = formatted_papers[0].keys()
    # highlight headers
    columns = ["**" + column + "**" for column in columns]
    header = "| " + " | ".join(columns) + " |"
    header = header + "\n" + "| " + " | ".join(["---"] * len(formatted_papers[0].keys())) + " |"
    # generate the body
    body = ""
    for paper in formatted_papers:
        body += "\n| " + " | ".join(paper.values()) + " |"
    
    logger.info("Successfully generated table")
    return header + body


def back_up_files() -> None:
    """Back up README and issue template files before regeneration.

    The current ``README.md`` and ``.github/ISSUE_TEMPLATE.md`` files are moved
    to ``*.bk`` siblings so that the main script can restore them in case of
    failure.
    """
    logger.info("Backing up files")

    # Back up README.md if it exists
    if os.path.exists("README.md"):
        try:
            shutil.move("README.md", "README.md.bk")
            logger.debug("Backed up README.md to README.md.bk")
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to back up README.md: %s", e)
            raise
    else:
        logger.info("README.md not found, skipping README backup")

    # Back up ISSUE_TEMPLATE.md if it exists
    if os.path.exists(".github/ISSUE_TEMPLATE.md"):
        try:
            shutil.move(".github/ISSUE_TEMPLATE.md", ".github/ISSUE_TEMPLATE.md.bk")
            logger.debug(
                "Backed up .github/ISSUE_TEMPLATE.md to .github/ISSUE_TEMPLATE.md.bk",
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to back up ISSUE_TEMPLATE.md: %s", e)
            raise
    else:
        logger.info(
            ".github/ISSUE_TEMPLATE.md not found, skipping ISSUE_TEMPLATE backup",
        )


def restore_files() -> None:
    """Restore README and issue template files from their backups."""
    logger.info("Restoring files from backup")

    # Restore README.md if backup exists
    if os.path.exists("README.md.bk"):
        try:
            shutil.move("README.md.bk", "README.md")
            logger.debug("Restored README.md from README.md.bk")
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to restore README.md: %s", e)
            raise
    else:
        logger.info("README.md.bk not found, skipping README restore")

    # Restore ISSUE_TEMPLATE.md if backup exists
    if os.path.exists(".github/ISSUE_TEMPLATE.md.bk"):
        try:
            shutil.move(".github/ISSUE_TEMPLATE.md.bk", ".github/ISSUE_TEMPLATE.md")
            logger.debug(
                "Restored .github/ISSUE_TEMPLATE.md from .github/ISSUE_TEMPLATE.md.bk",
            )
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to restore ISSUE_TEMPLATE.md: %s", e)
            raise
    else:
        logger.info(
            ".github/ISSUE_TEMPLATE.md.bk not found, skipping ISSUE_TEMPLATE restore",
        )


def remove_backups() -> None:
    """Remove backup files created by :func:`back_up_files`."""
    logger.info("Removing backup files")

    # Remove README.md backup if it exists
    if os.path.exists("README.md.bk"):
        try:
            os.remove("README.md.bk")
            logger.debug("Removed README.md.bk")
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to remove README.md.bk: %s", e)
            raise
    else:
        logger.info("README.md.bk not found, skipping removal")

    # Remove ISSUE_TEMPLATE.md backup if it exists
    if os.path.exists(".github/ISSUE_TEMPLATE.md.bk"):
        try:
            os.remove(".github/ISSUE_TEMPLATE.md.bk")
            logger.debug("Removed .github/ISSUE_TEMPLATE.md.bk")
        except Exception as e:  # noqa: BLE001
            logger.error("Failed to remove .github/ISSUE_TEMPLATE.md.bk: %s", e)
            raise
    else:
        logger.info(
            ".github/ISSUE_TEMPLATE.md.bk not found, skipping ISSUE_TEMPLATE removal",
        )


def get_daily_date() -> str:
    """Return today's date string in Beijing time for issue titles.

    The format is ``\"Month DD, YYYY\"`` (for example, ``\"March 01, 2025\"``),
    which is used when constructing the daily issue template title.
    """
    beijing_timezone = pytz.timezone("Asia/Shanghai")
    today = datetime.datetime.now(beijing_timezone)
    date_str = today.strftime("%B %d, %Y")
    logger.debug("Generated date string: %s", date_str)
    return date_str
