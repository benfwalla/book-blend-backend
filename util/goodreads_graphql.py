"""Read Goodreads' public AppSync data used by its own web application."""

import json
import re
import threading
import time
from datetime import datetime, timezone

import requests

from util.goodreads_images import high_resolution_goodreads_image


BASE_URL = "https://www.goodreads.com"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
_NEXT_DATA = re.compile(
    r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
    re.DOTALL,
)
_APP_CHUNK = re.compile(r'src="(/_next/static/chunks/pages/_app-[0-9a-f]+\.js)"')
_APPSYNC_ENDPOINT = re.compile(
    r'"endpoint":"(https://[a-z0-9.\-]+\.appsync-api\.[a-z0-9.\-]+/graphql)"'
)
_APPSYNC_KEY = re.compile(r"da2-[a-z0-9]+")

_config = None
_config_lock = threading.Lock()


def _request(method, url, attempts=3, **kwargs):
    last_error = None
    request_headers = {**HEADERS, **kwargs.pop("headers", {})}
    for attempt in range(attempts):
        try:
            response = requests.request(
                method,
                url,
                headers=request_headers,
                timeout=(5, 30),
                **kwargs,
            )
            if response.status_code not in (429, 500, 502, 503, 504):
                response.raise_for_status()
                return response
            last_error = requests.HTTPError(
                f"Goodreads returned {response.status_code} for {url}",
                response=response,
            )
        except (requests.Timeout, requests.ConnectionError) as error:
            last_error = error
        if attempt < attempts - 1:
            time.sleep(0.5 * (attempt + 1))
    raise last_error


def _discover_graphql_config():
    global _config
    if _config:
        return _config

    with _config_lock:
        if _config:
            return _config

        page = _request("GET", f"{BASE_URL}/giveaway").text
        next_data_match = _NEXT_DATA.search(page)
        app_chunk_match = _APP_CHUNK.search(page)
        if not next_data_match or not app_chunk_match:
            raise RuntimeError("Goodreads did not expose its public GraphQL configuration")

        next_data = json.loads(next_data_match.group(1))
        api_key = next_data.get("props", {}).get("pageProps", {}).get("apiKey")
        if not isinstance(api_key, str) or not _APPSYNC_KEY.fullmatch(api_key):
            raise RuntimeError("Goodreads did not expose a valid public GraphQL key")

        bundle = _request("GET", f"{BASE_URL}{app_chunk_match.group(1)}").text
        endpoints = list(_APPSYNC_ENDPOINT.finditer(bundle))
        if not endpoints:
            raise RuntimeError("Goodreads did not expose its public GraphQL endpoint")

        endpoint = None
        for index, match in enumerate(endpoints):
            next_start = endpoints[index + 1].start() if index + 1 < len(endpoints) else len(bundle)
            if '"shortName":"Prod"' in bundle[match.end():next_start]:
                endpoint = match.group(1)
                break
        endpoint = endpoint or endpoints[0].group(1)
        _config = (endpoint, api_key)
        return _config


def _graphql(query, variables=None):
    endpoint, api_key = _discover_graphql_config()
    response = _request(
        "POST",
        endpoint,
        headers={"x-api-key": api_key, "content-type": "application/json"},
        json={"query": query, "variables": variables or {}},
    )
    body = response.json()
    if body.get("data") is None:
        raise RuntimeError(f"Goodreads GraphQL error: {body.get('errors')}")
    return body["data"]


def _year_from_milliseconds(value):
    if not value:
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).year
    except (TypeError, ValueError, OSError):
        return None


def enrich_books(books, batch_size=50):
    """Add page count, original publication year, and Goodreads genres in batches."""
    numeric_books = [book for book in books if str(book.get("book_id", "")).isdigit()]
    for start in range(0, len(numeric_books), batch_size):
        batch = numeric_books[start:start + batch_size]
        selections = []
        for index, book in enumerate(batch):
            selections.append(
                f'b{index}: getBookByLegacyId(legacyId: {int(book["book_id"])}) {{ '
                "legacyId details { numPages publicationTime } "
                "work { details { publicationTime } } "
                "bookGenres { genre { name } } }"
            )
        data = _graphql("query { " + " ".join(selections) + " }")

        by_id = {
            str(value.get("legacyId")): value
            for value in data.values()
            if isinstance(value, dict) and value.get("legacyId")
        }
        for book in batch:
            metadata = by_id.get(str(book["book_id"]))
            if not metadata:
                continue
            details = metadata.get("details") or {}
            work_details = (metadata.get("work") or {}).get("details") or {}
            book["num_pages"] = details.get("numPages")
            book["book_published"] = _year_from_milliseconds(
                work_details.get("publicationTime") or details.get("publicationTime")
            )
            book["genres"] = [
                genre["genre"]["name"]
                for genre in metadata.get("bookGenres") or []
                if (genre.get("genre") or {}).get("name")
            ]
    return books


def fetch_all_friends(user_id, page_size=100):
    """Return every public Goodreads friend through the paginated connection."""
    query = """
    query($legacyId: Int!, $pagination: PaginationInput) {
      getUser(legacyId: $legacyId) {
        friendsCount
        friends(paginationInput: $pagination) {
          totalCount
          edges { node { legacyId name webUrl imageUrlSquare } }
          pageInfo { hasNextPage nextPageToken }
        }
      }
    }
    """
    friends = {}
    token = None
    expected_count = None
    while True:
        pagination = {"limit": page_size}
        if token:
            pagination["after"] = token
        user = _graphql(
            query,
            {"legacyId": int(user_id), "pagination": pagination},
        ).get("getUser") or {}
        connection = user.get("friends") or {}
        expected_count = connection.get("totalCount", user.get("friendsCount"))
        for edge in connection.get("edges") or []:
            node = edge.get("node") or {}
            friend_id = node.get("legacyId")
            if friend_id is None:
                continue
            friends[str(friend_id)] = {
                "name": node.get("name") or "Goodreads user",
                "image_url": high_resolution_goodreads_image(node.get("imageUrlSquare") or ""),
                "id": str(friend_id),
                "profile_url": node.get("webUrl") or f"{BASE_URL}/user/show/{friend_id}",
                "book_count": "",
                "connection_type": "friend",
            }
        page_info = connection.get("pageInfo") or {}
        next_token = page_info.get("nextPageToken")
        if not page_info.get("hasNextPage") or not next_token or next_token == token:
            break
        token = next_token

    if expected_count is not None and len(friends) < int(expected_count):
        raise RuntimeError(
            f"Goodreads returned only {len(friends)} of {expected_count} friends for user {user_id}"
        )
    return list(friends.values())
