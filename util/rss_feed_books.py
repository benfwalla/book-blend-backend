import requests
import xml.etree.ElementTree as ET
import pandas as pd
import concurrent.futures
import json
from datetime import datetime, date
import re
import time
from bs4 import BeautifulSoup

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

BOOK_COLUMNS = [
    "title",
    "author",
    "user_shelves",
    "link",
    "isbn",
    "average_rating",
    "user_rating",
    "user_review",
    "read_at",
    "date_added",
    "book_id",
    "num_pages",
    "book_published",
    "image_small",
    "image_medium",
    "image_large",
]


class GoodreadsShelfFeedUnavailable(Exception):
    """Raised when Goodreads' public shelf RSS feed is unavailable."""


GOODREADS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

DEFAULT_SHELVES = ("read", "currently-reading", "to-read")
WIDGET_SORTS = (
    "date_added",
    "date_read",
    "title",
    "author",
    "rating",
    "avg_rating",
    "num_ratings",
    "num_pages",
    "date_pub",
    "position",
    "cover",
    "year_pub",
    "date_pub_edition",
    "date_started",
    "date_updated",
    "recommender",
    "review",
    "read_count",
    "votes",
    "comments",
    "notes",
    "isbn",
    "isbn13",
    "asin",
    "format",
    "shelves",
    "owned",
    "date_purchased",
    "purchase_location",
    "condition",
)


def _get_goodreads(url, attempts=3, **kwargs):
    """GET a public Goodreads page with small, bounded transient retries."""
    last_error = None
    for attempt in range(attempts):
        try:
            response = requests.get(
                url,
                headers=GOODREADS_HEADERS,
                timeout=(5, 20),
                **kwargs,
            )
            if response.status_code not in (429, 500, 502, 503, 504):
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


def fetch_goodreads_rss(user_id, page_num=1, shelf="all"):
    url = f"https://www.goodreads.com/review/list_rss/{user_id}?page={page_num}&shelf={shelf}"
    print(f"Fetching page {page_num}: {url}")
    response = _get_goodreads(url)
    if response.status_code == 404:
        raise GoodreadsShelfFeedUnavailable(
            "Goodreads has retired its public shelf feeds, so new blends are temporarily unavailable."
        )
    response.raise_for_status()
    return response.text


def _make_book_row(book_id, title, author, link, image, shelf, read_year=None, user_rating=0):
    absolute_link = link if link.startswith("http") else f"https://www.goodreads.com{link}"
    return {
        "title": title.strip(),
        "author": author.strip(),
        "user_shelves": shelf,
        "link": absolute_link,
        "isbn": None,
        "average_rating": 0.0,
        "user_rating": user_rating,
        "user_review": "",
        "read_at": date(read_year, 12, 31) if read_year else None,
        "date_added": None,
        "book_id": str(book_id),
        "num_pages": None,
        "book_published": None,
        "image_small": image,
        "image_medium": image,
        "image_large": image,
    }


def _fetch_profile_shelf_counts(user_id):
    """Return the public profile's authoritative default-shelf counts."""
    url = f"https://www.goodreads.com/user/show/{user_id}"
    response = _get_goodreads(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    counts = {}
    for anchor in soup.select('#shelves a[href*="shelf="]'):
        shelf_match = re.search(r"[?&]shelf=([^&]+)", anchor.get("href", ""))
        count_match = re.search(r"\(([\d,]+)\)", anchor.get_text(" ", strip=True))
        if shelf_match and count_match:
            counts[shelf_match.group(1)] = int(count_match.group(1).replace(",", ""))
    return counts


def _decode_widget_html(script):
    """Decode the HTML string assigned to a Goodreads widget's JS variable."""
    marker = "var widget_code = '"
    start = script.index(marker) + len(marker)
    end = script.index("'\n  var widget_div", start)
    encoded = script[start:end].replace("\\/", "/").replace("\\'", "'")
    return json.loads(f'"{encoded}"')


def _widget_rating(item):
    rating = 0.0
    for image in item.select('[class^="gr_custom_rating"] img'):
        filename = image.get("src", "").rsplit("/", 1)[-1].lower()
        if "inactive" in filename:
            continue
        if "three_quarter" in filename:
            rating += 0.75
        elif "half" in filename:
            rating += 0.5
        elif "quarter" in filename:
            rating += 0.25
        else:
            rating += 1.0
    return rating


def _fetch_shelf_widget(user_id, shelf, sort, order, cache_key=""):
    """Fetch up to 100 books from one end of a public Goodreads shelf."""
    url = f"https://www.goodreads.com/review/custom_widget/{user_id}.BookBlend:%20{shelf}"
    response = _get_goodreads(
        url,
        params={
            "shelf": shelf,
            "sort": sort,
            "order": order,
            "num_books": 100,
            "show_author": 1,
            "show_cover": 1,
            "show_rating": 1,
            "show_review": 0,
            "show_tags": 0,
            "show_title": 1,
            "cover_size": "small",
            "widget_id": f"bookblend-{shelf}-{sort}-{order}-{cache_key}",
        },
    )
    response.raise_for_status()
    soup = BeautifulSoup(_decode_widget_html(response.text), "html.parser")
    books = []

    for item in soup.select('[class^="gr_custom_each_container"]'):
        title_node = item.select_one('[class^="gr_custom_title"]')
        author_node = item.select_one('[class^="gr_custom_author"] a')
        image_node = item.select_one('[class^="gr_custom_book_container"] img')
        review_link = item.select_one('[class^="gr_custom_title"] a')
        if not title_node or not review_link:
            continue

        image = image_node.get("src", "") if image_node else ""
        book_match = re.search(r"/(\d+)\._", image)
        review_match = re.search(r"/review/show/(\d+)", review_link.get("href", ""))
        # Coverless books do not expose a book id in the widget. The review id
        # is still stable and keeps the entry from being silently discarded.
        if not book_match and not review_match:
            continue
        book_id = book_match.group(1) if book_match else f"review-{review_match.group(1)}"
        link = (
            f"/book/show/{book_id}"
            if book_match
            else review_link.get("href", "")
        )
        books.append(
            _make_book_row(
                book_id=book_id,
                title=title_node.get_text(" ", strip=True),
                author=author_node.get_text(" ", strip=True) if author_node else "Unknown",
                link=link,
                image=image,
                shelf=shelf,
                user_rating=_widget_rating(item),
            )
        )

    return books


def _fetch_complete_shelf(user_id, shelf, expected_count):
    """Combine opposing widget sorts until the public shelf count is met."""
    if expected_count <= 0:
        return []

    books = {}
    for sort in WIDGET_SORTS:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            pages = executor.map(
                lambda order: _fetch_shelf_widget(user_id, shelf, sort, order),
                ("d", "a"),
            )
            for page in pages:
                books.update((book["book_id"], book) for book in page)

        if len(books) >= expected_count:
            break

    # Sort extremes cover most large shelves. A few random, cache-busted
    # samples fill records that tie across every deterministic sort (for
    # example books with no ISBN, date, rating, or review metadata).
    random_attempts = min(20, max(4, (expected_count + 99) // 100))
    for attempt in range(random_attempts):
        if len(books) >= expected_count:
            break
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            pages = executor.map(
                lambda order: _fetch_shelf_widget(
                    user_id,
                    shelf,
                    "random",
                    order,
                    cache_key=str(attempt),
                ),
                ("d", "a"),
            )
            for page in pages:
                books.update((book["book_id"], book) for book in page)

    if len(books) < expected_count:
        raise GoodreadsShelfFeedUnavailable(
            f"Goodreads exposed only {len(books)} of {expected_count} books "
            f"on the public {shelf} shelf for user {user_id}."
        )
    return list(books.values())


def fetch_users_books_from_public_pages(user_id, shelf="all"):
    """Fetch complete public shelves through Goodreads' embeddable widgets."""
    counts = _fetch_profile_shelf_counts(user_id)
    if not counts:
        raise GoodreadsShelfFeedUnavailable(
            f"Goodreads did not expose public shelf counts for user {user_id}."
        )
    shelves = DEFAULT_SHELVES if shelf == "all" else (shelf,)
    books = []

    # A blend already fetches two users in parallel. Process shelves in order
    # while each shelf fetches its two ends concurrently, limiting a blend to
    # four simultaneous Goodreads requests.
    for name in shelves:
        books.extend(_fetch_complete_shelf(user_id, name, counts.get(name, 0)))

    return books


def parse_rss(xml_data):
    root = ET.fromstring(xml_data)
    items = root.find("channel").findall("item")

    rows = []
    for item in items:
        row = {
            "title": item.findtext("title").strip(),
            "author": re.sub(r'\s+', ' ', item.findtext("author_name")).strip(),
            "user_shelves": item.findtext("user_shelves") or "read",
            "link": f"https://www.goodreads.com/book/show/{item.findtext('book_id')}",
            "isbn": item.findtext("isbn"),
            "average_rating": float(item.findtext("average_rating")),
            "user_rating": int(item.findtext("user_rating")) if item.findtext("user_rating") else None,
            "user_review": item.findtext("user_review") or "",
            "read_at": to_date(item.findtext("user_read_at")),
            "date_added": to_date(item.findtext("user_date_added")),
            "book_id": item.findtext("book_id"),
            "num_pages": None,
            "book_published": int(item.findtext("book_published")) if item.findtext("book_published") else None,
            "image_small": item.findtext("book_small_image_url"),
            "image_medium": item.findtext("book_medium_image_url"),
            "image_large": item.findtext("book_large_image_url")
        }

        book_elem = item.find("book")
        if book_elem is not None:
            row["book_id"] = book_elem.attrib.get("id")
            num_pages = book_elem.findtext("num_pages")
            if num_pages and num_pages.isdigit():
                row["num_pages"] = int(num_pages)

        rows.append(row)

    # Preserve the expected schema for users with an empty or private shelf.
    # Downstream blend calculations can operate on an empty DataFrame, but not
    # on one with no columns at all.
    return pd.DataFrame(rows, columns=BOOK_COLUMNS)


def to_date(rss_datetime):
    if not rss_datetime:
        return None
    try:
        return datetime.strptime(rss_datetime.strip(), "%a, %d %b %Y %H:%M:%S %z").date()
    except ValueError:
        return None


# Auto-paginate and concatenate all results
def fetch_users_books(user_id, shelf="all", return_type="df"):
    all_dfs = []
    page = 1

    try:
        while True:
            xml_data = fetch_goodreads_rss(user_id, page, shelf)
            df = parse_rss(xml_data)

            if df.empty or len(df) < 100:
                all_dfs.append(df)
                break

            all_dfs.append(df)
            page += 1
    except (GoodreadsShelfFeedUnavailable, requests.RequestException):
        public_books = fetch_users_books_from_public_pages(user_id, shelf=shelf)
        all_dfs = [pd.DataFrame(public_books, columns=BOOK_COLUMNS)]

    full_df = pd.concat(all_dfs, ignore_index=True)

    # Sort by user_shelves priority
    shelf_order = ["currently-reading", "to-read", "read"]
    full_df["shelf_order"] = full_df["user_shelves"].apply(
        lambda s: shelf_order.index(s) if s in shelf_order else len(shelf_order)
    )
    full_df = full_df.sort_values("shelf_order").drop(columns="shelf_order").reset_index(drop=True)

    if return_type == "json":
        # Convert date fields to ISO strings
        for col in ["read_at", "date_added"]:
            full_df[col] = full_df[col].apply(lambda x: x.isoformat() if isinstance(x, (datetime, date)) else x)

        return full_df.to_dict(orient="records")

    return full_df



if __name__ == "__main__":

    # Usage
    df_all = fetch_users_books("42944663", "all", "dataframe")
    print(df_all)
