import requests
import xml.etree.ElementTree as ET
import pandas as pd
import concurrent.futures
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


def _get_goodreads(url, attempts=3):
    """GET a public Goodreads page with small, bounded transient retries."""
    last_error = None
    for attempt in range(attempts):
        try:
            response = requests.get(url, headers=GOODREADS_HEADERS, timeout=(5, 20))
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


def _split_title_author(label):
    """Split Goodreads image labels of the form '<title> by <author>'."""
    if " by " in label:
        return label.rsplit(" by ", 1)
    return label, "Unknown"


def _make_book_row(book_id, title, author, link, image, shelf, read_year=None, user_rating=0):
    return {
        "title": title.strip(),
        "author": author.strip(),
        "user_shelves": shelf,
        "link": f"https://www.goodreads.com{link}",
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


def _fetch_year_in_books(user_id, year):
    """Fetch books Goodreads exposes on a public Year in Books page."""
    url = f"https://www.goodreads.com/user/year_in_books/{year}/{user_id}"
    try:
        response = _get_goodreads(url)
    except requests.RequestException as error:
        # A single missing year should not discard every other year we could
        # successfully reconstruct for the user.
        print(f"Skipping Goodreads Year in Books {year} for {user_id}: {error}")
        return []
    # Goodreads returns 404 when a user has no public Year in Books page for
    # a particular year. That is an empty year, not a failed user scrape.
    if response.status_code == 404:
        return []
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    books = []
    for tooltip in soup.select(".bookCovers .bookTooltip[data-resource-id]"):
        anchor = tooltip.select_one('a[href*="/book/show/"]')
        image = anchor.find("img") if anchor else None
        if not anchor or not image:
            continue

        title, author = _split_title_author(image.get("alt", ""))
        rating_container = tooltip.find_parent("span", class_="doubleSizeCover")
        user_rating = len(rating_container.select(".readOnlyStar.full")) if rating_container else 0
        books.append(
            _make_book_row(
                book_id=tooltip.get("data-resource-id"),
                title=title,
                author=author,
                link=anchor.get("href"),
                image=image.get("src", ""),
                shelf="read",
                read_year=year,
                user_rating=user_rating,
            )
        )

    return books


def _fetch_profile_books(user_id):
    """Fetch favorite/read and currently-reading books from a public profile."""
    url = f"https://www.goodreads.com/user/show/{user_id}"
    response = _get_goodreads(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    books = []

    for anchor in soup.select('#featured_shelf a[href*="/book/show/"]'):
        image = anchor.find("img")
        match = re.search(r"/book/show/(\d+)", anchor.get("href", ""))
        if not image or not match:
            continue
        title, author = _split_title_author(image.get("alt", ""))
        books.append(
            _make_book_row(
                book_id=match.group(1),
                title=title,
                author=author,
                link=anchor.get("href"),
                image=image.get("src", ""),
                shelf="read",
            )
        )

    for update in soup.select("#currentlyReadingReviews .Updates"):
        anchor = update.select_one('a[href*="/book/show/"]')
        match = re.search(r"/book/show/(\d+)", anchor.get("href", "")) if anchor else None
        if not anchor or not match:
            continue
        title_node = update.select_one("a.bookTitle")
        author_node = update.select_one("a.authorName")
        image = anchor.find("img")
        books.append(
            _make_book_row(
                book_id=match.group(1),
                title=title_node.get_text(" ", strip=True) if title_node else anchor.get("title", "Unknown"),
                author=author_node.get_text(" ", strip=True) if author_node else "Unknown",
                link=anchor.get("href"),
                image=image.get("src", "") if image else "",
                shelf="currently-reading",
            )
        )

    return books


def fetch_users_books_from_public_pages(user_id, shelf="all"):
    """Reconstruct public reading history after Goodreads retired shelf RSS.

    Year in Books pages expose dated read books, while the profile exposes
    favorites and currently-reading titles. Goodreads no longer exposes the
    complete to-read shelf to signed-out users, so this is intentionally a
    best-effort public dataset.
    """
    books = []
    if shelf in ("all", "read"):
        years = range(2007, datetime.now().year + 1)
        # Keep concurrency conservative: two users are scraped in parallel by
        # blend.py, so this still permits eight simultaneous Goodreads calls.
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            for year_books in executor.map(lambda year: _fetch_year_in_books(user_id, year), years):
                books.extend(year_books)

    profile_books = _fetch_profile_books(user_id)
    if shelf == "all":
        books.extend(profile_books)
    elif shelf == "read":
        books.extend(book for book in profile_books if book["user_shelves"] == "read")
    elif shelf == "currently-reading":
        books.extend(book for book in profile_books if book["user_shelves"] == "currently-reading")

    # Prefer an explicit currently-reading profile entry over an older read
    # entry, and otherwise keep the richer Year in Books record.
    deduped = {}
    for book in books:
        existing = deduped.get(book["book_id"])
        if not existing or book["user_shelves"] == "currently-reading":
            deduped[book["book_id"]] = book
    return list(deduped.values())


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
