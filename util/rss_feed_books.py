import requests
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, date
import re

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)


def fetch_goodreads_rss(user_id, page_num=1, shelf="all"):
    url = f"https://www.goodreads.com/review/list_rss/{user_id}?page={page_num}&shelf={shelf}"
    print(f"Fetching page {page_num}: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.text


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

    return pd.DataFrame(rows)


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

    while True:
        xml_data = fetch_goodreads_rss(user_id, page, shelf)
        df = parse_rss(xml_data)

        if df.empty or len(df) < 100:
            all_dfs.append(df)
            break

        all_dfs.append(df)
        page += 1

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
