import concurrent.futures
import pandas as pd
import numpy as np
from collections import Counter
from util.rss_feed_books import fetch_users_books

def _make_json_serializable(obj):
    """
    Convert NumPy types to standard Python types for JSON serialization
    
    Args:
        obj: Any object that might contain NumPy types
        
    Returns:
        Object with NumPy types converted to standard Python types
    """
    if isinstance(obj, (np.integer, np.int64)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: _make_json_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_make_json_serializable(i) for i in obj]
    else:
        return obj

def calculate_blend_metrics(df1, df2):
    """
    Calculate various metrics to quantify the compatibility between two users' reading habits
    
    Args:
        df1 (DataFrame): First user's books
        df2 (DataFrame): Second user's books
        
    Returns:
        dict: Dictionary containing various blend metrics
    """
    metrics = {}
    
    # Filter for books in the "read" shelf
    df1_read = df1[df1["user_shelves"].str.contains("read", case=False, na=False)]
    df2_read = df2[df2["user_shelves"].str.contains("read", case=False, na=False)]
    
    # Basic count metrics - use read shelf
    metrics["user1_total_book_count"] = int(len(df1))
    metrics["user2_total_book_count"] = int(len(df2))
    metrics["user1_read_count"] = int(len(df1_read))
    metrics["user2_read_count"] = int(len(df2_read))
    
    # Calculate total pages read (ignoring None values) - use read shelf
    metrics["user1_pages_read"] = float(df1_read["num_pages"].dropna().sum())
    metrics["user2_pages_read"] = float(df2_read["num_pages"].dropna().sum())

    # Calculate average page length (ignoring None values) - use read shelf
    metrics["user1_avg_page_length"] = round(float(df1_read["num_pages"].dropna().mean()), 2) if not df1_read["num_pages"].dropna().empty else None
    metrics["user2_avg_page_length"] = round(float(df2_read["num_pages"].dropna().mean()), 2) if not df2_read["num_pages"].dropna().empty else None
    
    # Calculate median page length - use read shelf
    metrics["user1_median_page_length"] = round(float(df1_read["num_pages"].dropna().median()), 2) if not df1_read["num_pages"].dropna().empty else None
    metrics["user2_median_page_length"] = round(float(df2_read["num_pages"].dropna().median()), 2) if not df2_read["num_pages"].dropna().empty else None

    # Calculate total ratings given - use all shelves
    metrics["user1_ratings_given"] = int((df1["user_rating"] > 0).sum())
    metrics["user2_ratings_given"] = int((df2["user_rating"] > 0).sum())
    
    # Average rating comparison - use all shelves
    metrics["user1_avg_rating"] = round(float(df1["user_rating"][(df1["user_rating"] != 0) & df1["user_rating"].notna()].mean()), 2) if not df1["user_rating"][(df1["user_rating"] != 0) & df1["user_rating"].notna()].empty else None
    metrics["user2_avg_rating"] = round(float(df2["user_rating"][(df2["user_rating"] != 0) & df2["user_rating"].notna()].mean()), 2) if not df2["user_rating"][(df2["user_rating"] != 0) & df2["user_rating"].notna()].empty else None
    
    # Book overlap - use all shelves
    common_books_count = len(set(df1["book_id"]) & set(df2["book_id"]))
    metrics["common_books_count"] = common_books_count
    
    # Read book overlap - only books both have read
    common_read_books_count = len(set(df1_read["book_id"]) & set(df2_read["book_id"]))
    metrics["common_read_books_count"] = common_read_books_count
    
    # Publication year preferences - use read shelf
    metrics["user1_avg_pub_year"] = round(float(df1_read["book_published"].dropna().mean())) if not df1_read["book_published"].dropna().empty else None
    metrics["user2_avg_pub_year"] = round(float(df2_read["book_published"].dropna().mean())) if not df2_read["book_published"].dropna().empty else None
    
    # Oldest book read - use read shelf
    metrics["user1_oldest_book"] = int(df1_read["book_published"].dropna().min()) if not df1_read["book_published"].dropna().empty else None
    metrics["user2_oldest_book"] = int(df2_read["book_published"].dropna().min()) if not df2_read["book_published"].dropna().empty else None
    
    # Author overlap - use all shelves but with enhanced details
    user1_authors = set(df1["author"].dropna())
    user2_authors = set(df2["author"].dropna())
    common_authors = user1_authors & user2_authors
    metrics["common_authors_count"] = len(common_authors)
    
    # Enhanced author details
    author_details = []
    for author in common_authors:
        user1_books_by_author = df1[df1["author"] == author].to_dict(orient="records")
        user2_books_by_author = df2[df2["author"] == author].to_dict(orient="records")
        
        author_detail = {
            "author": author,
            "user1_books": [{"title": book["title"], "shelves": book["user_shelves"], "book_id": book["book_id"]} for book in user1_books_by_author],
            "user2_books": [{"title": book["title"], "shelves": book["user_shelves"], "book_id": book["book_id"]} for book in user2_books_by_author]
        }
        author_details.append(author_detail)
    
    # Sort authors by total book count
    author_details.sort(key=lambda x: len(x["user1_books"]) + len(x["user2_books"]), reverse=True)
    metrics["common_authors_details"] = author_details
    
    return metrics

def find_common_books(user1_books, user2_books):
    """
    Find books that both users have in their shelves, including which shelf each user has the book in
    
    Args:
        user1_books: List of books for user 1
        user2_books: List of books for user 2
        
    Returns:
        list: List of common books with both users' ratings and shelf information
    """
    # Create dictionaries for faster lookup
    user1_dict = {book["book_id"]: book for book in user1_books if "book_id" in book}
    user2_dict = {book["book_id"]: book for book in user2_books if "book_id" in book}
    
    # Find common book IDs
    common_ids = set(user1_dict.keys()) & set(user2_dict.keys())
    
    # Build common books list with both users' ratings and shelf information
    common_books = []
    for book_id in common_ids:
        book1 = user1_dict[book_id]
        book2 = user2_dict[book_id]
        
        common_book = {
            "book_id": book_id,
            "title": book1["title"],
            "author": book1["author"],
            "link": book1["link"],
            "image": book1["image_large"],
            "user1_rating": book1.get("user_rating"),
            "user2_rating": book2.get("user_rating"),
            "user1_review": book1.get("user_review"),
            "user2_review": book2.get("user_review"),
            "user1_shelves": book1.get("user_shelves", ""),
            "user2_shelves": book2.get("user_shelves", ""),
            "average_rating": book1["average_rating"],
            "publication_year": book1.get("book_published")
        }
        common_books.append(common_book)
    
    return common_books

def blend_two_users(user_id1, user_id2, shelf="all"):
    """
    Fetch Goodreads shelves for two users in parallel and return a combined JSON output
    with compatibility metrics and reading insights

    Args:
        user_id1 (str): First Goodreads user ID
        user_id2 (str): Second Goodreads user ID
        shelf (str): Which shelf to fetch, defaults to "all"

    Returns:
        dict: Combined JSON with results separated by user IDs and blend metrics
    """

    # Define a worker function that will be executed in parallel
    def fetch_user_books(user_id):
        return {
            "user_id": user_id,
            "books": fetch_users_books(user_id, shelf=shelf, return_type="json")
        }

    # Use ThreadPoolExecutor to run the fetches in parallel
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit both tasks
        future1 = executor.submit(fetch_user_books, user_id1)
        future2 = executor.submit(fetch_user_books, user_id2)

        # Wait for both to complete and get results
        result1 = future1.result()
        result2 = future2.result()

    # Calculate blend metrics
    user1_books = result1["books"]
    user2_books = result2["books"]

    # Convert to DataFrames for easier analysis
    df1 = pd.DataFrame(user1_books)
    df2 = pd.DataFrame(user2_books)

    # Calculate metrics
    blend_metrics = calculate_blend_metrics(df1, df2)

    # Find common books
    common_books = find_common_books(user1_books, user2_books)

    # Combine results into a single JSON output
    combined_results = {
        "results": [result1, result2],
        "blend_metrics": blend_metrics,
        "common_books": common_books
    }

    # Convert NumPy types to Python native types for JSON serialization
    combined_results = _make_json_serializable(combined_results)

    return combined_results

if __name__ == "__main__":
    user_id1 = "42944663"
    user_id2 = "48799880"
    combined_results = blend_two_users(user_id1, user_id2)
    print(combined_results)