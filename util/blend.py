import concurrent.futures
import pandas as pd
import math
from collections import Counter
from util.rss_feed_books import fetch_users_books

def fetch_two_users_books(user_id1, user_id2, shelf="all"):
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
    
    return combined_results

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
    
    # Basic count metrics
    metrics["user1_book_count"] = len(df1)
    metrics["user2_book_count"] = len(df2)
    
    # Calculate total pages read (ignoring None values)
    metrics["user1_pages_read"] = df1["num_pages"].dropna().sum()
    metrics["user2_pages_read"] = df2["num_pages"].dropna().sum()
    
    # Average rating comparison
    metrics["user1_avg_rating"] = df1["user_rating"].dropna().mean()
    metrics["user2_avg_rating"] = df2["user_rating"].dropna().mean()
    
    # Extract genres/shelves
    user1_shelves = " ".join([s for s in df1["user_shelves"] if isinstance(s, str)])
    user2_shelves = " ".join([s for s in df2["user_shelves"] if isinstance(s, str)])
    
    user1_shelf_counts = Counter([shelf.strip() for shelf in user1_shelves.split(",")])
    user2_shelf_counts = Counter([shelf.strip() for shelf in user2_shelves.split(",")])
    
    # Common shelves/genres
    common_shelves = set(user1_shelf_counts.keys()) & set(user2_shelf_counts.keys())
    metrics["common_shelves"] = list(common_shelves)
    metrics["common_shelves_count"] = len(common_shelves)
    
    # Book overlap
    common_books_count = len(set(df1["book_id"]) & set(df2["book_id"]))
    metrics["common_books_count"] = common_books_count
    
    # Reading speed and volume comparison
    try:
        if not df1["read_at"].dropna().empty and not df1["date_added"].dropna().empty:
            df1_with_dates = df1.dropna(subset=["read_at", "date_added"])
            df1_with_dates["read_duration"] = pd.to_datetime(df1_with_dates["read_at"]) - pd.to_datetime(df1_with_dates["date_added"])
            metrics["user1_avg_reading_days"] = df1_with_dates["read_duration"].dt.days.mean()
    except Exception:
        metrics["user1_avg_reading_days"] = None
    
    try:
        if not df2["read_at"].dropna().empty and not df2["date_added"].dropna().empty:
            df2_with_dates = df2.dropna(subset=["read_at", "date_added"])
            df2_with_dates["read_duration"] = pd.to_datetime(df2_with_dates["read_at"]) - pd.to_datetime(df2_with_dates["date_added"])
            metrics["user2_avg_reading_days"] = df2_with_dates["read_duration"].dt.days.mean()
    except Exception:
        metrics["user2_avg_reading_days"] = None
    
    # Author overlap
    user1_authors = set(df1["author"].dropna())
    user2_authors = set(df2["author"].dropna())
    common_authors = user1_authors & user2_authors
    metrics["common_authors_count"] = len(common_authors)
    metrics["common_authors"] = list(common_authors)
    
    # Publication year preferences
    metrics["user1_avg_pub_year"] = df1["book_published"].dropna().mean()
    metrics["user2_avg_pub_year"] = df2["book_published"].dropna().mean()
    
    # Calculate a blend score (0-100)
    blend_score = calculate_blend_score(df1, df2, common_books_count, common_authors, common_shelves)
    metrics["blend_score"] = blend_score
    
    # Add reading insights
    insights = generate_reading_insights(metrics)
    metrics["insights"] = insights
    
    return metrics

def calculate_blend_score(df1, df2, common_books_count, common_authors, common_shelves):
    """
    Calculate an overall blend score from 0-100
    
    Args:
        df1, df2: User DataFrames
        common_books_count: Number of books in common
        common_authors: Set of authors in common
        common_shelves: Set of shelves/genres in common
        
    Returns:
        float: Blend score from 0-100
    """
    max_books = max(len(df1), len(df2))
    min_books = min(len(df1), len(df2))
    
    if max_books == 0:
        return 0
    
    # Book overlap score (40% of total)
    book_overlap_score = (common_books_count / max_books) * 40
    
    # Author overlap score (30% of total)
    all_authors = set(df1["author"].dropna()) | set(df2["author"].dropna())
    author_overlap_score = (len(common_authors) / len(all_authors) if all_authors else 0) * 30
    
    # Genre/shelf overlap score (20% of total)
    all_shelves = set()
    for shelves in df1["user_shelves"].dropna():
        if isinstance(shelves, str):
            all_shelves.update([s.strip() for s in shelves.split(",")])
    for shelves in df2["user_shelves"].dropna():
        if isinstance(shelves, str):
            all_shelves.update([s.strip() for s in shelves.split(",")])
    
    genre_overlap_score = (len(common_shelves) / len(all_shelves) if all_shelves else 0) * 20
    
    # Rating similarity score (10% of total)
    rating_similarity = 10
    try:
        avg_rating1 = df1["user_rating"].dropna().mean()
        avg_rating2 = df2["user_rating"].dropna().mean()
        if not math.isnan(avg_rating1) and not math.isnan(avg_rating2):
            rating_diff = abs(avg_rating1 - avg_rating2)
            rating_similarity = max(0, (5 - rating_diff) / 5) * 10
    except:
        pass
    
    # Calculate final score
    blend_score = min(100, book_overlap_score + author_overlap_score + genre_overlap_score + rating_similarity)
    
    return round(blend_score, 1)

def generate_reading_insights(metrics):
    """
    Generate human-readable insights based on the calculated metrics
    
    Args:
        metrics: Dictionary of calculated metrics
        
    Returns:
        list: List of insight strings
    """
    insights = []
    
    # Who reads more
    if metrics["user1_book_count"] > metrics["user2_book_count"]:
        diff = metrics["user1_book_count"] - metrics["user2_book_count"]
        insights.append(f"User 1 has read {diff} more books than User 2")
    elif metrics["user2_book_count"] > metrics["user1_book_count"]:
        diff = metrics["user2_book_count"] - metrics["user1_book_count"]
        insights.append(f"User 2 has read {diff} more books than User 1")
    else:
        insights.append("Both users have read the same number of books")
    
    # Page count comparison
    if metrics["user1_pages_read"] and metrics["user2_pages_read"]:
        if metrics["user1_pages_read"] > metrics["user2_pages_read"]:
            insights.append(f"User 1 has read {round(metrics['user1_pages_read'] - metrics['user2_pages_read'])} more pages than User 2")
        elif metrics["user2_pages_read"] > metrics["user1_pages_read"]:
            insights.append(f"User 2 has read {round(metrics['user2_pages_read'] - metrics['user1_pages_read'])} more pages than User 1")
    
    # Reading speed comparison
    if metrics["user1_avg_reading_days"] and metrics["user2_avg_reading_days"]:
        if metrics["user1_avg_reading_days"] < metrics["user2_avg_reading_days"]:
            insights.append("User 1 tends to finish books faster than User 2")
        elif metrics["user2_avg_reading_days"] < metrics["user1_avg_reading_days"]:
            insights.append("User 2 tends to finish books faster than User 1")
    
    # Book era preferences
    if "user1_avg_pub_year" in metrics and "user2_avg_pub_year" in metrics:
        if metrics["user1_avg_pub_year"] and metrics["user2_avg_pub_year"]:
            diff = abs(metrics["user1_avg_pub_year"] - metrics["user2_avg_pub_year"])
            if diff > 10:
                older_user = "User 1" if metrics["user1_avg_pub_year"] < metrics["user2_avg_pub_year"] else "User 2"
                insights.append(f"{older_user} tends to read older books (avg. {round(diff)} years difference)")
    
    # Blend score insights
    if metrics["blend_score"] >= 80:
        insights.append("Your reading tastes are highly compatible!")
    elif metrics["blend_score"] >= 60:
        insights.append("You have good reading compatibility")
    elif metrics["blend_score"] >= 40:
        insights.append("You have moderate reading compatibility")
    else:
        insights.append("Your reading tastes are quite different")
    
    # Common authors
    if metrics["common_authors_count"] > 0:
        insights.append(f"You both enjoy {metrics['common_authors_count']} of the same authors")
    
    return insights

def find_common_books(user1_books, user2_books):
    """
    Find books that both users have read
    
    Args:
        user1_books: List of books for user 1
        user2_books: List of books for user 2
        
    Returns:
        list: List of common books with both users' ratings
    """
    # Create dictionaries for faster lookup
    user1_dict = {book["book_id"]: book for book in user1_books if "book_id" in book}
    user2_dict = {book["book_id"]: book for book in user2_books if "book_id" in book}
    
    # Find common book IDs
    common_ids = set(user1_dict.keys()) & set(user2_dict.keys())
    
    # Build common books list with both users' ratings
    common_books = []
    for book_id in common_ids:
        book1 = user1_dict[book_id]
        book2 = user2_dict[book_id]
        
        common_book = {
            "book_id": book_id,
            "title": book1["title"],
            "author": book1["author"],
            "link": book1["link"],
            "image": book1["image_medium"],
            "user1_rating": book1.get("user_rating"),
            "user2_rating": book2.get("user_rating"),
            "average_rating": book1["average_rating"],
            "publication_year": book1.get("book_published")
        }
        common_books.append(common_book)
    
    return common_books

if __name__ == "__main__":
    user_id1 = "42944663"
    user_id2 = "48799880"
    combined_results = fetch_two_users_books(user_id1, user_id2)
    print(combined_results)