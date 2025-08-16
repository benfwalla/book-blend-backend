import concurrent.futures
import pandas as pd
import numpy as np
from util.rss_feed_books import fetch_users_books
from util.user_info import get_goodreads_user_info
from util.ai_insights import get_ai_insights

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

def filter_shelf_books(df, shelf_name):
    """Filter dataframe to only include books on a specific shelf"""
    return df[df["user_shelves"] == shelf_name]

def calculate_basic_metrics(df1, df2, df1_read, df2_read, df1_to_read, df2_to_read, df1_currently_reading, df2_currently_reading):
    """Calculate basic count metrics for both users"""
    metrics = {}
    # Basic count metrics
    metrics["user1_total_book_count"] = int(len(df1))
    metrics["user2_total_book_count"] = int(len(df2))
    metrics["user1_read_count"] = int(len(df1_read))
    metrics["user2_read_count"] = int(len(df2_read))
    metrics["user1_to_read_count"] = int(len(df1_to_read))
    metrics["user2_to_read_count"] = int(len(df2_to_read))
    metrics["user1_currently_reading_count"] = int(len(df1_currently_reading))
    metrics["user2_currently_reading_count"] = int(len(df2_currently_reading))
    
    # Calculate total pages read (ignoring None values) - use read shelf
    metrics["user1_pages_read"] = float(df1_read["num_pages"].dropna().sum())
    metrics["user2_pages_read"] = float(df2_read["num_pages"].dropna().sum())
    
    return metrics

def calculate_page_metrics(df1_read, df2_read):
    """Calculate metrics related to book page counts"""
    metrics = {}
    # Average page length - use read shelf
    metrics["user1_avg_page_length"] = round(float(df1_read["num_pages"].dropna().mean()), 2) if not df1_read["num_pages"].dropna().empty else None
    metrics["user2_avg_page_length"] = round(float(df2_read["num_pages"].dropna().mean()), 2) if not df2_read["num_pages"].dropna().empty else None
    
    # Median page length - use read shelf
    metrics["user1_median_page_length"] = float(df1_read["num_pages"].dropna().median()) if not df1_read["num_pages"].dropna().empty else None
    metrics["user2_median_page_length"] = float(df2_read["num_pages"].dropna().median()) if not df2_read["num_pages"].dropna().empty else None
    
    return metrics

def calculate_rating_metrics(df1, df2):
    """Calculate metrics related to book ratings"""
    metrics = {}
    # Count of non-zero ratings
    metrics["user1_ratings_given"] = int(len(df1[df1["user_rating"] > 0]))
    metrics["user2_ratings_given"] = int(len(df2[df2["user_rating"] > 0]))
    
    # Average user ratings (ignoring zero ratings)
    user1_ratings = df1[df1["user_rating"] > 0]["user_rating"]
    user2_ratings = df2[df2["user_rating"] > 0]["user_rating"]
    metrics["user1_avg_rating"] = round(float(user1_ratings.mean()), 2) if not user1_ratings.empty else None
    metrics["user2_avg_rating"] = round(float(user2_ratings.mean()), 2) if not user2_ratings.empty else None
    
    return metrics

def calculate_book_overlap_metrics(df1, df2, df1_read, df2_read):
    """Calculate metrics related to book overlap between users"""
    metrics = {}
    # Book overlap - use all shelves
    user1_book_ids = set(df1["book_id"].dropna())
    user2_book_ids = set(df2["book_id"].dropna())
    common_book_ids = user1_book_ids.intersection(user2_book_ids)
    metrics["common_books_count"] = len(common_book_ids)
    
    # Count common books that are on the "read" shelf for both users
    user1_read_ids = set(df1_read["book_id"].dropna())
    user2_read_ids = set(df2_read["book_id"].dropna())
    common_read_books_count = len(user1_read_ids.intersection(user2_read_ids))
    metrics["common_read_books_count"] = common_read_books_count
    
    return metrics

def calculate_publication_year_metrics(df1_read, df2_read):
    """Calculate metrics related to book publication years"""
    metrics = {}
    # Publication year preferences - use read shelf
    metrics["user1_avg_pub_year"] = int(df1_read["book_published"].dropna().mean()) if not df1_read["book_published"].dropna().empty else None
    metrics["user2_avg_pub_year"] = int(df2_read["book_published"].dropna().mean()) if not df2_read["book_published"].dropna().empty else None
    
    return metrics

def get_oldest_book_details(df_read):
    """Get details about the oldest book a user has read"""
    if not df_read["book_published"].dropna().empty:
        oldest_book_year = int(df_read["book_published"].dropna().min())
        oldest_book = df_read[df_read["book_published"] == oldest_book_year].iloc[0]
        return {
            "year": oldest_book_year,
            "details": {
                "title": oldest_book["title"],
                "author": oldest_book["author"],
                "year": int(oldest_book["book_published"]),
                "book_id": oldest_book["book_id"],
                "image": oldest_book.get("image_medium", "")
            }
        }
    return {"year": None, "details": None}

def get_longest_book_details(df_read):
    """Get details about the longest book a user has read"""
    if not df_read["num_pages"].dropna().empty:
        longest_book_pages = int(df_read["num_pages"].dropna().max())
        longest_book = df_read[df_read["num_pages"] == longest_book_pages].iloc[0]
        return {
            "details": {
                "title": longest_book["title"],
                "author": longest_book["author"],
                "pages": int(longest_book["num_pages"]),
                "book_id": longest_book["book_id"],
                "image": longest_book.get("image_medium", "")
            }
        }
    return {"details": None}

def calculate_era_metrics(df1_read, df2_read):
    """Calculate metrics related to book publication eras"""
    # Publication era distribution
    era_ranges = {
        "pre_1900": (0, 1900),
        "1900_1950": (1900, 1950),
        "1950_1980": (1950, 1980),
        "1980_2000": (1980, 2000),
        "2000_2010": (2000, 2010),
        "2010_present": (2010, 3000)  # Using 3000 as an upper bound
    }
    
    # Calculate publication era distributions for both users
    user1_era_dist = {era: 0 for era in era_ranges}
    user2_era_dist = {era: 0 for era in era_ranges}
    
    for _, book in df1_read.iterrows():
        if pd.notna(book["book_published"]):
            year = int(book["book_published"])
            for era, (start, end) in era_ranges.items():
                if start <= year < end:
                    user1_era_dist[era] += 1
                    break
    
    for _, book in df2_read.iterrows():
        if pd.notna(book["book_published"]):
            year = int(book["book_published"])
            for era, (start, end) in era_ranges.items():
                if start <= year < end:
                    user2_era_dist[era] += 1
                    break
    
    # Convert to percentages
    total_books1 = sum(user1_era_dist.values())
    total_books2 = sum(user2_era_dist.values())
    
    if total_books1 > 0:
        user1_era_pct = {era: round(count / total_books1 * 100, 1) for era, count in user1_era_dist.items()}
    else:
        user1_era_pct = {era: 0 for era in era_ranges}
    
    if total_books2 > 0:
        user2_era_pct = {era: round(count / total_books2 * 100, 1) for era, count in user2_era_dist.items()}
    else:
        user2_era_pct = {era: 0 for era in era_ranges}
    
    # Find dominant era for each user
    user1_dominant_era = max(user1_era_dist.items(), key=lambda x: x[1])[0] if total_books1 > 0 else None
    user2_dominant_era = max(user2_era_dist.items(), key=lambda x: x[1])[0] if total_books2 > 0 else None
    
    # Calculate era overlap (dot product of normalized distributions)
    era_similarity = 0
    if total_books1 > 0 and total_books2 > 0:
        for era in era_ranges:
            era_similarity += (user1_era_dist[era] / total_books1) * (user2_era_dist[era] / total_books2)
        era_similarity = round(era_similarity * 100, 1)  # Convert to percentage
    
    # Return era metrics
    return {
        "user1_era_distribution": user1_era_pct,
        "user2_era_distribution": user2_era_pct,
        "user1_dominant_era": user1_dominant_era,
        "user2_dominant_era": user2_dominant_era,
        "era_similarity": era_similarity
    }

def find_common_authors(df1, df2):
    """Calculate metrics related to author overlap between users"""
    # Find common authors and their books
    user1_authors = set(df1["author"].dropna())
    user2_authors = set(df2["author"].dropna())
    common_authors = user1_authors.intersection(user2_authors)
    
    # Get detailed information about common authors
    common_authors_info = []
    for author in common_authors:
        user1_author_books = df1[df1["author"] == author]
        user2_author_books = df2[df2["author"] == author]
        
        author_info = {
            "author": author,
            "user1_books": [
                {
                    "title": row["title"],
                    "shelves": row["user_shelves"],
                    "book_id": row["book_id"]
                }
                for _, row in user1_author_books.iterrows()
            ],
            "user2_books": [
                {
                    "title": row["title"],
                    "shelves": row["user_shelves"],
                    "book_id": row["book_id"]
                }
                for _, row in user2_author_books.iterrows()
            ]
        }
        common_authors_info.append(author_info)
    
    # Sort by total number of books by this author between both users
    common_authors_info.sort(key=lambda x: len(x["user1_books"]) + len(x["user2_books"]), reverse=True)
    
    return {
        "common_authors_count": len(common_authors),
        "common_authors": common_authors_info
    }

def calculate_blend_metrics(df1, df2):
    """
    Calculate various metrics to quantify the compatibility between two users' reading habits
    
    Args:
        df1 (DataFrame): First user's books
        df2 (DataFrame): Second user's books
        
    Returns:
        dict: Dictionary containing various blend metrics
    """
    # Filter books by shelf
    df1_read = filter_shelf_books(df1, "read")
    df2_read = filter_shelf_books(df2, "read")
    df1_to_read = filter_shelf_books(df1, "to-read")
    df2_to_read = filter_shelf_books(df2, "to-read")
    df1_currently_reading = filter_shelf_books(df1, "currently-reading")
    df2_currently_reading = filter_shelf_books(df2, "currently-reading")
    
    # Initialize metrics dictionary
    metrics = {}
    
    # Calculate different metric categories
    basic_metrics = calculate_basic_metrics(df1, df2, df1_read, df2_read, df1_to_read, df2_to_read, df1_currently_reading, df2_currently_reading)
    page_metrics = calculate_page_metrics(df1_read, df2_read)
    rating_metrics = calculate_rating_metrics(df1, df2)
    book_overlap_metrics = calculate_book_overlap_metrics(df1, df2, df1_read, df2_read)
    publication_year_metrics = calculate_publication_year_metrics(df1_read, df2_read)
    
    # Get book details
    user1_oldest_book = get_oldest_book_details(df1_read)
    user2_oldest_book = get_oldest_book_details(df2_read)
    user1_longest_book = get_longest_book_details(df1_read)
    user2_longest_book = get_longest_book_details(df2_read)
    
    # Calculate era metrics
    era_metrics = calculate_era_metrics(df1_read, df2_read)
    
    # Calculate author overlap
    author_metrics = find_common_authors(df1, df2)
    
    # Combine all metrics
    metrics.update(basic_metrics)
    metrics.update(page_metrics)
    metrics.update(rating_metrics)
    metrics.update(book_overlap_metrics)
    metrics.update(publication_year_metrics)
    metrics.update(era_metrics)
    metrics.update(author_metrics)
    
    # Add book details
    metrics["user1_oldest_book"] = user1_oldest_book["year"]
    metrics["user1_oldest_book_details"] = user1_oldest_book["details"]
    metrics["user2_oldest_book"] = user2_oldest_book["year"]
    metrics["user2_oldest_book_details"] = user2_oldest_book["details"]
    metrics["user1_longest_book_details"] = user1_longest_book["details"]
    metrics["user2_longest_book_details"] = user2_longest_book["details"]
    
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

def compute_blend_score(df1, df2, metrics, ai_insights):
    """Compute a single compatibility score (0-100) from existing metrics plus genre similarity.
    Components: common read books, common authors, shared genres, era similarity, rating proximity,
    median page length proximity, average publication year proximity.
    Returns a dict with the score, components, and weights.
    """
    # Safely fetch needed metrics
    u1_read = metrics.get("user1_read_count") or 0
    u2_read = metrics.get("user2_read_count") or 0
    common_read_books = metrics.get("common_read_books_count") or 0

    # Common books similarity: give partial credit for any-shelf overlap, full credit for read-read
    denom_rr = max(1, min(u1_read, u2_read))
    sim_rr = min(1.0, common_read_books / denom_rr) if denom_rr > 0 else 0.0
    any_common = metrics.get("common_books_count") or 0
    u1_total = metrics.get("user1_total_book_count") or 0
    u2_total = metrics.get("user2_total_book_count") or 0
    denom_any = max(1, min(u1_total, u2_total))
    sim_any = min(1.0, any_common / denom_any) if denom_any > 0 else 0.0
    # Blend: prioritize read-read, but keep 30% weight for any overlap
    sim_common_books = 0.7 * sim_rr + 0.3 * sim_any

    # Common authors similarity (over union of authors)
    u1_authors = set(df1["author"].dropna())
    u2_authors = set(df2["author"].dropna())
    union_authors = len(u1_authors.union(u2_authors)) or 1
    overlap_authors = metrics.get("common_authors_count") or 0
    sim_common_authors = min(1.0, overlap_authors / union_authors)

    # Genre similarity from ai_insights (already normalized to taxonomy)
    gi = (ai_insights or {}).get("genre_insights", {}) or {}
    u1_genres = gi.get("user1_preferences", []) or []
    u2_genres = gi.get("user2_preferences", []) or []
    shared_genres = gi.get("shared_genres", []) or []
    denom_genres = max(1, min(len(u1_genres), len(u2_genres)))
    sim_genre = min(1.0, len(shared_genres) / denom_genres) if denom_genres > 0 else 0.0

    # Era similarity already in [0,100]
    sim_era = (metrics.get("era_similarity") or 0) / 100.0

    # Rating proximity similarity (normalize diff on 0..4 scale since Goodreads uses 1..5)
    u1_avg_rating = metrics.get("user1_avg_rating")
    u2_avg_rating = metrics.get("user2_avg_rating")
    if u1_avg_rating is not None and u2_avg_rating is not None:
        rating_diff = abs(u1_avg_rating - u2_avg_rating)
        sim_rating = max(0.0, 1.0 - (rating_diff / 4.0))
    else:
        sim_rating = 0.5  # neutral if insufficient data

    # Median page length similarity (cap differences at 400 pages)
    u1_med_pages = metrics.get("user1_median_page_length")
    u2_med_pages = metrics.get("user2_median_page_length")
    if u1_med_pages is not None and u2_med_pages is not None:
        pages_diff = abs(u1_med_pages - u2_med_pages)
        sim_length = max(0.0, 1.0 - min(1.0, pages_diff / 400.0))
    else:
        sim_length = 0.5

    # Publication year preference similarity (cap differences at 50 years)
    u1_avg_year = metrics.get("user1_avg_pub_year")
    u2_avg_year = metrics.get("user2_avg_pub_year")
    if u1_avg_year is not None and u2_avg_year is not None:
        year_diff = abs(u1_avg_year - u2_avg_year)
        sim_year = max(0.0, 1.0 - min(1.0, year_diff / 50.0))
    else:
        sim_year = 0.5

    # Weights (sum to 1.0). Emphasize overlap and genres as primary signals.
    weights = {
        "common_books": 0.25,
        "common_authors": 0.10,
        "genres": 0.25,
        "era": 0.15,
        "rating": 0.10,
        "length": 0.10,
        "year": 0.05,
    }

    score = (
        sim_common_books * weights["common_books"] +
        sim_common_authors * weights["common_authors"] +
        sim_genre * weights["genres"] +
        sim_era * weights["era"] +
        sim_rating * weights["rating"] +
        sim_length * weights["length"] +
        sim_year * weights["year"]
    ) * 100.0

    return {
        "score": round(float(score), 1),
        "components": {
            "common_books": round(sim_common_books, 3),
            "common_authors": round(sim_common_authors, 3),
            "genres": round(sim_genre, 3),
            "era": round(sim_era, 3),
            "rating": round(sim_rating, 3),
            "length": round(sim_length, 3),
            "year": round(sim_year, 3),
        },
        "weights": weights,
    }

def blend_two_users(user_id1, user_id2, shelf="all", include_books=False):
    """
    Fetch Goodreads shelves for two users in parallel and return a combined JSON output
    with compatibility metrics and reading insights

    Args:
        user_id1 (str): First Goodreads user ID
        user_id2 (str): Second Goodreads user ID
        shelf (str): Which shelf to fetch, defaults to "all"
        include_books (bool): Whether to include all books in the response, defaults to False

    Returns:
        dict: Combined JSON with results separated by user IDs and blend metrics
    """
    
    # Fetch user info in parallel with book data
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Submit user info tasks
        future_user1_info = executor.submit(get_goodreads_user_info, user_id1)
        future_user2_info = executor.submit(get_goodreads_user_info, user_id2)
        
        # Submit book data tasks
        future_user1_books = executor.submit(fetch_users_books, user_id1, shelf=shelf, return_type="json")
        future_user2_books = executor.submit(fetch_users_books, user_id2, shelf=shelf, return_type="json")
        
        # Get results
        user1_info = future_user1_info.result()["user"]
        user2_info = future_user2_info.result()["user"]
        user1_books = future_user1_books.result()
        user2_books = future_user2_books.result()

    # Convert to DataFrames for easier analysis
    df1 = pd.DataFrame(user1_books)
    df2 = pd.DataFrame(user2_books)

    # Calculate metrics
    blend_metrics = calculate_blend_metrics(df1, df2)

    # Find common books
    common_books = find_common_books(user1_books, user2_books)

    # Extract user-specific metrics
    user1_metrics = {}
    user2_metrics = {}
    common_metrics = {}
    
    for key, value in blend_metrics.items():
        if key.startswith("user1_"):
            user1_metrics[key.replace("user1_", "")] = value
        elif key.startswith("user2_"):
            user2_metrics[key.replace("user2_", "")] = value
        else:
            common_metrics[key] = value
    
    # Combine results into a single JSON output with restructured format
    combined_results = {
        "users": {
            user_id1: {
                "id": user_id1,
                "name": user1_info["name"],
                "profile_url": user1_info["profile_url"],
                "image_url": user1_info["image_url"],
                "metrics": user1_metrics
            },
            user_id2: {
                "id": user_id2,
                "name": user2_info["name"],
                "profile_url": user2_info["profile_url"],
                "image_url": user2_info["image_url"],
                "metrics": user2_metrics
            }
        },
        "common_books": common_books,
        "common_authors": blend_metrics.get("common_authors", [])
    }
    
    # Add common metrics
    combined_results.update(common_metrics)
    
    # Assess data quality and optionally skip AI insights if data is very sparse for both users
    u1_total = blend_metrics.get("user1_total_book_count") or 0
    u2_total = blend_metrics.get("user2_total_book_count") or 0
    u1_read_count = blend_metrics.get("user1_read_count") or 0
    u2_read_count = blend_metrics.get("user2_read_count") or 0

    def _status(total, read):
        if total < 5 or read < 3:
            return "low"
        if total < 10 or read < 5:
            return "moderate"
        return "ok"

    u1_status = _status(u1_total, u1_read_count)
    u2_status = _status(u2_total, u2_read_count)
    preliminary = u1_status != "ok" or u2_status != "ok"

    # If both users are very sparse, skip AI to save latency/cost
    skip_ai = (u1_status == "low" and u2_status == "low")

    if skip_ai:
        ai_insights = {"skipped": True, "reason": "insufficient_data"}
    else:
        # Generate AI insights based on books and metrics (used by blend score for genres)
        ai_insights = get_ai_insights(
            user1_books=user1_books,
            user2_books=user2_books,
            user1_name=user1_info["name"],
            user2_name=user2_info["name"],
            blend_metrics=blend_metrics
        )
    
    # Compute and attach final blend score (uses full dataframes, metrics, and ai_insights genres)
    combined_results["blend"] = compute_blend_score(df1, df2, blend_metrics, ai_insights if not skip_ai else {})
    
    # If data is sparse for either user, mark blend as preliminary with a short note
    if preliminary:
        combined_results["blend"]["preliminary"] = True
        combined_results["blend"]["note"] = "Limited data for one or both users; score may be less stable."
    
    # Only include all books if requested
    if include_books:
        combined_results["users"][user_id1]["books"] = user1_books
        combined_results["users"][user_id2]["books"] = user2_books

    # Convert NumPy types to Python native types for JSON serialization
    combined_results = _make_json_serializable(combined_results)

    return combined_results

if __name__ == "__main__":
    user_id1 = "42944663"
    user_id2 = "48799880"
    combined_results = blend_two_users(user_id1, user_id2)
    print(combined_results)