import concurrent.futures
from util.rss_feed_books import fetch_users_books

def fetch_two_users_books(user_id1, user_id2, shelf="all"):
    """
    Fetch Goodreads shelves for two users in parallel and return a combined JSON output
    
    Args:
        user_id1 (str): First Goodreads user ID
        user_id2 (str): Second Goodreads user ID
        shelf (str): Which shelf to fetch, defaults to "all"
        
    Returns:
        dict: Combined JSON with results separated by user IDs
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
    
    # Combine results into a single JSON output
    combined_results = {
        "results": [result1, result2]
    }
    
    return combined_results

if __name__ == "__main__":
    user_id1 = "42944663"
    user_id2 = "48799880"
    combined_results = fetch_two_users_books(user_id1, user_id2)
    print(combined_results)