import math
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from rss_feed_books import fetch_all_goodreads
from friends import get_goodreads_friends

app = FastAPI(title="BookBlend API")

def sanitize(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

@app.get("/")
def read_root():
    return {"message": "📚 Welcome to the BookBlend API!"}

@app.get("/books")
def get_books(user_id: str, shelf: str = "all"):
    try:
        # Get possibly dirty list of dicts
        raw_data = fetch_all_goodreads(user_id=user_id, shelf=shelf, return_type='json')

        # Sanitize dict values
        sanitized = [
            {k: sanitize(v) for k, v in row.items()}
            for row in raw_data
        ]

        return JSONResponse(content=sanitized)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/friends")
def get_friends(user_id: str):
    try:
        # Get list of friends for the user
        raw_data = get_goodreads_friends(user_id=user_id)

        # Sanitize dict values
        sanitized = [
            {k: sanitize(v) for k, v in friend.items()}
            for friend in raw_data
        ]

        return JSONResponse(content=sanitized)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
