import math
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from util.rss_feed_books import fetch_users_books
from util.user_info import get_goodreads_user_info
from util.blend import blend_two_users

# Local execution
# uvicorn main:app --reload
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
        raw_data = fetch_users_books(user_id=user_id, shelf=shelf, return_type='json')

        # Sanitize dict values
        sanitized = [
            {k: sanitize(v) for k, v in row.items()}
            for row in raw_data
        ]

        return JSONResponse(content=sanitized)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/user")
def get_friends(user_id: str):
    try:
        # Get list of friends for the user
        raw_data = get_goodreads_user_info(user_id=user_id)

        return JSONResponse(content=raw_data)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/blend")
def get_blend(user_id1: str, user_id2: str):
    try:
        # Get enhanced blend data with metrics
        raw_data = blend_two_users(user_id1=user_id1, user_id2=user_id2, shelf='all')

        # Sanitize the data structure recursively
        def sanitize_nested(obj):
            if isinstance(obj, dict):
                return {k: sanitize_nested(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [sanitize_nested(item) for item in obj]
            else:
                return sanitize(obj)
                
        sanitized_data = sanitize_nested(raw_data)
        
        return JSONResponse(content=sanitized_data)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})