import math
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from util.rss_feed_books import fetch_users_books
from util.user_info import get_goodreads_user_info
from util.blend import blend_two_users
from docs.generate_docs import generate_openapi_schema
import json

# Local execution
# uvicorn main:app --reload
app = FastAPI(title="BookBlend API")

def sanitize(obj):
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj

@app.get("/books")
def get_books(user_id: str, shelf: str = "all"):
    """
    Get a list of books for a given Goodreads user and shelf (default is "all")
    :param user_id: The ID of the user on Goodreads
    :param shelf: The name of the Goodreads shelf (i.e. "all", "to-read", "currently-reading", "read")
    :return: A JSON object containing a list of books
    """
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
def get_user_info(user_id: str) -> JSONResponse:
    """
    Get information about a given Goodreads user

    :param user_id: The ID of the user on Goodreads
    :return: A JSON object with information about the user
    """
    try:
        # Get list of friends for the user
        raw_data = get_goodreads_user_info(user_id=user_id)

        return JSONResponse(content=raw_data)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/blend")
def get_blend(user_id1: str, user_id2: str):
    """
    Gets blend data between two given Goodreads users
    :param user_id1: The ID of the first user
    :param user_id2: The ID of the second user
    :return: A JSON object with blend data
    """
    try:
        # Get enhanced blend data with metrics
        raw_data = blend_two_users(user_id1=user_id1, user_id2=user_id2, shelf='all', include_books=False)

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

if __name__ == "__main__":

    generate_openapi_schema(app)