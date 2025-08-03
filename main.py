import math
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from util.rss_feed_books import fetch_users_books
from util.user_info import get_goodreads_user_info
from util.blend import blend_two_users
from docs.generate_docs import generate_openapi_schema  # centralized in docs/

app = FastAPI(
    title="BookBlend API",
    version="1.0.0",
    description="API Docs for bookblend.io"
)


def sanitize(value):
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


@app.get(
    "/books",
    summary="Get Books",
    description="Returns a list of books for a specific Goodreads user and shelf."
)
def get_books(
    user_id: str = Query(..., description="The Goodreads user ID."),
    shelf: str = Query("all", description="The Goodreads shelf name (e.g., 'read', 'to-read').")
) -> JSONResponse:
    try:
        raw_data = fetch_users_books(user_id=user_id, shelf=shelf, return_type='json')
        sanitized = [{k: sanitize(v) for k, v in row.items()} for row in raw_data]
        return JSONResponse(content=sanitized)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get(
    "/user",
    summary="Get User Info",
    description="Fetches metadata about a Goodreads user, including name and profile information."
)
def get_user_info(
    user_id: str = Query(..., description="The Goodreads user ID.")
) -> JSONResponse:
    try:
        raw_data = get_goodreads_user_info(user_id=user_id)
        return JSONResponse(content=raw_data)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get(
    "/blend",
    summary="Get Blend",
    description="Generates shared book insights between two Goodreads users."
)
def get_blend(
    user_id1: str = Query(..., description="The Goodreads user ID for the first user."),
    user_id2: str = Query(..., description="The Goodreads user ID for the second user.")
) -> JSONResponse:
    try:
        raw_data = blend_two_users(user_id1=user_id1, user_id2=user_id2, shelf='all', include_books=False)

        def sanitize_nested(obj):
            if isinstance(obj, dict):
                return {k: sanitize_nested(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [sanitize_nested(item) for item in obj]
            return sanitize(obj)

        sanitized = sanitize_nested(raw_data)
        return JSONResponse(content=sanitized)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


if __name__ == "__main__":
    generate_openapi_schema(app)
