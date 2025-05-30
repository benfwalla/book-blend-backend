import math
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from goodreads_parser import get_all_goodreads_user_books

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
        raw_data = get_all_goodreads_user_books(user_id=user_id, shelf=shelf, return_format='json')

        # Sanitize dict values
        sanitized = [
            {k: sanitize(v) for k, v in row.items()}
            for row in raw_data
        ]

        return JSONResponse(content=sanitized)

    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
