from fastapi import FastAPI
from fastapi.responses import JSONResponse
from util.rss_feed_books import fetch_all_goodreads
from util.user_info import get_goodreads_user_info
from mangum import Mangum
import math

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
        raw_data = fetch_all_goodreads(user_id=user_id, shelf=shelf, return_type='json')
        sanitized = [{k: sanitize(v) for k, v in row.items()} for row in raw_data]
        return JSONResponse(content=sanitized)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/user")
def get_friends(user_id: str):
    try:
        raw_data = get_goodreads_user_info(user_id=user_id)
        return JSONResponse(content=raw_data)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

# This is the magic that wraps your ASGI app to work on Vercel
handler = Mangum(app)
