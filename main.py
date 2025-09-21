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
    description="Returns a list of books for a specific Goodreads user and shelf.",
    responses={
        200: {
            "description": "A list of books for the user/shelf",
            "content": {
                "application/json": {
                    "examples": {
                        "books_example": {
                            "summary": "Example return for /books",
                            "value": [
                                {
                                    "title": "The Gunslinger (The Dark Tower, #1)",
                                    "author": "Stephen King",
                                    "user_shelves": "currently-reading",
                                    "link": "https://www.goodreads.com/book/show/59359648",
                                    "isbn": "",
                                    "average_rating": 3.9,
                                    "user_rating": 0,
                                    "user_review": "",
                                    "read_at": None,
                                    "date_added": "2025-07-09",
                                    "book_id": "59359648",
                                    "num_pages": 287,
                                    "book_published": 1982.0,
                                    "image_small": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1656505743l/59359648._SY75_.jpg",
                                    "image_medium": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1656505743l/59359648._SX98_.jpg",
                                    "image_large": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1656505743l/59359648._SY475_.jpg"
                                },
                                {
                                    "title": "Espresso (Zion Sawyer #3)",
                                    "author": "M.L. Hamilton",
                                    "user_shelves": "currently-reading",
                                    "link": "https://www.goodreads.com/book/show/36507000",
                                    "isbn": "",
                                    "average_rating": 4.55,
                                    "user_rating": 0,
                                    "user_review": "",
                                    "read_at": None,
                                    "date_added": "2025-06-11",
                                    "book_id": "36507000",
                                    "num_pages": 305,
                                    "book_published": None,
                                    "image_small": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1509418927l/36507000._SX50_.jpg",
                                    "image_medium": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1509418927l/36507000._SX98_.jpg",
                                    "image_large": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1509418927l/36507000._SX318_.jpg"
                                }
                            ]
                        }
                    }
                }
            }
        }
    }
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
    description="Fetches metadata about a Goodreads user, including name and profile information. Accepts either user_id or username.",
    responses={
        200: {
            "description": "User info with friends list",
            "content": {
                "application/json": {
                    "examples": {
                        "user_example": {
                            "summary": "Example return for /user",
                            "value": {
                                "user": {
                                    "name": "Ben Wallace",
                                    "image_url": "https://images.gr-assets.com/users/1584022699p5/42944663.jpg",
                                    "id": "42944663",
                                    "profile_url": "https://www.goodreads.com/user/show/42944663",
                                    "book_count": "142"
                                },
                                "friends": [
                                    {
                                        "name": "Brenna Stubenbort",
                                        "image_url": "https://images.gr-assets.com/users/1428068652p2/24874938.jpg",
                                        "id": "24874938",
                                        "profile_url": "https://www.goodreads.com/user/show/24874938-brenna-stubenbort",
                                        "book_count": "277"
                                    },
                                    {
                                        "name": "Clayton Marshall",
                                        "image_url": "https://images.gr-assets.com/users/1679267847p2/93322377.jpg",
                                        "id": "93322377",
                                        "profile_url": "https://www.goodreads.com/user/show/93322377-clayton-marshall",
                                        "book_count": "27"
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
)
def get_user_info(
    user_id: str = Query(None, description="The Goodreads user ID."),
    username: str = Query(None, description="The Goodreads username.")
) -> JSONResponse:
    try:
        if not user_id and not username:
            return JSONResponse(status_code=400, content={"error": "Either user_id or username must be provided"})
        if user_id and username:
            return JSONResponse(status_code=400, content={"error": "Provide either user_id or username, not both"})
        
        raw_data = get_goodreads_user_info(user_id=user_id, username=username)
        return JSONResponse(content=raw_data)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get(
    "/blend",
    summary="Get Blend",
    description="Generates shared book insights between two Goodreads users.",
    responses={
        200: {
            "description": "Blend response including users, common items, components, and score",
            "content": {
                "application/json": {
                    "examples": {
                        "with_ai_insights": {
                            "summary": "Example with AI insights included",
                            "value": {"users": {"42944663": {"id": "42944663", "name": "Ben Wallace", "profile_url": "https://www.goodreads.com/user/show/42944663", "image_url": "https://images.gr-assets.com/users/1584022699p5/42944663.jpg", "metrics": {"total_book_count": 202, "read_count": 96, "to_read_count": 104, "currently_reading_count": 2, "pages_read": 23964.0, "avg_page_length": 266.27, "median_page_length": 263.0, "ratings_given": 15, "avg_rating": 4.73, "avg_pub_year": 1988, "era_distribution": {"pre_1900": 4.3, "1900_1950": 14.1, "1950_1980": 10.9, "1980_2000": 13.0, "2000_2010": 13.0, "2010_present": 44.6}, "dominant_era": "2010_present", "oldest_book": 1891, "oldest_book_details": {"title": "A Scandal in Bohemia (The Adventures of Sherlock Holmes, #1)", "author": "Ronald Holt", "year": 1891, "book_id": "1848444", "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1189170223l/1848444._SX98_.jpg"}, "longest_book_details": {"title": "A People’s History of the United States: 1492 - Present", "author": "Howard Zinn", "pages": 729, "book_id": "2767", "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1494279423l/2767._SX98_.jpg"}}}, "93322377": {"id": "93322377", "name": "Clayton Marshall", "profile_url": "https://www.goodreads.com/user/show/93322377", "image_url": "https://images.gr-assets.com/users/1679267847p5/93322377.jpg", "metrics": {"total_book_count": 27, "read_count": 18, "to_read_count": 9, "currently_reading_count": 0, "pages_read": 5503.0, "avg_page_length": 305.72, "median_page_length": 303.5, "ratings_given": 9, "avg_rating": 4.22, "avg_pub_year": 1994, "era_distribution": {"pre_1900": 0.0, "1900_1950": 16.7, "1950_1980": 22.2, "1980_2000": 0.0, "2000_2010": 16.7, "2010_present": 44.4}, "dominant_era": "2010_present", "oldest_book": 1936, "oldest_book_details": {"title": "How to Win Friends & Influence People", "author": "Dale Carnegie", "year": 1936, "book_id": "4865", "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1442726934l/4865._SX98_.jpg"}, "longest_book_details": {"title": "The Shining (The Shining, #1)", "author": "Stephen King", "pages": 497, "book_id": "11588", "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1353277730l/11588._SX98_.jpg"}}}}, "common_books": [{"book_id": "386162", "title": "The Hitchhiker’s Guide to the Galaxy (The Hitchhiker's Guide to the Galaxy, #1)", "author": "Douglas Adams", "link": "https://www.goodreads.com/book/show/386162", "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1559986152l/386162._SX318_SY475_.jpg", "user1_rating": 0, "user2_rating": 4, "user1_review": "", "user2_review": "", "user1_shelves": "read", "user2_shelves": "read", "average_rating": 4.28, "publication_year": 1979.0}], "common_authors": [{"author": "Stefan Zweig", "user1_books": [{"title": "Twenty-Four Hours in the Life of a Woman", "shelves": "read", "book_id": "182163"}], "user2_books": [{"title": "Chess Story", "shelves": "read", "book_id": "59151"}]}], "common_books_count": 10, "common_read_books_count": 9, "era_similarity": 26.8, "common_authors_count": 8, "ai_insights": {"genre_insights": {"user1_preferences": ["Science Fiction", "Non-Fiction", "Historical Fiction", "Literary Fiction", "Biography"], "user2_preferences": ["Non-Fiction", "Literary Fiction", "Science Fiction", "History", "Biography"], "shared_genres": ["Biography", "Literary Fiction", "Non-Fiction", "Science Fiction"], "recommendations": ["Consider exploring Historical Fiction together", "Both might enjoy Science books"]}, "fiction_nonfiction": {"user1_ratio": 0.75, "user2_ratio": 0.25, "compatibility": "User1 strongly prefers fiction while User2 favors non-fiction. This creates an opportunity to expand each other's horizons."}, "reading_style": {"user1_summary": "Tends to read contemporary bestsellers with high ratings", "user2_summary": "Favors classics and literary fiction with diverse themes", "compatibility_score": 0.65, "compatibility_details": "While their genre preferences differ, both show appreciation for well-crafted narratives and character-driven stories."}, "book_recommendations": {"for_both": ["The Three-Body Problem by Liu Cixin", "An Absolutely Remarkable Thing by Hank Green"], "for_user1": ["How the Word Is Passed by Clint Smith", "The Disordered Cosmos by Chanda Prescod-Weinstein"], "for_user2": ["The Metamorphosis of Prime Intellect by Roger Williams", "The Common Good by Robert B. Reich"]}, "users": {"user1": "Ben Wallace", "user2": "Clayton Marshall"}}, "blend": {"score": 85.7, "score_raw": 58.1, "components": {"common_books": 0.461, "common_authors": 0.043, "genres": 0.8, "era": 0.268, "rating": 0.872, "length": 0.899, "year": 0.88}, "weights": {"common_books": 0.25, "common_authors": 0.1, "genres": 0.25, "era": 0.15, "rating": 0.1, "length": 0.1, "year": 0.05}}}
                        },
                        "sparse_data": {
                            "summary": "Sparse data example (AI insights skipped)",
                            "value": {
                                "users": {
                                    "170982409": {
                                        "id": "170982409",
                                        "name": "Donald Bough",
                                        "profile_url": "https://www.goodreads.com/user/show/170982409",
                                        "image_url": "https://s.gr-assets.com/assets/nophoto/user/u_200x266-e183445fd1a1b5cc7075bb1cf7043306.png",
                                        "metrics": {
                                            "total_book_count": 8,
                                            "read_count": 4,
                                            "to_read_count": 0,
                                            "currently_reading_count": 4,
                                            "pages_read": 2143.0,
                                            "avg_page_length": 535.75,
                                            "median_page_length": 552.0,
                                            "ratings_given": 4,
                                            "avg_rating": 5.0,
                                            "avg_pub_year": 2005,
                                            "era_distribution": {
                                                "pre_1900": 0.0,
                                                "1900_1950": 0.0,
                                                "1950_1980": 25.0,
                                                "1980_2000": 0.0,
                                                "2000_2010": 25.0,
                                                "2010_present": 50.0
                                            },
                                            "dominant_era": "2010_present",
                                            "oldest_book": 1977,
                                            "oldest_book_details": {
                                                "title": "The Shining (The Shining, #1)",
                                                "author": "Stephen King",
                                                "year": 1977,
                                                "book_id": "220505376",
                                                "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1729172989l/220505376._SX98_.jpg"
                                            },
                                            "longest_book_details": {
                                                "title": "The Shining (The Shining, #1)",
                                                "author": "Stephen King",
                                                "pages": 673,
                                                "book_id": "220505376",
                                                "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1729172989l/220505376._SX98_.jpg"
                                            }
                                        }
                                    }
                                },
                                "common_books": [
                                    {
                                        "book_id": "53504553",
                                        "title": "Science and Development of Muscle Hypertrophy",
                                        "author": "Brad Schoenfeld",
                                        "link": "https://www.goodreads.com/book/show/53504553",
                                        "image": "https://i.gr-assets.com/images/S/compressed.photo.goodreads.com/books/1590611337l/53504553._SX318_.jpg",
                                        "user1_rating": 0,
                                        "user2_rating": 0,
                                        "user1_review": "",
                                        "user2_review": "",
                                        "user1_shelves": "currently-reading",
                                        "user2_shelves": "currently-reading",
                                        "average_rating": 4.64,
                                        "publication_year": 2016.0
                                    }
                                ],
                                "common_authors": [
                                    {
                                        "author": "Stephen King",
                                        "user1_books": [
                                            {"title": "The Gunslinger (The Dark Tower, #1)", "shelves": "currently-reading", "book_id": "59359648"}
                                        ],
                                        "user2_books": [
                                            {"title": "The Gunslinger (The Dark Tower, #1)", "shelves": "currently-reading", "book_id": "59359648"}
                                        ]
                                    }
                                ],
                                "common_books_count": 8,
                                "common_read_books_count": 4,
                                "era_similarity": 37.5,
                                "common_authors_count": 6,
                                "ai_insights": {
                                    "skip": True,
                                    "reason": "Insufficient data for reliable insights."
                                },
                                "blend": {
                                    "score": 100.0,
                                    "score_raw": 90.6,
                                    "components": {
                                        "common_books": 1.0,
                                        "common_authors": 1.0,
                                        "genres": 1.0,
                                        "era": 0.375,
                                        "rating": 1.0,
                                        "length": 1.0,
                                        "year": 1.0
                                    },
                                    "weights": {
                                        "common_books": 0.25,
                                        "common_authors": 0.1,
                                        "genres": 0.25,
                                        "era": 0.15,
                                        "rating": 0.1,
                                        "length": 0.1,
                                        "year": 0.05
                                    },
                                    "preliminary": True,
                                    "note": "Limited data for one or both users; score may be less stable."
                                }
                            }
                        }
                    }
                }
            }
        }
    }
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
