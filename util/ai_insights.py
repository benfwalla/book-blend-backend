import os
import dotenv
import json
from typing import Dict, List, Any, Optional
from openai import OpenAI

dotenv.load_dotenv()

# Initialize OpenAI client
client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))

# Define the structure for the AI insights response
INSIGHT_STRUCTURE = {
    "genre_insights": {
        "user1_preferences": [],  # List of top genres
        "user2_preferences": [],  # List of top genres
        "shared_genres": [],      # List of genres both users enjoy
        "recommendations": []     # Genre-based recommendations
    },
    "fiction_nonfiction": {
        "user1_ratio": 0.0,       # Fiction to non-fiction ratio (0.0 = all non-fiction, 1.0 = all fiction)
        "user2_ratio": 0.0,
        "compatibility": "",      # Text describing compatibility in fiction/non-fiction preferences
    },
    "reading_style": {
        "user1_summary": "",      # Brief summary of reading style
        "user2_summary": "",
        "compatibility_score": 0.0,  # 0.0-1.0 score
        "compatibility_details": ""   # Detailed analysis
    },
    "book_recommendations": {
        "for_both": [],           # Books both might enjoy
        "for_user1": [],          # Books user1 might enjoy based on user2's library
        "for_user2": []           # Books user2 might enjoy based on user1's library
    }
}

# Canonical genre taxonomy (balanced, compressed, UI-friendly)
# Frontend and scoring will rely on these exact strings.
GENRE_TAXONOMY: List[str] = [
    # Fiction
    "Literary Fiction",
    "Contemporary Fiction",
    "Classics",
    "Historical Fiction",
    "Science Fiction",
    "Fantasy",
    "Mystery",
    "Thriller & Crime",
    "Horror",
    "Romance",
    "Young Adult",
    "Graphic Novels & Comics",
    # Non-Fiction
    "Non-Fiction",
    "Memoir",
    "Biography",
    "History",
    "Philosophy",
    "Psychology",
    "Self-Help",
    "Business",
    "Science",
    "Poetry",
    "Religion & Spirituality",
]

# Alias/normalization mapping (lowercased keys) → canonical taxonomy
GENRE_ALIASES: Dict[str, str] = {
    # Sci-fi variants
    "sci-fi": "Science Fiction",
    "scifi": "Science Fiction",
    "sci fi": "Science Fiction",
    # YA/children
    "ya": "Young Adult",
    "young adult": "Young Adult",
    "children": "Young Adult",
    "children's": "Young Adult",
    # Self-help variants
    "self help": "Self-Help",
    # Biography/memoir
    "bio": "Biography",
    "autobiography": "Memoir",
    # Fiction buckets
    "lit fic": "Literary Fiction",
    "litfic": "Literary Fiction",
    "contemporary": "Contemporary Fiction",
    "fiction": "Contemporary Fiction",
    "classics": "Classics",
    # Crime/thriller/suspense
    "crime": "Thriller & Crime",
    "suspense": "Thriller & Crime",
    "thriller": "Thriller & Crime",
    # Comics/graphic novels/manga
    "comics": "Graphic Novels & Comics",
    "graphic novels": "Graphic Novels & Comics",
    "graphic novel": "Graphic Novels & Comics",
    "manga": "Graphic Novels & Comics",
    # Horror/paranormal
    "paranormal": "Horror",
    # Romance variants
    "chick lit": "Romance",
    # Religion/spiritual
    "christian": "Religion & Spirituality",
    "spirituality": "Religion & Spirituality",
    "religion": "Religion & Spirituality",
    # Nonfiction spelling
    "nonfiction": "Non-Fiction",
    # Humor
    "humor": "Contemporary Fiction",
    "humor and comedy": "Contemporary Fiction",
    "comedy": "Contemporary Fiction",
    # Topic-style non-fiction often appearing on Goodreads
    "travel": "Non-Fiction",
    "sports": "Non-Fiction",
    "music": "Non-Fiction",
    "art": "Non-Fiction",
    "cookbooks": "Non-Fiction",
    # LGBTQ buckets (kept broad)
    "gay and lesbian": "Contemporary Fiction",
}

# Limits to keep responses tight and useful
MAX_USER_GENRES = 8
MAX_SHARED_GENRES = 5
MAX_RECOMMENDATIONS = 4

def prepare_book_data_for_analysis(user1_books: List[Dict], user2_books: List[Dict]) -> Dict:
    """
    Extract and structure relevant book data for LLM analysis.
    
    Args:
        user1_books: List of books from first user
        user2_books: List of books from second user
        
    Returns:
        Dictionary with structured book data
    """
    # Extract just the necessary fields to keep prompt size manageable
    user1_processed = []
    for book in user1_books[:100]:  # Limit to 100 books to avoid context limits
        user1_processed.append({
            "title": book.get("title", "Unknown"),
            "author": book.get("author", "Unknown"),
            "shelves": book.get("shelves", []),
            "user_rating": book.get("user_rating", None),
            "publication_year": book.get("publication_year", None)
        })
    
    user2_processed = []
    for book in user2_books[:100]:
        user2_processed.append({
            "title": book.get("title", "Unknown"),
            "author": book.get("author", "Unknown"),
            "shelves": book.get("shelves", []),
            "user_rating": book.get("user_rating", None),
            "publication_year": book.get("publication_year", None)
        })
    
    return {
        "user1_books": user1_processed,
        "user2_books": user2_processed
    }


def _canonicalize_genre(label: str) -> Optional[str]:
    """Map a free-form label to the canonical taxonomy if possible."""
    if not label:
        return None
    s = str(label).strip()
    if not s:
        return None
    low = s.lower()
    if low in GENRE_ALIASES:
        return GENRE_ALIASES[low]
    # Try canonical exact match (case-insensitive)
    for g in GENRE_TAXONOMY:
        if low == g.lower():
            return g
    # Try simple contains heuristics
    contains_map = {
        "science": "Science",
        "philosophy": "Philosophy",
        "business": "Business",
        "history": "History",
        "memoir": "Memoir",
        "biograph": "Biography",
        "romance": "Romance",
        "thriller": "Thriller & Crime",
        "mystery": "Mystery",
        "poetry": "Poetry",
        "horror": "Horror",
        "fantasy": "Fantasy",
        "fiction": "Contemporary Fiction",
    }
    for k, v in contains_map.items():
        if k in low and v in GENRE_TAXONOMY:
            return v
    return None


def _filter_and_cap_genres(genres: List[str], max_len: int) -> List[str]:
    seen: set = set()
    out: List[str] = []
    for g in genres or []:
        cg = _canonicalize_genre(g)
        if cg and cg in GENRE_TAXONOMY and cg not in seen:
            out.append(cg)
            seen.add(cg)
        if len(out) >= max_len:
            break
    return out


def _sanitize_and_finalize(insights: Dict) -> Dict:
    """Ensure genres are from taxonomy, cap list sizes, and recompute shared_genres."""
    result = INSIGHT_STRUCTURE.copy()
    result.update(insights or {})

    gi = result.get("genre_insights", {}) or {}
    u1 = _filter_and_cap_genres(gi.get("user1_preferences", []), MAX_USER_GENRES)
    u2 = _filter_and_cap_genres(gi.get("user2_preferences", []), MAX_USER_GENRES)
    shared = list(sorted(set(u1).intersection(u2)))[:MAX_SHARED_GENRES]
    recs = (gi.get("recommendations", []) or [])[:MAX_RECOMMENDATIONS]

    result["genre_insights"] = {
        "user1_preferences": u1,
        "user2_preferences": u2,
        "shared_genres": shared,
        "recommendations": recs,
    }

    return result


def generate_insights_with_llm(book_data: Dict, user1_name: str, user2_name: str, blend_metrics: Dict) -> Dict:
    """
    Generate structured insights using GPT-4o-mini based on book data.
    
    Args:
        book_data: Structured book data for both users
        user1_name: Name of first user
        user2_name: Name of second user
        blend_metrics: Blend metrics for context
        
    Returns:
        Dictionary with structured AI insights
    """
    # Create prompt for the LLM
    system_prompt = f"""You are an expert literary analyst and book recommendation engine.
Your task is to analyze reading preferences for two users and provide structured insights.
You must follow these requirements exactly:
1. Analyze the book data to identify patterns, genre preferences, and reading styles
2. Compare fiction vs non-fiction preferences between users
3. Generate book recommendations based on shared interests
4. Return ONLY a valid JSON object with exactly the structure defined in the "REQUIRED_OUTPUT_FORMAT"
5. Do not include any explanations or text outside the JSON structure
6. Every field in the REQUIRED_OUTPUT_FORMAT must be filled

GENRE TAXONOMY (choose only from this list; do NOT invent new labels):
{json.dumps(GENRE_TAXONOMY, indent=2)}

LIMITS:
- user1_preferences: up to {MAX_USER_GENRES}
- user2_preferences: up to {MAX_USER_GENRES}
- shared_genres: up to {MAX_SHARED_GENRES}, must be intersection of the two user lists
- recommendations: up to {MAX_RECOMMENDATIONS}

REQUIRED_OUTPUT_FORMAT:
{{
  "genre_insights": {{
    "user1_preferences": ["Genre1", "Genre2", "Genre3"],
    "user2_preferences": ["Genre1", "Genre2", "Genre3"],
    "shared_genres": ["Genre1", "Genre3"],
    "recommendations": ["Consider exploring Historical Fiction together", "Both might enjoy Science books"]
  }},
  "fiction_nonfiction": {{
    "user1_ratio": 0.75,
    "user2_ratio": 0.25,
    "compatibility": "User1 strongly prefers fiction while User2 favors non-fiction. This creates an opportunity to expand each other's horizons."
  }},
  "reading_style": {{
    "user1_summary": "Tends to read contemporary bestsellers with high ratings",
    "user2_summary": "Favors classics and literary fiction with diverse themes",
    "compatibility_score": 0.65,
    "compatibility_details": "While their genre preferences differ, both show appreciation for well-crafted narratives and character-driven stories."
  }},
  "book_recommendations": {{
    "for_both": ["Title by Author", "Title by Author"],
    "for_user1": ["Title by Author", "Title by Author"],
    "for_user2": ["Title by Author", "Title by Author"]
  }}
}}
"""

    # User prompt with the book data
    user_prompt = f"""Analyze the reading preferences for {user1_name} and {user2_name} based on their book data.
Generate insights about their genre preferences using ONLY the provided GENRE TAXONOMY (do not create new labels), fiction vs non-fiction ratio, and overall compatibility.
Use the users' actual names in your response. Recommend books that each user might enjoy based on the other's reading history. Respect the LIMITS specified.

USER 1 ({user1_name}) BOOKS:
{json.dumps(book_data['user1_books'][:30], indent=2)}

USER 2 ({user2_name}) BOOKS:
{json.dumps(book_data['user2_books'][:30], indent=2)}

BLEND METRICS:
{json.dumps(blend_metrics, indent=2)}

Return the analysis in the exact JSON format specified.
"""

    try:
        # Call the OpenAI API with GPT-4o-mini
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.3,  # Lower temperature for more consistent results
            response_format={"type": "json_object"},  # Ensure JSON response
            max_tokens=1200
        )
        
        # Parse the response
        insights_text = response.choices[0].message.content
        insights = json.loads(insights_text)
        
        # Ensure the response matches our expected structure and sanitize to taxonomy
        validated_insights = validate_and_structure_insights(insights)
        validated_insights = _sanitize_and_finalize(validated_insights)
        
        # Add user names
        validated_insights["users"] = {
            "user1": user1_name,
            "user2": user2_name
        }
        
        return validated_insights
        
    except Exception as e:
        # Fallback with error message
        return {
            "error": f"Failed to generate AI insights: {str(e)}",
            "insights": INSIGHT_STRUCTURE
        }


def validate_and_structure_insights(insights: Dict) -> Dict:
    """
    Ensure the insights match the expected structure and fill in any missing fields.
    
    Args:
        insights: Raw insights from LLM
        
    Returns:
        Validated and structured insights
    """
    result = INSIGHT_STRUCTURE.copy()
    
    # Copy over values from the insights, keeping the structure
    if "genre_insights" in insights:
        for key in result["genre_insights"].keys():
            if key in insights["genre_insights"]:
                result["genre_insights"][key] = insights["genre_insights"][key]
    
    if "fiction_nonfiction" in insights:
        for key in result["fiction_nonfiction"].keys():
            if key in insights["fiction_nonfiction"]:
                result["fiction_nonfiction"][key] = insights["fiction_nonfiction"][key]
    
    if "reading_style" in insights:
        for key in result["reading_style"].keys():
            if key in insights["reading_style"]:
                result["reading_style"][key] = insights["reading_style"][key]
    
    if "book_recommendations" in insights:
        for key in result["book_recommendations"].keys():
            if key in insights["book_recommendations"]:
                result["book_recommendations"][key] = insights["book_recommendations"][key]
    
    return result


def get_ai_insights(user1_books: List[Dict], user2_books: List[Dict],
                    user1_name: str = "User 1", user2_name: str = "User 2",
                    blend_metrics: Optional[Dict] = None) -> Dict[str, Any]:
    """
    Generate AI-powered insights about two users' reading preferences.

    Args:
        user1_books: List of books from first user
        user2_books: List of books from second user
        user1_name: Name of first user
        user2_name: Name of second user
        blend_metrics: Optional metrics from blend function

    Returns:
        Dictionary containing structured AI insights
    """
    # Check if we have books data
    if not user1_books or not user2_books:
        return {"error": "No books found for one or both users"}

    # Prepare data for LLM analysis
    analysis_data = prepare_book_data_for_analysis(user1_books, user2_books)

    # Generate AI insights
    return generate_insights_with_llm(analysis_data, user1_name, user2_name, blend_metrics or {})


if __name__ == "__main__":
    user1_id = "42944663"
    user2_id = "91692289"
