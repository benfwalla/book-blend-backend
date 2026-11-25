import os
import dotenv
import json
from copy import deepcopy
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
    "connection_insights": {
        "summary": "",
        "shared_signals": [],
        "taste_bridges": [],
        "conversation_starters": []
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
    # Core - Fiction
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

    # Core - Nonfiction
    "Memoir",
    "Biography",
    "History",
    "Philosophy",
    "Psychology",
    "Self-Help",
    "Business & Economics",
    "Science & Technology",
    "Poetry",
    "Religion & Spirituality",

    # Flexible - Audience/Form
    "Young Adult",
    "New Adult",
    "Middle Grade",
    "Children’s",
    "Short Stories & Essays",
    "Graphic Novels & Comics",

    # Flexible - Identity/Theme
    "LGBTQ+",
    "Cultural & Regional Literature",
    "True Crime",
    "Health, Food & Lifestyle"
]



# Limits to keep responses tight and useful
MAX_USER_GENRES = 8
MAX_SHARED_GENRES = 5
MAX_RECOMMENDATIONS = 4
MAX_SHARED_SIGNALS = 2
MAX_TASTE_BRIDGES = 2
MAX_CONVERSATION_STARTERS = 3


def _limit_words(value: Any, max_words: int) -> str:
    """Keep generated prose compact even when the model drifts past the prompt limit."""
    text = str(value or "").strip()
    words = text.split()
    if len(words) <= max_words:
        return text
    ending = "?" if text.endswith("?") else "…"
    shortened = " ".join(words[:max_words]).rstrip(" ,;:.-!?—")
    return shortened + ending

def prepare_book_data_for_analysis(user1_books: List[Dict], user2_books: List[Dict]) -> Dict:
    """
    Extract and structure relevant book data for LLM analysis.
    Only includes books that are on the "read" shelf to ensure we analyze completed books.
    
    Args:
        user1_books: List of books from first user
        user2_books: List of books from second user
        
    Returns:
        Dictionary with structured book data (only "read" books)
    """
    def is_read_book(book: Dict) -> bool:
        """Check if a book is on the 'read' shelf."""
        shelves = book.get("user_shelves", [])
        
        # Handle shelves as a string (e.g., "read")
        if isinstance(shelves, str):
            return shelves.lower() == "read"
        
        # Handle shelves as a list (e.g., ["read", "fantasy"])
        if isinstance(shelves, list):
            return "read" in [shelf.lower() for shelf in shelves]
        
        return False
    
    # Extract just the necessary fields and filter for "read" books only
    user1_processed = []
    for book in user1_books:
        if is_read_book(book):
            user1_processed.append({
                "title": book.get("title", "Unknown"),
                "author": book.get("author", "Unknown"),
                "genres": book.get("genres", []),
                "user_rating": book.get("user_rating", None),
                "publication_year": book.get("book_published", None),
                "pages": book.get("num_pages", None),
            })
    
    user2_processed = []
    for book in user2_books:
        if is_read_book(book):
            user2_processed.append({
                "title": book.get("title", "Unknown"),
                "author": book.get("author", "Unknown"),
                "genres": book.get("genres", []),
                "user_rating": book.get("user_rating", None),
                "publication_year": book.get("book_published", None),
                "pages": book.get("num_pages", None),
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
    
    # Try canonical exact match (case-insensitive)
    for g in GENRE_TAXONOMY:
        if low == g.lower():
            return g
    
    # Try simple contains heuristics
    contains_map = {
        "science": "Science & Technology",
        "philosophy": "Philosophy",
        "business": "Business & Economics",
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
    result = deepcopy(INSIGHT_STRUCTURE)
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

    style = result.get("reading_style", {}) or {}
    result["reading_style"] = {
        **style,
        "user1_summary": _limit_words(style.get("user1_summary", ""), 40),
        "user2_summary": _limit_words(style.get("user2_summary", ""), 40),
        "compatibility_details": _limit_words(style.get("compatibility_details", ""), 40),
    }

    connection = result.get("connection_insights", {}) or {}
    result["connection_insights"] = {
        "summary": _limit_words(connection.get("summary", ""), 45),
        "shared_signals": [_limit_words(item, 28) for item in connection.get("shared_signals", []) if str(item).strip()][:MAX_SHARED_SIGNALS],
        "taste_bridges": [_limit_words(item, 28) for item in connection.get("taste_bridges", []) if str(item).strip()][:MAX_TASTE_BRIDGES],
        "conversation_starters": [_limit_words(item, 24) for item in connection.get("conversation_starters", []) if str(item).strip()][:MAX_CONVERSATION_STARTERS],
    }

    return result


def generate_insights_with_llm(book_data: Dict, user1_name: str, user2_name: str, blend_metrics: Dict) -> Dict:
    """
    Generate structured, evidence-based insights from both reading histories.
    
    Args:
        book_data: Structured book data for both users
        user1_name: Name of first user
        user2_name: Name of second user
        blend_metrics: Blend metrics for context
        
    Returns:
        Dictionary with structured AI insights
    """
    # Create prompt for the LLM
    system_prompt = f"""You are a perceptive literary analyst writing a personalized reading-chemistry report for two real people.
Your task is to analyze their completed books and produce specific, evidence-based insights that feel observant rather than generic.
You must follow these requirements exactly:
1. Ground every qualitative claim in supplied titles, authors, genres, ratings, dates, page counts, or blend metrics.
2. Mention concrete books or authors in both reading-style summaries and in most connection bullets.
3. Distinguish durable patterns from one-off books. Never infer demographics, politics, health, or other sensitive traits.
4. Compare fiction vs non-fiction preferences and explain both affinities and productive differences.
5. Recommend books based on shared interests. Cross-recommendations must come from the other user's supplied list.
6. Make the prose warm, conversational, and economical. Prefer a sharp observation over an exhaustive explanation. Avoid generic phrases such as "diverse themes," "well-crafted narratives," "expand their horizons," or "something for everyone."
7. Return ONLY a valid JSON object with exactly the structure defined in the "REQUIRED_OUTPUT_FORMAT".
8. Do not include explanations outside the JSON and fill every field.
9. Keep each reading-style summary to one sentence and at most 35 words. Keep compatibility_details to one sentence and at most 35 words.
10. Keep the connection summary to one sentence and at most 40 words. Each connection bullet must be one sentence and at most 24 words; each conversation starter must be a short question of at most 20 words.

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
    "user1_preferences": ["Genre1", "Genre2", "Genre3", "Genre4", "Genre5"],
    "user2_preferences": ["Genre1", "Genre2", "Genre3", "Genre4", "Genre5"],
    "shared_genres": ["Genre1", "Genre3"],
    "recommendations": ["Consider exploring Historical Fiction together", "Both might enjoy Science books"]
  }},
  "fiction_nonfiction": {{
    "user1_ratio": 0.75,
    "user2_ratio": 0.25,
    "compatibility": "User1 strongly prefers fiction while User2 favors non-fiction. This creates an opportunity to expand each other's horizons."
  }},
  "reading_style": {{
    "user1_summary": "One concise, specific sentence grounded in titles, authors, genres, ratings, eras, or length patterns.",
    "user2_summary": "One concise, specific sentence grounded in titles, authors, genres, ratings, eras, or length patterns.",
    "compatibility_score": 0.65,
    "compatibility_details": "One concise sentence explaining where their tastes connect and diverge."
  }},
  "connection_insights": {{
    "summary": "One short, evidence-rich sentence about this pair's reading chemistry.",
    "shared_signals": ["A brief shared pattern with book or author evidence", "A second brief shared pattern"],
    "taste_bridges": ["A brief way one reader's library can lead the other somewhere new", "A second brief bridge"],
    "conversation_starters": ["A short question tied to books they read", "A second short question", "A third short question"]
  }},
  "book_recommendations": {{
    "for_both": ["Book Title 1 by Author Name", "Book Title 2 by Author Name"],
    "for_user1": ["Book Title 3 by Author Name", "Book Title 4 by Author Name"],
    "for_user2": ["Book Title 5 by Author Name", "Book Title 6 by Author Name"]
  }}
}}
"""
    # User prompt with the book data
    user_prompt = f"""Analyze the reading preferences for {user1_name} and {user2_name} based on their book data.
Generate a distinctive reading-chemistry report using the users' actual names. Use ONLY the provided GENRE TAXONOMY for genre labels.

CRITICAL FOR GENRE ANALYSIS:
1. Prefer the supplied Goodreads genres, then use titles and authors to resolve ambiguity.
2. For each user, identify their TOP genres from the GENRE TAXONOMY based on the books they've read
3. Map books like "Dune" → "Science Fiction", "The Hobbit" → "Fantasy", "When Breath Becomes Air" → "Memoir", etc.
4. You MUST populate the user1_preferences and user2_preferences arrays with genres from the taxonomy
5. The shared_genres should be the intersection of the two users' preferences
6. DO NOT leave genre arrays empty unless there are truly no books to analyze

CRITICAL: For book recommendations - YOU MUST PROVIDE REAL, SPECIFIC BOOK TITLES AND AUTHORS:
- "for_user1": Recommend REAL books from User2's reading list that User1 hasn't read but would likely enjoy based on User1's preferences
- "for_user2": Recommend REAL books from User1's reading list that User2 hasn't read but would likely enjoy based on User2's preferences
- "for_both": Recommend REAL, well-known books that NEITHER user has read, based on their shared interests and genres
- Do NOT use placeholder text like "Book Title 1 by Author Name". Use actual book titles and authors (i.e. "The Seven Husbands of Evelyn Hugo by Taylor Jenkins Reid"). Every recommendation must be a real, published book with the exact title and author name.

CRITICAL FOR CONNECTION INSIGHTS:
- Cite specific titles/authors and use the supplied numeric metrics where useful.
- "shared_signals" explains genuine overlap; "taste_bridges" explains interesting differences.
- Conversation starters should sound like questions two friends might actually ask each other after seeing this blend.
- Do not merely restate the genre arrays.

Respect the LIMITS specified.

BLEND METRICS:
{json.dumps(blend_metrics, ensure_ascii=False, default=str)}

USER 1 ({user1_name}) read books with metadata:
{json.dumps(book_data['user1_books'], ensure_ascii=False, default=str)}

USER 2 ({user2_name}) read books with metadata:
{json.dumps(book_data['user2_books'], ensure_ascii=False, default=str)}

Return the analysis in the exact JSON format specified.
"""

    try:
        model = os.environ.get("OPENAI_INSIGHTS_MODEL", "gpt-6.2-sol")
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            reasoning_effort="medium",
            response_format={"type": "json_object"},
            max_completion_tokens=3000
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
        fallback = deepcopy(INSIGHT_STRUCTURE)
        fallback["error"] = f"Failed to generate AI insights: {str(e)}"
        fallback["users"] = {"user1": user1_name, "user2": user2_name}
        return fallback


def validate_and_structure_insights(insights: Dict) -> Dict:
    """
    Ensure the insights match the expected structure and fill in any missing fields.
    
    Args:
        insights: Raw insights from LLM
        
    Returns:
        Validated and structured insights
    """
    result = deepcopy(INSIGHT_STRUCTURE)
    
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

    if "connection_insights" in insights:
        for key in result["connection_insights"].keys():
            if key in insights["connection_insights"]:
                result["connection_insights"][key] = insights["connection_insights"][key]
    
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
