import os
import dotenv
import json
from typing import Dict, List, Any, Optional
from openai import OpenAI
from blend import blend_two_users

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
Generate insights about their genre preferences, fiction vs non-fiction ratio, and overall compatibility.
Recommend books that each user might enjoy based on the other's reading history.

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
            max_tokens=2000
        )
        
        # Parse the response
        insights_text = response.choices[0].message.content
        insights = json.loads(insights_text)
        
        # Ensure the response matches our expected structure
        validated_insights = validate_and_structure_insights(insights)
        
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


if __name__ == "__main__":
    user1_id = "42944663"
    user2_id = "91692289"
    
    # Get blend data with books included
    blend_data = blend_two_users(user1_id, user2_id, shelf="all", include_books=True)
    
    # Extract user names for better prompting
    user1_name = blend_data["users"][user1_id].get("name", f"User {user1_id}")
    user2_name = blend_data["users"][user2_id].get("name", f"User {user2_id}")
    
    # Get book data for analysis
    user1_books = blend_data["users"][user1_id].get("books", [])
    user2_books = blend_data["users"][user2_id].get("books", [])
    
    # Extract blend metrics
    blend_metrics = {k: v for k, v in blend_data.items() if k not in ['users']}
    
    # Generate insights
    insights = get_ai_insights(user1_books, user2_books, user1_name, user2_name, blend_metrics)
    print(json.dumps(insights, indent=2))
