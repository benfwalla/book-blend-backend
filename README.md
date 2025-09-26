# Book Blend Backend
An API for Book Blend: Spotify Blend for books

The API is hosted on Render and provides endpoints to analyze reading compatibility between two Goodreads users.

## Endpoints

### `/books` - Get User's Books
Get all books from a given Goodreads user_id and shelf.

Example cURL:
```
curl --location 'https://book-blend-backend.onrender.com/books?user_id=42944663&shelf=all'
```

**Query Params:**
- `user_id`: A Goodreads user id. Found in your profile URL: ![goodreads_id.png](goodreads_id.png)
- `shelf`: The shelf to retrieve (`all`, `to-read`, `currently-reading`, `read`, or custom shelf)

### `/blend` - Compare Two Users
Analyze reading compatibility between two Goodreads users.

Example cURL:
```
curl --location 'https://book-blend-backend.onrender.com/blend?user_id1=42944663&user_id2=91692289'
```

**Query Params:**
- `user_id1`: First Goodreads user ID
- `user_id2`: Second Goodreads user ID

## How the Blend Score Works

The `/blend` endpoint calculates a compatibility score (0-100) based on multiple factors:

### Score Components & Weights:
1. **Shared Books (30%)** - Books both users have read
   - *Example: If you've both read 10 books out of your 50-book library, that's 20% overlap*
   
2. **Common Authors (25%)** - Authors both users enjoy
   - *Example: You both love J.R.R. Tolkien, Stephen King, and 15 other authors*
   
3. **Genre Compatibility (20%)** - Similar reading preferences (AI-analyzed)
   - *Example: You both enjoy Fantasy, Science Fiction, and Non-Fiction*
   
4. **Era Preferences (10%)** - When the books you read were published
   - *Example: You both prefer books from 2010-present vs. classics*
   
5. **Rating Standards (5%)** - How similarly you rate books
   - *Example: You both tend to give 4-star ratings on average*
   
6. **Book Length (8%)** - Preference for similar page counts
   - *Example: You both prefer 300-400 page books*
   
7. **Publication Year (2%)** - Specific year preferences
   - *Example: You both read lots of 2020s releases*

### Context-Aware Scoring:
- **Library Size Matters**: 10 shared books means more if you have 50 books vs. 500 books
- **Absolute Bonuses**: Having many shared books/authors gets extra credit
- **Meaningful Overlap**: The algorithm rewards genuine compatibility over coincidence

### Score Ranges:
- **80-100**: Exceptional compatibility - you're reading soulmates!
- **60-79**: High compatibility - lots of shared interests
- **40-59**: Moderate compatibility - some overlap with room to explore
- **20-39**: Low compatibility - different tastes but potential for discovery
- **0-19**: Very different reading styles - great for expanding horizons!