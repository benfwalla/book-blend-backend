# BookBlend Scoring Method
BookBlend compares two Goodreads users to estimate how well their reading tastes “blend” on a 0–100 scale. It looks at what you’ve read and saved, the kinds of books you like, and a few simple patterns.

Where the data comes from
We fetch your books via Goodreads’ RSS (see [util/rss_feed_books.py](util/rss_feed_books.py)). This includes shelves like “read”, “to-read”, ratings, pages, and publication year.

Since the RSS feed doesn't include desirable metadata, namely the genre of each book, we ask an AI to summarize each person’s top genres using a fixed, simple genre list (see 
[util/ai_insights.py](util/ai_insights.py)). We normalize synonyms (e.g., “sci-fi” → “Science Fiction”) so results are consistent.

### How we tidy the data
- **Shelf focus:** Some stats use only “read” books (e.g., pages read, eras). Others consider all shelves (e.g., overlap).
- **Light normalization:** Titles/authors are compared by IDs; dates/pages/ratings are converted to comparable numbers.
- **Genres constrained:** The AI is instructed to choose only from our small, curated genre list; we map aliases to that list.

### What goes into the score
We compute a few component scores between 0 and 1, then combine them with weights. Finally, we scale to 0–100:

`score = 100 × [0.25×common_books + 0.10×common_authors + 0.25×genres + 0.15×era + 0.10×rating + 0.10×length + 0.05×year]`

**Score breakdown:**
- **Common books (25%):** How much your libraries overlap. Mostly based on books you’ve both read. Also gives partial credit if a title is on any shelf for both (e.g., one read, one to‑read).
- **Common authors (10%):** How many authors you share, relative to your combined author pool.
- **Genres (25%):** How many genres you share from the canonical list, normalized by the smaller person’s list.
- **Era similarity (15%):** How similarly your reading skews by publication era (e.g., mostly 2010–present vs. classics).
- **Rating proximity (10%):** How close your average ratings are; closer averages score higher.
- **Length similarity (10%): H**ow similar your median page lengths are; closer medians score higher.
- **Year proximity (5%):** How similar your average publication years are.
