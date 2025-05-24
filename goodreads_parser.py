import requests
from bs4 import BeautifulSoup
import sys
import pandas as pd
import re
from typing import Union, Dict, List
import warnings

warnings.filterwarnings('ignore')


def clean_data(df):
    """Clean and transform the raw DataFrame."""
    if df is None or df.empty:
        return df
        
    # Make a copy to avoid warnings
    cleaned_df = df.copy()
    
    # Clean up author names - remove trailing asterisks and normalize format
    if 'author' in cleaned_df.columns:
        cleaned_df['author'] = cleaned_df['author'].str.replace(r'\*$', '', regex=True)
    
    # Clean cover image URLs - completely remove size limitation for higher resolution
    if 'cover' in cleaned_df.columns:
        # First check if we're dealing with S.gr-assets or compressed.photo.goodreads URLs
        # Handle the pattern that looks like: /books/1234567890l/12345._SY75_.jpg
        cleaned_df['cover'] = cleaned_df['cover'].str.replace(r'(\d+)\.(_S[XY]\d+_)(\.jpg)', r'\1\3', regex=True)
        
        # Also handle any remaining _SX/SY patterns that might exist elsewhere in the URL
        cleaned_df['cover'] = cleaned_df['cover'].str.replace(r'_S[XY]\d+_', '', regex=True)

    if 'rating' in cleaned_df.columns:
        # Extract numeric rating only if it's in brackets like [ 4 of 5 stars ]
        cleaned_df['rating_numeric'] = (
            cleaned_df['rating']
            .str.extract(r'\[\s*(\d) of 5 stars\s*\]', expand=False)
            .astype(float)
        )

    # Clean ISBN columns
    for isbn_col in ['isbn', 'isbn13', 'asin']:
        if isbn_col in cleaned_df.columns:
            # Remove any prefix like "isbn" or "asin"
            cleaned_df[isbn_col] = cleaned_df[isbn_col].str.replace(f'{isbn_col}', '', regex=False)
    
    # Clean date fields - but handle pub dates and reading dates differently
    date_columns = [col for col in cleaned_df.columns if 'date' in col]
    for date_col in date_columns:
        if date_col not in cleaned_df.columns:
            continue
            
        # Convert "not set" to NaN for all date columns
        cleaned_df[date_col] = cleaned_df[date_col].replace('not set', pd.NA)
        
        # For publication dates, extract just the year
        if date_col in ['date_pub', 'date_pub_edition']:
            # Use a more specific regex to extract the year from pub date strings
            # Handle formats like "Mar 18, 2025", "2019", "Apr 09, 2001"
            year_pattern = r'(?:^|\D)(\d{4})(?:\D|$)'  # Look for 4-digit year
            years = cleaned_df[date_col].str.extract(year_pattern, expand=False)
            cleaned_df[date_col] = years
        # For reading dates (started, read), convert to ISO format directly
        elif date_col in ['date_started', 'date_read', 'date_added', 'date_purchased']:
            try:
                # Convert various date formats to consistent ISO format in-place
                # Handle formats like "Mar 18, 2025", "2019", "Apr 09, 2001"
                temp_dates = pd.to_datetime(cleaned_df[date_col], errors='coerce', format='mixed')
                # Replace original column values with ISO formatted dates
                cleaned_df[date_col] = temp_dates.dt.strftime('%Y-%m-%d')
            except:
                # If conversion fails, just keep the original
                pass
    
    # Clean up page numbers - extract just the digits
    if 'num_pages' in cleaned_df.columns:
        cleaned_df['pages'] = cleaned_df['num_pages'].str.extract(r'(\d+)', expand=False)
        cleaned_df['pages'] = pd.to_numeric(cleaned_df['pages'], errors='coerce')
        # Drop the original num_pages column with "pp" text
        cleaned_df = cleaned_df.drop(columns=['num_pages'])
    
    # Clean up num_ratings - remove commas and convert to numeric
    if 'num_ratings' in cleaned_df.columns:
        # Remove commas from numbers (e.g., "1,234" -> "1234")
        cleaned_df['num_ratings'] = cleaned_df['num_ratings'].str.replace(',', '', regex=False)
        # Convert to numeric
        cleaned_df['num_ratings'] = pd.to_numeric(cleaned_df['num_ratings'], errors='coerce')
    
    # Clean up review column - replace "None" with empty string
    if 'review' in cleaned_df.columns:
        cleaned_df['review'] = cleaned_df['review'].replace('None', '')
        
    # Clean up comments column - convert to integer
    if 'comments' in cleaned_df.columns:
        cleaned_df['comments'] = pd.to_numeric(cleaned_df['comments'], errors='coerce').fillna(0).astype(int)
    
    # Remove unnecessary/duplicate columns or rename for clarity
    columns_to_drop = []
    for col in cleaned_df.columns:
        # Drop columns that are just IDs, labels, or redundant
        # Preserve 'shelf' information
        if col in ['checkbox', 'position', 'recommender', 'purchase_location', 'condition', 'actions',
                   'rating', 'shelves', 'notes', 'votes', 'owned', 'date_purchased', 'format', 'date_added']:
            # Don't drop the shelf column
            if col != 'shelf':
                columns_to_drop.append(col)
            
    # Drop the columns
    if columns_to_drop:
        cleaned_df = cleaned_df.drop(columns=[col for col in columns_to_drop if col in cleaned_df.columns])
    
    # Reorder columns - ensure pages is next to asin
    if all(col in cleaned_df.columns for col in ['asin', 'pages']):
        # Get the current column list
        cols = list(cleaned_df.columns)
        
        # Find the position of asin
        asin_pos = cols.index('asin')
        
        # Remove pages from its current position
        if 'pages' in cols:
            cols.remove('pages')
            
        # Insert pages right after asin
        cols.insert(asin_pos + 1, 'pages')
        
        # Reorder the DataFrame
        cleaned_df = cleaned_df[cols]
    
    # Make sure shelf is near the front of the columns if present
    if 'shelf' in cleaned_df.columns:
        cols = list(cleaned_df.columns)
        if 'shelf' in cols:
            cols.remove('shelf')
            # Insert shelf after title and author
            if 'author' in cols:
                author_pos = cols.index('author')
                cols.insert(author_pos + 1, 'shelf')
            elif 'title' in cols:
                title_pos = cols.index('title')
                cols.insert(title_pos + 1, 'shelf')
            else:
                # Insert at beginning if no title/author
                cols.insert(0, 'shelf')
            cleaned_df = cleaned_df[cols]
    
    return cleaned_df

def fetch_goodreads_data_as_df(url):
    """Fetches URL, finds 'books' table, and converts it to a pandas DataFrame (MVP)."""
    # 1. Fetch HTML
    print(f"Fetching: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'cookie': 'ubid-main=134-4882758-0608221; _session_id2=68a44b4822176e6ad8cd03f555e9071e; ccsid=674-4080800-6001320; locale=en; at-main=Atza|IwEBIGAPM--VV4UkJdGeSaO5M3C-dEjuFag_fzajS_mvXF5IzzXQiF23a0SN4kvSI2wTKC1GOOEvG2-c826pTHEmQEdgoA7rgAgJa8mxVygOIBxTienV5tLvb4kKujkJAlI0DXGm5P6oKc2A0NZF5vMViTCTQXR35HhaFQ8lVeQGNh03c9wsHrqOtCNrrgWFHjV_PctlnGor6c6IQ6Jm7rUUG6HubWl2r6F0NQNcBzOYGy1rKGUnaj_Fac1n-YuAeXQx2go'
        }
        response = requests.get(url, headers=headers, timeout=15, verify=False)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}", file=sys.stderr)
        return None

    # 2. Parse HTML and Find Table
    soup = BeautifulSoup(response.text, 'html.parser')
    books_table = soup.find('table', id='books')
    
    if not books_table:
        print("Error: Could not find the 'books' table.", file=sys.stderr)
        return None

    # 3. Extract Data with Column Consistency - But with Better Value Extraction
    # First, get the header row to identify all possible columns
    header_row = books_table.find('tr', id='booksHeader')

    # Get all header cells (including hidden ones)
    header_cells = [cell for cell in header_row.find_all('th')]
    header_values = []
    for cell in header_cells:
        # Get the alt attribute which is the clean column name
        header_name = cell.get('alt', '').strip()
        if not header_name:
            # Fall back to the text if alt is not available
            header_name = cell.get_text(strip=True)
        header_values.append(header_name)

    # Add new columns for Goodreads ID and URL
    header_values.extend(['goodreads_id', 'goodreads_url'])

    # Get column positions for data extraction
    positions = []
    for cell in header_cells:
        # For each header cell, save its position in the table
        alt_attr = cell.get('alt', '')
        positions.append(alt_attr)

    # Now extract data rows with consistent column alignment
    all_rows = [header_values]  # Start with header values

    # Process each data row
    for row in books_table.find_all('tr', class_='bookalike'):
        # Extract data cells more carefully
        row_data = []

        # Track Goodreads ID and URL for this row
        book_id = ""
        book_url = ""

        # Get all cells regardless of visibility
        data_cells = row.find_all('td')

        # First, search for book ID and URL from the title link
        title_cell = None
        for i, pos in enumerate(positions):
            if pos == 'title' and i < len(data_cells):
                title_cell = data_cells[i]
                break

        if title_cell:
            title_link = title_cell.find('a')
            if title_link and title_link.has_attr('href'):
                href = title_link.get('href', '')
                # Extract book ID from URL path
                # Format is typically: /book/show/36072.The_7_Habits_of_Highly_Effective_People
                book_id_match = re.search(r'/book/show/(\d+)', href)
                if book_id_match:
                    book_id = book_id_match.group(1)
                    book_url = f"https://www.goodreads.com{href}"

        # Now process each cell normally
        # Ensure we have a value for each header column
        for i, pos in enumerate(positions):
            if i < len(data_cells):
                cell = data_cells[i]

                # Special handling for cover column - extract image URL
                if pos == 'cover':
                    img_tag = cell.find('img')
                    if img_tag and img_tag.has_attr('src'):
                        cell_value = img_tag['src']
                    else:
                        cell_value = ""
                # Special handling for title column - extract title text
                elif pos == 'title':
                    title_link = cell.find('a')
                    if title_link:
                        cell_value = title_link.get_text(strip=True)
                    else:
                        cell_value = cell.get_text(strip=True)
                # Special handling for rating column - try to get the title attribute
                elif pos == 'rating':
                    # Try to extract the rating text from the staticStars span's title attribute
                    stars_span = cell.find('span', class_='staticStars')
                    if stars_span and stars_span.has_attr('title'):
                        cell_value = stars_span['title']
                    else:
                        # Fall back to normal text extraction if no title attribute
                        value_div = cell.find('div', class_='value')
                        if value_div:
                            cell_value = value_div.get_text(strip=True)
                        else:
                            cell_value = cell.get_text(strip=True)

                        # Remove label from value if present
                        label = cell.find('label')
                        if label:
                            label_text = label.get_text(strip=True)
                            if cell_value.startswith(label_text):
                                cell_value = cell_value[len(label_text):].strip()
                else:
                    # For other columns, extract only the value div content, not the label
                    value_div = cell.find('div', class_='value')
                    if value_div:
                        # Use the value div's content
                        cell_value = value_div.get_text(strip=True)
                    else:
                        # Fall back to the cell's text if no value div
                        cell_value = cell.get_text(strip=True)

                    # If the cell has a label, try to remove it from the value
                    label = cell.find('label')
                    if label:
                        label_text = label.get_text(strip=True)
                        # Only remove if the label text is at the beginning of the value
                        if cell_value.startswith(label_text):
                            cell_value = cell_value[len(label_text):].strip()

                row_data.append(cell_value)
            else:
                # If cell doesn't exist, add an empty value
                row_data.append("")

        # Add Goodreads ID and URL as the last columns
        row_data.append(book_id)
        row_data.append(book_url)

        if row_data:  # Only add if we got some data
            all_rows.append(row_data)

    df = pd.DataFrame(all_rows[1:], columns=all_rows[0])

    return df

def get_goodreads_user_books_by_page(user_id: str, page_num: int = 1, shelf: str = 'all', return_format: str = 'dataframe') -> Union[pd.DataFrame, Dict]:
    """
    Fetch a specific page of a Goodreads user's books and return as DataFrame or JSON.
    
    Args:
        user_id (str): Goodreads user ID
        page_num (int): Page number to fetch (default: 1)
        shelf (str): Which shelf to query - 'read', 'currently-reading', 'to-read', 'all' (default: 'all')
        return_format (str): 'dataframe' or 'json' (default: 'dataframe')
        
    Returns:
        Union[pd.DataFrame, Dict]: User's books in the requested format
    """
    # If 'all' is specified, fetch books from each shelf and combine them
    if shelf.lower() == 'all':
        # Define the shelves to fetch
        shelves = ['read', 'currently-reading', 'to-read']
        all_books = []
        
        # Fetch books from each shelf
        for single_shelf in shelves:
            # Construct the URL for this shelf
            url = f"https://www.goodreads.com/review/list/129990632?utf8=%E2%9C%93&utf8=%E2%9C%93&per_page=100&page={page_num}&shelf={single_shelf}"
            
            # Fetch and process the data
            raw_df = fetch_goodreads_data_as_df(url)
            
            # If fetching succeeded, add shelf information and clean the data
            if raw_df is not None and not raw_df.empty:
                # Add shelf information 
                raw_df['shelf'] = single_shelf
                clean_df = clean_data(raw_df)
                all_books.append(clean_df)
                print(f"Found {len(clean_df)} books on shelf '{single_shelf}' (page {page_num})")
        
        # Combine all books
        if all_books:
            combined_df = pd.concat(all_books, ignore_index=True)
            
            # Return in requested format
            if return_format.lower() == 'json':
                return combined_df.to_dict(orient='records')
            else:
                return combined_df
        else:
            return pd.DataFrame() if return_format.lower() == 'dataframe' else []
    
    # For specific shelves, just fetch that shelf
    else:
        # Construct the URL for this user's page
        url = f"https://www.goodreads.com/review/list/{user_id}?page={page_num}&shelf={shelf}"
        
        # Fetch and process the data
        raw_df = fetch_goodreads_data_as_df(url)
        
        # If fetching failed, return empty result
        if raw_df is None:
            return pd.DataFrame() if return_format.lower() == 'dataframe' else []
        
        # Add shelf information to the dataframe
        if 'shelf' not in raw_df.columns:
            raw_df['shelf'] = shelf
        
        # Clean the data
        clean_df = clean_data(raw_df)
        
        # Return in requested format
        if return_format.lower() == 'json':
            return clean_df.to_dict(orient='records')
        else:
            return clean_df

def get_all_goodreads_user_books(user_id: str, shelf: str = 'all', return_format: str = 'dataframe') -> Union[pd.DataFrame, List[Dict]]:
    """
    Fetch all pages of books for a Goodreads user.
    
    Args:
        user_id (str): Goodreads user ID
        shelf (str): Which shelf to query - 'read', 'currently-reading', 'to-read', 'all' (default: 'all')
        return_format (str): 'dataframe' or 'json' (default: 'dataframe')
        
    Returns:
        Union[pd.DataFrame, List[Dict]]: All user's books in the requested format
    """
    # If 'all' is specified, fetch books from each shelf and combine them
    if shelf.lower() == 'all':
        # Define the shelves to fetch
        shelves = ['read', 'currently-reading', 'to-read']
        all_shelf_books = []
        
        print(f"Fetching ALL shelves for user {user_id}...")
        
        # Fetch books from each shelf
        for single_shelf in shelves:
            
            # Get all books for this shelf using the existing function
            shelf_books = get_all_goodreads_user_books(user_id, single_shelf, return_format='dataframe')
            
            if not shelf_books.empty:
                all_shelf_books.append(shelf_books)
                print(f"Found {len(shelf_books)} total books on shelf '{single_shelf}'")
        
        # Combine all books
        if all_shelf_books:
            combined_df = pd.concat(all_shelf_books, ignore_index=True)
            
            # Return in requested format
            if return_format.lower() == 'json':
                return combined_df.to_dict(orient='records')
            else:
                return combined_df
        else:
            return pd.DataFrame() if return_format.lower() == 'dataframe' else []
    
    # For specific shelves, perform normal pagination for that shelf
    else:
        all_books = []
        page_num = 1
        empty_page_count = 0
        MAX_EMPTY_PAGES = 1  # Stop after finding this many empty pages
        
        print(f"Fetching all books for user {user_id}, shelf: {shelf}...")

        while True:
            page_books = get_goodreads_user_books_by_page(user_id, page_num, shelf, return_format='dataframe')
            print(f"Page {page_num}: Found {len(page_books)} books")

            if page_books.empty:
                break

            if return_format.lower() == 'dataframe':
                all_books.append(page_books)
            else:
                all_books.extend(page_books.to_dict(orient='records'))

            if len(page_books) < 90:
                break

            page_num += 1

        # Combine all books
        if return_format.lower() == 'dataframe':
            if all_books:
                return pd.concat(all_books, ignore_index=True)
            else:
                return pd.DataFrame()
        else:
            return all_books

if __name__ == "__main__":
    # Example: Get books from different shelves
    USER_ID = "42944663"

    all_books = get_all_goodreads_user_books(USER_ID, shelf='all', return_format='dataframe')
    all_books.to_csv("all_books.csv", index=False)

    print("All books:")
    print(all_books)
