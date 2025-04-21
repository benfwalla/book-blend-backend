import json
import os
import warnings
import re
import pandas as pd
from bs4 import BeautifulSoup
import requests
from dotenv import load_dotenv
from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader
import time
from typing import Union, Dict, List, Any

# Load environment variables and set pandas options
load_dotenv()
warnings.filterwarnings('ignore')
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', None)

# Set constants for environment variables
HARDCOVER_BEARER_TOKEN = os.getenv('HARDCOVER_BEARER_TOKEN')
BOOKBLEND_API_KEY = os.getenv("BOOKBLEND_API_KEY")

# API key header setup
api_key_header = APIKeyHeader(name="X-API-Key")


def get_api_key(api_key_header: str = Security(api_key_header)) -> str:
    if api_key_header == BOOKBLEND_API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="401: Invalid API Key",
    )


def format_date(date_str):
    if pd.isna(date_str) or 'not set' in date_str:
        return pd.NA
    parts = date_str.split()
    if len(parts) == 2:  # Format is 'Month Year'
        return f"{parts[0]} 1, {parts[1]}"  # Inserting default day
    return date_str


def format_and_convert_date(series, date_pattern):
    series = series.str.extract(date_pattern)[0]
    series = series.replace('not set', pd.NA)
    series = series.apply(lambda x: f"{x.split()[0]} 1, {x.split()[1]}" if pd.notna(x) and len(x.split()) == 2 else x)
    return pd.to_datetime(series, errors='coerce')


def get_goodreads_user_books_by_page(user_id: str, page_num: int = 1, return_format: str = 'dataframe') -> Union[pd.DataFrame, Dict]:
    """
    Fetch a specific page of a Goodreads user's books and parse the data.
    
    Args:
        user_id (str): Goodreads user ID
        page_num (int): Page number to fetch (default: 1)
        return_format (str): 'dataframe' or 'json' (default: 'dataframe')
    
    Returns:
        Union[pd.DataFrame, Dict]: User's books data in the specified format
    """
    url = f"https://www.goodreads.com/review/list/{user_id}?page={page_num}&shelf=read"
    
    # 1. Fetch HTML
    print(f"Fetching page {page_num} for user {user_id}...")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL: {e}")
        return pd.DataFrame() if return_format == 'dataframe' else {}

    # 2. Parse HTML and Find Table
    soup = BeautifulSoup(response.text, 'html.parser')
    books_table = soup.find('table', id='books')
    
    # Check if we've reached the end (no table or empty table)
    if not books_table or not books_table.find_all('tr', class_='bookalike'):
        print(f"No books found on page {page_num}. This may be the last page.")
        return pd.DataFrame() if return_format == 'dataframe' else {}

    # 3. Extract Data from Table
    # First, get the header row to identify columns
    header_row = books_table.find('tr', id='booksHeader')
    if not header_row:
        print("Warning: Could not find header row.")
        return pd.DataFrame() if return_format == 'dataframe' else {}
        
    # Found the header row, use it to drive extraction
    header_cells = [cell for cell in header_row.find_all('th')]
    # Get clean header values
    header_values = []
    for cell in header_cells:
        # Get the alt attribute which is the clean column name
        header_name = cell.get('alt', '').strip()
        if not header_name:
            # Fall back to the text if alt is not available
            header_name = cell.get_text(strip=True)
        header_values.append(header_name)
    
    # Add columns for Goodreads ID and URL
    header_values.extend(['goodreads_id', 'goodreads_url'])
    
    # Get column positions for data extraction
    positions = []
    for cell in header_cells:
        # Get position from alt attribute
        alt_attr = cell.get('alt', '')
        positions.append(alt_attr)
    
    # Extract data from rows
    all_rows = [header_values]  # Start with header values
    
    # Process each data row
    for row in books_table.find_all('tr', class_='bookalike'):
        # Track Goodreads ID and URL 
        book_id = ""
        book_url = ""
        
        # Get all cells
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
                # Extract book ID from URL path (format: /book/show/36072.The_7_Habits...)
                book_id_match = re.search(r'/book/show/(\d+)', href)
                if book_id_match:
                    book_id = book_id_match.group(1)
                    book_url = f"https://www.goodreads.com{href}"
        
        # Now extract data for each cell
        row_data = []
        for i, pos in enumerate(positions):
            if i < len(data_cells):
                cell = data_cells[i]
                
                # Special handling for cover column - extract image URL
                if pos == 'cover':
                    img_tag = cell.find('img')
                    if img_tag and img_tag.has_attr('src'):
                        cell_value = img_tag['src']
                        # Remove size limitations from image URL
                        cell_value = re.sub(r'(\d+)\.(_S[XY]\d+_)(\.jpg)', r'\1\3', cell_value)
                        cell_value = re.sub(r'_S[XY]\d+_', '', cell_value)
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
                    stars_span = cell.find('span', class_='staticStars')
                    if stars_span and stars_span.has_attr('title'):
                        cell_value = stars_span['title']
                    else:
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
                    # For other columns, extract only the value div content
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
                
                row_data.append(cell_value)
            else:
                # If cell doesn't exist, add an empty value
                row_data.append("")
        
        # Add Goodreads ID and URL
        row_data.append(book_id)
        row_data.append(book_url)
        
        if row_data:  # Only add if we got some data
            all_rows.append(row_data)

    # 4. Create DataFrame
    if not all_rows or len(all_rows) < 2:  # Need at least header + 1 data row
        print("Warning: No data rows found in table.")
        return pd.DataFrame() if return_format == 'dataframe' else {}
        
    try:
        df = pd.DataFrame(all_rows[1:], columns=all_rows[0])
    except Exception as e:
        print(f"Error creating DataFrame: {e}")
        return pd.DataFrame() if return_format == 'dataframe' else {}
    
    # 5. Clean the data
    df = clean_goodreads_data(df)
    
    # Return in the requested format
    if return_format.lower() == 'json':
        return df.to_dict(orient='records')
    else:  # default to dataframe
        return df


def get_all_goodreads_user_books(user_id: str, return_format: str = 'dataframe') -> Union[pd.DataFrame, List[Dict]]:
    """
    Fetch all pages of a Goodreads user's books and combine them.
    
    Args:
        user_id (str): Goodreads user ID
        return_format (str): 'dataframe' or 'json' (default: 'dataframe')
    
    Returns:
        Union[pd.DataFrame, List[Dict]]: All user's books in the specified format
    """
    all_books = []
    page_num = 1
    empty_page_count = 0
    max_empty_pages = 2  # Stop after this many consecutive empty pages
    
    print(f"Fetching all books for user {user_id}...")
    
    while empty_page_count < max_empty_pages:
        # Get books for current page
        page_books = get_goodreads_user_books_by_page(user_id, page_num, return_format='dataframe')
        
        # Check if we got any books
        if page_books.empty:
            empty_page_count += 1
            print(f"Empty page #{page_num} (empty count: {empty_page_count})")
        else:
            empty_page_count = 0  # Reset counter when we find books
            
            # For dataframe, append to list to concat later
            if return_format.lower() == 'dataframe':
                all_books.append(page_books)
            # For JSON, extend the list directly
            else:
                page_json = page_books.to_dict(orient='records')
                all_books.extend(page_json)
                
            print(f"Page {page_num}: Found {len(page_books)} books")
        
        # Go to next page
        page_num += 1
        
        # Be nice to the server
        time.sleep(1)
    
    # Combine all books
    if return_format.lower() == 'dataframe':
        if all_books:
            return pd.concat(all_books, ignore_index=True)
        else:
            return pd.DataFrame()
    else:  # JSON format
        return all_books


def clean_goodreads_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform the raw DataFrame from Goodreads.
    
    Args:
        df (pd.DataFrame): Raw DataFrame from Goodreads
    
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    if df.empty:
        return df
        
    # Make a copy to avoid warnings
    cleaned_df = df.copy()
    
    # Clean up author names - remove trailing asterisks
    if 'author' in cleaned_df.columns:
        cleaned_df['author'] = cleaned_df['author'].str.replace(r'\*$', '', regex=True)
    
    # Extract and map ratings
    if 'rating' in cleaned_df.columns:
        # Create mapping from text ratings to numeric values
        rating_mapping = {
            'did not like it': 1,
            'it was ok': 2,
            'liked it': 3,
            'really liked it': 4,
            'it was amazing': 5
        }
        
        # Extract text ratings
        cleaned_df['rating_text'] = cleaned_df['rating'].str.extract(r'(.+?)\'s rating\s*(.*?)$', expand=True)[1].str.strip()
        
        # If that doesn't work well, try other methods
        mask = cleaned_df['rating_text'].isna() | (cleaned_df['rating_text'] == '')
        if mask.any():
            for rating_text in rating_mapping.keys():
                text_mask = cleaned_df['rating'].str.contains(rating_text, na=False, regex=False)
                cleaned_df.loc[mask & text_mask, 'rating_text'] = rating_text
                
        # Map text ratings to numeric values
        cleaned_df['rating_numeric'] = cleaned_df['rating_text'].map(rating_mapping)
        
        # Convert to float
        cleaned_df['rating_numeric'] = pd.to_numeric(cleaned_df['rating_numeric'], errors='coerce')
    
    # Clean ISBN columns
    for isbn_col in ['isbn', 'isbn13', 'asin']:
        if isbn_col in cleaned_df.columns:
            # Remove any prefix like "isbn" or "asin"
            cleaned_df[isbn_col] = cleaned_df[isbn_col].str.replace(f'{isbn_col}', '', regex=False)
    
    # Clean date fields
    date_columns = [col for col in cleaned_df.columns if 'date' in col]
    for date_col in date_columns:
        if date_col not in cleaned_df.columns:
            continue
            
        # Convert "not set" to NaN for all date columns
        cleaned_df[date_col] = cleaned_df[date_col].replace('not set', pd.NA)
        
        # For publication dates, extract just the year
        if date_col in ['date_pub', 'date_pub_edition']:
            # Use regex to extract the year from pub date strings
            year_pattern = r'(?:^|\D)(\d{4})(?:\D|$)'  
            years = cleaned_df[date_col].str.extract(year_pattern, expand=False)
            cleaned_df[date_col] = years
        # For reading dates, convert to ISO format directly
        elif date_col in ['date_started', 'date_read', 'date_added', 'date_purchased']:
            try:
                # Convert to ISO format in-place
                temp_dates = pd.to_datetime(cleaned_df[date_col], errors='coerce', format='mixed')
                cleaned_df[date_col] = temp_dates.dt.strftime('%Y-%m-%d')
            except:
                pass
    
    # Clean up page numbers - extract just the digits
    if 'num_pages' in cleaned_df.columns:
        cleaned_df['pages'] = cleaned_df['num_pages'].str.extract(r'(\d+)', expand=False)
        cleaned_df['pages'] = pd.to_numeric(cleaned_df['pages'], errors='coerce')
        # Drop the original column
        cleaned_df = cleaned_df.drop(columns=['num_pages'])
    
    # Clean up num_ratings - remove commas and convert to numeric
    if 'num_ratings' in cleaned_df.columns:
        cleaned_df['num_ratings'] = cleaned_df['num_ratings'].str.replace(',', '', regex=False)
        cleaned_df['num_ratings'] = pd.to_numeric(cleaned_df['num_ratings'], errors='coerce')
    
    # Clean up review column - replace "None" with empty string
    if 'review' in cleaned_df.columns:
        cleaned_df['review'] = cleaned_df['review'].replace('None', '')
        
    # Clean up comments column - convert to integer
    if 'comments' in cleaned_df.columns:
        cleaned_df['comments'] = pd.to_numeric(cleaned_df['comments'], errors='coerce').fillna(0).astype(int)
    
    # Remove unnecessary columns or rename for clarity
    columns_to_drop = []
    for col in cleaned_df.columns:
        # Drop columns that are just IDs, labels, or redundant
        if col in ['checkbox', 'position', 'recommender', 'purchase_location', 'condition', 'actions',
                   'rating', 'shelves', 'notes', 'votes', 'owned', 'date_purchased', 'format', 'date_added']:
            columns_to_drop.append(col)
            
    # Drop the columns
    if columns_to_drop:
        cleaned_df = cleaned_df.drop(columns=[col for col in columns_to_drop if col in cleaned_df.columns])
    
    # Reorder columns - ensure pages is next to asin
    if all(col in cleaned_df.columns for col in ['asin', 'pages']):
        cols = list(cleaned_df.columns)
        asin_pos = cols.index('asin')
        if 'pages' in cols:
            cols.remove('pages')
            cols.insert(asin_pos + 1, 'pages')
        cleaned_df = cleaned_df[cols]
    
    return cleaned_df


def get_genres_from_hardcover(goodreads_ids):
    url = "https://hardcover-production.hasura.app/v1/graphql"
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {HARDCOVER_BEARER_TOKEN}'
    }

    # Convert the Series or list of IDs to the required string format
    ids_string = ', '.join(f'"{id_}"' for id_ in goodreads_ids)

    # Construct the GraphQL query
    query = f"""
    query GetBookByGoodreadsIDs {{
      book_mappings(
        where: {{platform: {{id: {{_eq: 1}}}}, external_id: {{_in: [{ids_string}]}}}}
      ) {{
        external_id
        book {{
          title      
          taggings {{
            tag {{
              tag
            }}
          }}
        }}
      }}
    }}
    """

    payload = json.dumps({"query": query, "variables": {}})
    response = requests.post(url, headers=headers, data=payload).json()

    books_json = response['data']['book_mappings']
    flattened_data = []

    # Iterate through each book entry in the JSON
    for entry in books_json:
        book_id = entry['external_id']
        title = entry['book']['title']

        # Flatten the taggings into a single string separated by commas
        tags = [tag['tag']['tag'] for tag in entry['book']['taggings']]

        # Append the flattened data to the list
        flattened_data.append({'external_id': book_id,
                               'title': title,
                               'tags': tags})

    genres_df = pd.DataFrame(flattened_data)

    return genres_df


def combine_goodreads_and_hardcover(goodreads_df, hardcover_df):
    return pd.merge(goodreads_df, hardcover_df, left_on='goodreads_id', right_on='external_id', how='left')


def get_user_info(user_id):
    url = f"https://www.goodreads.com/user/show/{user_id}"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    try:
        canonical_link = soup.find('link', {'rel': 'canonical'})['href']
        user_id = canonical_link.split('/')[-1].split('-')[0]
    except TypeError:
        user_id = ''

    try:
        title = soup.find('title').text
        books_shelved_match = re.search(r'(\d{1,3}(?:,\d{3})*|\d+)\s+books', title)
        books_shelved = books_shelved_match.group(1).replace(',', '')
    except AttributeError:
        books_shelved = ''

    try:
        books_read_match = re.search(r'read\s*\(.*?(\d{1,3}(?:,\d{3})*|\d+)\)', soup.text)
        books_read = books_read_match.group(1).replace(',', '')
    except AttributeError:
        books_read = ''

    try:
        currently_reading_count_match = re.search(r'currently-reading&lrm;\s*\((\d{1,3}(?:,\d{3})*|\d+)\)', soup.text)
        currently_reading_count = currently_reading_count_match.group(1).replace(',', '')
    except AttributeError:
        currently_reading_count = ''

    try:
        to_read_count_match = re.search(r'to-read&lrm;\s*\((\d{1,3}(?:,\d{3})*|\d+)\)', soup.text)
        to_read_count = to_read_count_match.group(1).replace(',', '')
    except AttributeError:
        to_read_count = ''

    try:
        full_name = soup.find('meta', {'property': 'og:title'})['content']
    except TypeError:
        full_name = ''

    try:
        first_name = soup.find('meta', {'property': 'profile:first_name'})['content']
    except TypeError:
        first_name = ''

    try:
        last_name = soup.find('meta', {'property': 'profile:last_name'})['content']
    except TypeError:
        last_name = ''

    try:
        username = soup.find('meta', {'property': 'profile:username'})['content']
    except TypeError:
        username = ''

    try:
        friends_match = re.search(r" Friends \((\d+)\)", soup.text)
        friends = friends_match.group(1)
    except AttributeError:
        friends = ''

    return {
        'user_id': user_id,
        'full_name': full_name,
        'first_name': first_name,
        'last_name': last_name,
        'username': username,
        'books_shelved': books_shelved,
        'number_of_friends': friends,
        'books_read': books_read,
        'currently_reading_count': currently_reading_count,
        'to_read_count': to_read_count
    }

if __name__ == "__main__":
    print(get_goodreads_user_books_by_page(42944663, 1))
    #print(get_all_goodreads_user_books(42944663))
    #print(get_genres_from_hardcover([176444106, 57693457, 4808763, 157981748, 164780, 158875813, 43889703]))
