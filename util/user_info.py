import requests
from bs4 import BeautifulSoup

from util.goodreads_images import high_resolution_goodreads_image
from util.goodreads_graphql import fetch_all_friends

def get_goodreads_user_info(user_id=None, username=None):
    if not user_id and not username:
        raise ValueError("Either user_id or username must be provided")
    if user_id and username:
        raise ValueError("Provide either user_id or username, not both")
    
    if username:
        user_url = f"https://www.goodreads.com/{username}"
        # We'll extract the actual user_id from the response for consistency
        actual_user_id = username  # Will be updated after parsing
    else:
        user_url = f"https://www.goodreads.com/user/show/{user_id}"
        actual_user_id = user_id
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    response = requests.get(user_url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch page: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")
    base_url = "https://www.goodreads.com"
    
    # Extract user information from the meta tags
    user_info = {
        "name": "",
        "image_url": "",
        "id": actual_user_id,
        "profile_url": user_url,
        "book_count": "",
        "username": ""
    }
    
    # Get name from og:title
    og_title = soup.find("meta", property="og:title")
    if og_title:
        user_info["name"] = og_title.get("content", "")
    
    # Get image from og:image
    og_image = soup.find("meta", property="og:image")
    if og_image:
        user_info["image_url"] = high_resolution_goodreads_image(og_image.get("content", ""))
    
    # Get book count from og:description
    og_description = soup.find("meta", property="og:description")
    if og_description:
        description = og_description.get("content", "")
        if " has " in description and " books on Goodreads" in description:
            book_count_part = description.split(" has ")[1].split(" books on Goodreads")[0]
            user_info["book_count"] = book_count_part
    
    # Get username from profile:username
    profile_username = soup.find("meta", property="profile:username")
    if profile_username:
        user_info["username"] = profile_username.get("content", "")
    
    # If we used username to access the page, extract the actual user_id from the URL
    if username:
        # Look for canonical URL or user profile links to extract the numeric user_id
        canonical_link = soup.find("link", rel="canonical")
        if canonical_link:
            canonical_url = canonical_link.get("href", "")
            if "/user/show/" in canonical_url:
                # Extract user_id from URL like https://www.goodreads.com/user/show/113735659-mark-o-connell
                user_id_part = canonical_url.split("/user/show/")[1].split("-")[0]
                user_info["id"] = user_id_part
                actual_user_id = user_id_part

    # Parse the profile previews as a fallback and to retain followed users,
    # which Goodreads exposes separately from the complete friend graph.
    profile_connections = []

    # Parse friends
    for div in soup.select(".bigBoxContent.containerWithHeaderContent > div"):
        a_tag = div.select_one("a.leftAlignedImage")
        name_tag = div.select_one(".friendName a")
        img_tag = a_tag.find("img") if a_tag else None

        if not a_tag or not name_tag or not img_tag:
            continue

        profile_path = a_tag["href"]
        friend_user_id = profile_path.split("/")[-1].split("-")[0]
        name = name_tag.text.strip()

        # Extract book count
        count_container = div.select_one(".left")
        book_count = ""
        if count_container:
            text_parts = list(count_container.stripped_strings)
            for part in text_parts:
                if "books" in part:
                    book_count = part.replace(" books", "").strip()

        profile_connections.append({
            "name": name,
            "image_url": high_resolution_goodreads_image(img_tag["src"]),
            "id": friend_user_id,
            "profile_url": base_url + profile_path,
            "book_count": book_count,
            "connection_type": "profile_connection",
        })

    # Parse followed users (no book count)
    for a in soup.select("div.bigBoxBody div.bigBoxContent a.leftAlignedImage"):
        profile_path = a.get("href")
        title = a.get("title")
        img_tag = a.find("img")

        if not profile_path or not title or not img_tag:
            continue

        followed_user_id = profile_path.split("/")[-1].split("-")[0]

        profile_connections.append({
            "name": title.strip(),
            "image_url": high_resolution_goodreads_image(img_tag["src"]),
            "id": followed_user_id,
            "profile_url": base_url + profile_path,
            "book_count": "",
            "connection_type": "following",
        })

    try:
        complete_friends = fetch_all_friends(actual_user_id)
    except Exception as error:
        print(f"Goodreads friend graph failed for user {actual_user_id}: {error}")
        complete_friends = []

    # GraphQL supplies every friend; the profile preview supplies followed
    # profiles. Prefer the richer preview record when the same user appears.
    connections_by_id = {friend["id"]: friend for friend in complete_friends}
    for connection in profile_connections:
        existing = connections_by_id.get(connection["id"])
        if existing and existing.get("connection_type") == "friend":
            connection["connection_type"] = "friend"
        connections_by_id[connection["id"]] = connection
    friends = list(connections_by_id.values())

    return {
        "user": user_info,
        "friends": friends
    }


if __name__ == "__main__":
    # Test with user_id
    user_id = "42944663"
    result = get_goodreads_user_info(user_id=user_id)
    print("Result with user_id:", result)
    
    # Test with username (uncomment to test)
    # username = "markoconnell"
    # result = get_goodreads_user_info(username=username)
    # print("Result with username:", result)
