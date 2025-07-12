import requests
from bs4 import BeautifulSoup

def get_goodreads_user_info(user_id):
    user_url = f"https://www.goodreads.com/user/show/{user_id}"
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
        "id": user_id,
        "profile_url": user_url,
        "book_count": ""
    }
    
    # Get name from og:title
    og_title = soup.find("meta", property="og:title")
    if og_title:
        user_info["name"] = og_title.get("content", "")
    
    # Get image from og:image
    og_image = soup.find("meta", property="og:image")
    if og_image:
        user_info["image_url"] = og_image.get("content", "")
    
    # Get book count from og:description
    og_description = soup.find("meta", property="og:description")
    if og_description:
        description = og_description.get("content", "")
        if " has " in description and " books on Goodreads" in description:
            book_count_part = description.split(" has ")[1].split(" books on Goodreads")[0]
            user_info["book_count"] = book_count_part

    # Parse friends
    friends = []

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

        friends.append({
            "name": name,
            "image_url": img_tag["src"],
            "id": friend_user_id,
            "profile_url": base_url + profile_path,
            "book_count": book_count
        })

    # Parse followed users (no book count)
    for a in soup.select("div.bigBoxBody div.bigBoxContent a.leftAlignedImage"):
        profile_path = a.get("href")
        title = a.get("title")
        img_tag = a.find("img")

        if not profile_path or not title or not img_tag:
            continue

        followed_user_id = profile_path.split("/")[-1].split("-")[0]

        friends.append({
            "name": title.strip(),
            "image_url": img_tag["src"],
            "id": followed_user_id,
            "profile_url": base_url + profile_path,
            "book_count": ""
        })

    return {
        "user": user_info,
        "friends": friends
    }


if __name__ == "__main__":
    user_id = "42944663"
    result = get_goodreads_user_info(user_id)
    print(result)
