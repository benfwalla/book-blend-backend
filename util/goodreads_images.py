import re


_COVER_RESIZE = re.compile(
    r"\._[^./]+_(?=\.(?:jpe?g|png|webp)(?:$|\?))",
    re.IGNORECASE,
)
_PROFILE_SIZE = re.compile(
    r"(images\.gr-assets\.com/users/\d+)p\d+(/)",
    re.IGNORECASE,
)


def high_resolution_goodreads_image(url):
    """Return the original Goodreads image represented by a thumbnail URL."""
    if not url:
        return url

    return _PROFILE_SIZE.sub(r"\1p8\2", _COVER_RESIZE.sub("", url))
