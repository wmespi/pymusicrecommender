from langchain_community.document_loaders import AZLyricsLoader

# AZLyrics website
AZ_LYRICS_BASE_URL = 'https://www.azlyrics.com'
AZ_LYRICS_ARTIST_LETTER_LIST = [
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '19'
]

def get_artist_url_list(artist_letter):
    """
    Retrieves the AZLyrics website URLs for all the artists given its first character.
    :param artist_letter: First character of an artist.
    :return: List of pairs containing the artist name and its AZLyrics URL.
    """
    artist_url_list = []

    try:
        artist_letter_url = f'{AZ_LYRICS_BASE_URL}/{artist_letter}.html'
        html_content = _get_html(artist_letter_url)
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')

            column_list = soup.find_all('div', {'class': 'artist-col'})
            for column in column_list:
                for a in column.find_all('a'):
                    artist_name = string_cleaner.clean_name(a.text)
                    artist_url = string_cleaner.clean_url('{}/{}'.format(AZ_LYRICS_BASE_URL, a['href']))
                    artist_url_list.append((artist_name, artist_url))
    except Exception as e:
        print(f'Error while getting artists from letter {artist_letter}: {e}')

    return artist_url_list

# Iteratve over every letter
for artist_letter in AZ_LYRICS_ARTIST_LETTER_LIST:

    #### Replace with logging ####
    print(f'[1] Processing [{artist_letter}] letter...')


# # Initiate loader
# loader = AZLyricsLoader(
#     "https://www.azlyrics.com/lyrics/a/a1.html",
#     requests_per_second=0.5
# )

# data = loader.load()
# print(data)
