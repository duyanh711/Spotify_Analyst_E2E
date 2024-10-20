import pandas as pd
import os

# Define URLs
URL = "https://kworb.net/spotify/artists.html"

# Define path to store list of artists name
FILE_PATH = "data/artists_names.txt"


def get_artists_name(url: str):
    """_summary_:
    Get artists name from URL

    Args:
        url (str): URL to get artists name

    Returns:
        artists_name (list): List of artists name
    """
    # Read table from URL
    spotify_artists_table = pd.read_html(url)[0]

    # Get artists name
    artists_name = spotify_artists_table["Artist"]

    # Extract 1000 artists name
    artists_name = artists_name.tolist()[:2000]
    return artists_name


def store_artists_name(artists_name, file_name=FILE_PATH):
    """_summary_

    Args:
        artists_name (list): List of artists name
    """
    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    try:
        # Write the artist names to file
        with open(file_name, 'w', encoding='utf-8') as f:
            for artist_name in artists_name:
                f.write(artist_name + "\n")
        print(f"Successfully stored artist names to {file_name}")
    except Exception as e:
        print(f"Error while storing artists: {e}")


def artists_crawler(path=FILE_PATH):
    """_summary_:
    Main function
    """
    artists_name = get_artists_name(URL)
    store_artists_name(artists_name, path)


if __name__ == "__main__":
    print("Start")
    artists_crawler()
    print("Success")